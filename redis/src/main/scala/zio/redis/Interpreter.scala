package zio.redis

import java.io.{ EOFException, IOException }
import java.net.{ InetAddress, InetSocketAddress, SocketAddress, StandardSocketOptions }
import java.nio.ByteBuffer
import java.nio.channels.{ AsynchronousByteChannel, AsynchronousSocketChannel, Channel, CompletionHandler }

import zio._
import zio.stream.Stream

trait Interpreter {

  import Interpreter._

  type RedisExecutor = Has[RedisExecutor.Service]

  object RedisExecutor {

    val defaultPort = 6379

    trait Service {

      def execute(command: Chunk[RespValue.BulkString]): IO[RedisError, RespValue]

    }

    /**
     * This is to make better use of pipelining by writing requests to the socket in batches.
     * Ideally this*(average RESP serialized message size) should equal the buffer size used by `ByteStream`,
     * but we can only guesstimate.
     */
    private val requestQueueSize = 20

    private final class Live(
      reqQueue: Queue[Request],
      resQueue: Queue[Promise[RedisError, RespValue]],
      byteStream: Managed[IOException, ByteStream.ReadWriteBytes]
    ) extends Service {

      override def execute(command: Chunk[RespValue.BulkString]): IO[RedisError, RespValue] =
        Promise
          .make[RedisError, RespValue]
          .flatMap(promise => reqQueue.offer(Request(command, promise)) *> promise.await)

      private def sendNext(out: Chunk[Byte] => IO[IOException, Unit]): IO[RedisError, Unit] =
        reqQueue.takeBetween(1, Int.MaxValue).flatMap { reqs =>
          val bytes = Chunk.fromIterable(reqs).flatMap(req => RespValue.Array(req.command).serialize)
          out(bytes)
            .mapError(RedisError.IOError)
            .tapError(e => IO.foreach(reqs)(_.promise.fail(e)))
            .zipLeft(IO.foreach(reqs)(req => resQueue.offer(req.promise)))
        }

      private def runReceive(inStream: Stream[IOException, Byte]): IO[RedisError, Unit] =
        inStream
          .mapError(RedisError.IOError)
          .transduce(RespValue.deserialize.toTransducer)
          .foreach(response => resQueue.take.flatMap(_.succeed(response)))

      /**
       * Opens a connection to the server and launches send and receive operations.
       * All failures are retried by opening a new connection.
       * Only exits by interruption or defect.
       */
      def run: IO[RedisError, Unit] =
        byteStream
          .mapError(RedisError.IOError)
          .use { rwBytes =>
            sendNext(rwBytes.write).forever race runReceive(rwBytes.read)
          }
          .tapError { e =>
            IO.effectTotal(println(s"Reconnecting due to error: $e")) *>
              resQueue.takeAll.flatMap(IO.foreach(_)(_.fail(e)))
          }
          .retryWhile(Function.const(true))
          .tapCause(c => IO.effectTotal(Console.err.println(s"Executor exiting: $c")))

    }

    def live: ZLayer[ByteStream, RedisError.IOError, RedisExecutor] =
      ZLayer.fromServiceManaged { env =>
        for {
          reqQueue <- Queue.bounded[Request](requestQueueSize).toManaged_
          resQueue <- Queue.unbounded[Promise[RedisError, RespValue]].toManaged_
          live      = new Live(reqQueue, resQueue, env.connect)
          _        <- live.run.forkManaged
        } yield live
      }

    def liveServer(address: SocketAddress): Layer[RedisError.IOError, RedisExecutor] =
      ByteStream.socket(address).mapError(RedisError.IOError) >>> live

    def liveServer(host: String, port: Int = defaultPort): Layer[RedisError.IOError, RedisExecutor] =
      ByteStream.socket(host, port).mapError(RedisError.IOError) >>> live

    def liveLoopback(port: Int = defaultPort): Layer[RedisError.IOError, RedisExecutor] =
      ByteStream.socketLoopback(port).mapError(RedisError.IOError) >>> live

  }

  type ByteStream = Has[ByteStream.Service]

  object ByteStream {

    val ResponseBufferSize = 1024

    def connect: ZManaged[ByteStream, IOException, ReadWriteBytes] = ZManaged.accessManaged(_.get.connect)

    trait Service {

      def connect: Managed[IOException, ReadWriteBytes]

    }

    trait ReadWriteBytes {

      def read: Stream[IOException, Byte]

      def write(chunk: Chunk[Byte]): IO[IOException, Unit]

    }

    /**
     * Adapts `ComplectionHandler` async channel operations with interruption support.
     */
    private def effectAsyncChannel[C <: Channel, T](
      channel: C
    )(op: C => CompletionHandler[T, Any] => Any): IO[IOException, T] =
      IO.effectAsyncInterrupt { k =>
        val handler = new CompletionHandler[T, Any] {
          def completed(result: T, u: Any): Unit = k(IO.succeedNow(result))

          def failed(t: Throwable, u: Any): Unit =
            t match {
              case e: IOException => k(IO.fail(e))
              case _              => k(IO.die(t))
            }
        }

        op(channel)(handler)
        Left(IO.effect(channel.close()).ignore)
      }

    def socket(host: String, port: Int): Layer[IOException, ByteStream] =
      socket(IO.effectTotal(new InetSocketAddress(host, port)))

    def socket(address: SocketAddress): Layer[IOException, ByteStream] = socket(UIO.succeed(address))

    def socketLoopback(port: Int): Layer[IOException, ByteStream] =
      socket(IO.effectTotal(new InetSocketAddress(InetAddress.getLoopbackAddress, port)))

    private def socket(getAddress: UIO[SocketAddress]): Layer[IOException, ByteStream] = {
      val makeBuffer = IO.effectTotal(ByteBuffer.allocateDirect(ResponseBufferSize))
      ZLayer.fromEffect {
        for {
          address     <- getAddress
          readBuffer  <- makeBuffer
          writeBuffer <- makeBuffer
        } yield new Connection(address, readBuffer, writeBuffer)
      }
    }

    private final class Connection(
      address: SocketAddress,
      readBuffer: ByteBuffer,
      writeBuffer: ByteBuffer
    ) extends Service {

      private def openChannel: Managed[IOException, AsynchronousSocketChannel] = {
        val make = for {
          channel <- IO.effect {
                       val channel = AsynchronousSocketChannel.open()
                       channel.setOption(StandardSocketOptions.SO_KEEPALIVE, Boolean.box(true))
                       channel.setOption(StandardSocketOptions.TCP_NODELAY, Boolean.box(true))
                       channel
                     }
          _       <- effectAsyncChannel[AsynchronousSocketChannel, Void](channel)(c => c.connect(address, null, _))
        } yield channel
        Managed.fromAutoCloseable(make).refineToOrDie[IOException]
      }

      override def connect: Managed[IOException, ReadWriteBytes] =
        openChannel.map { channel =>
          val readChunk: IO[Option[IOException], Chunk[Byte]] =
            (for {
              _     <- IO.effectTotal(readBuffer.clear())
              _     <- effectAsyncChannel[AsynchronousByteChannel, Integer](channel)(c => c.read(readBuffer, null, _))
                     .filterOrFail(_ >= 0)(new EOFException())
              chunk <- IO.effectTotal {
                         readBuffer.flip()
                         val count = readBuffer.remaining()
                         val array = Array.ofDim[Byte](count)
                         readBuffer.get(array)
                         Chunk.fromArray(array)
                       }
            } yield chunk).mapError {
              case _: EOFException => None
              case e: IOException  => Some(e)
            }

          def writeChunk(chunk: Chunk[Byte]): IO[IOException, Unit] =
            IO.when(chunk.nonEmpty) {
              IO.effectTotal {
                writeBuffer.clear()
                val (c, remainder) = chunk.splitAt(writeBuffer.capacity())
                writeBuffer.put(c.toArray)
                writeBuffer.flip()
                remainder
              }.flatMap { remainder =>
                effectAsyncChannel[AsynchronousByteChannel, Integer](channel)(c => c.write(writeBuffer, null, _))
                  .repeatWhileM(_ => IO.effectTotal(writeBuffer.hasRemaining))
                  .zipRight(writeChunk(remainder))
              }
            }

          new ReadWriteBytes {
            override def read = Stream.repeatEffectChunkOption(readChunk)

            override def write(chunk: Chunk[Byte]) = writeChunk(chunk)
          }
        }

    }

  }

}

object Interpreter {

  private final case class Request(
    command: Chunk[RespValue.BulkString],
    promise: Promise[RedisError, RespValue]
  )

}
