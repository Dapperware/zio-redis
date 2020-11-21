package zio.redis

import java.io.{ EOFException, IOException }
import java.net.{ InetAddress, InetSocketAddress, SocketAddress, StandardSocketOptions }
import java.nio.ByteBuffer
import java.nio.channels.{ AsynchronousByteChannel, AsynchronousSocketChannel, Channel, CompletionHandler }

import zio._
import zio.logging._
import zio.stream.Stream

private[redis] object ByteStream {
  private[this] final val ResponseBufferSize = 1024

  def connect: ZManaged[ByteStream, IOException, ReadWriteBytes] = ZManaged.accessManaged(_.get.connect)

  trait Service {
    val connect: Managed[IOException, ReadWriteBytes]
  }

  def socket(host: String, port: Int): ZLayer[Logging, IOException, ByteStream] =
    socket(IO.effectTotal(new InetSocketAddress(host, port)))

  def socket(address: SocketAddress): ZLayer[Logging, IOException, ByteStream] = socket(UIO.succeed(address))

  def socketLoopback(port: Int = RedisExecutor.DefaultPort): ZLayer[Logging, IOException, ByteStream] =
    socket(IO.effectTotal(new InetSocketAddress(InetAddress.getLoopbackAddress, port)))

  private def socket(getAddress: UIO[SocketAddress]): ZLayer[Logging, IOException, ByteStream] = {
    val makeBuffer = IO.effectTotal(ByteBuffer.allocateDirect(ResponseBufferSize))

    ZLayer.fromServiceM { logger =>
      for {
        address     <- getAddress
        readBuffer  <- makeBuffer
        writeBuffer <- makeBuffer
      } yield new Connection(address, readBuffer, writeBuffer, logger)
    }
  }

  trait ReadWriteBytes {
    def read: Stream[IOException, Byte]
    def write(chunk: Chunk[Byte]): IO[IOException, Unit]
  }

  private def completionHandlerCallback[A](k: IO[IOException, A] => Unit): CompletionHandler[A, Any] =
    new CompletionHandler[A, Any] {
      def completed(result: A, u: Any): Unit = k(IO.succeedNow(result))

      def failed(t: Throwable, u: Any): Unit =
        t match {
          case e: IOException => k(IO.fail(e))
          case _              => k(IO.die(t))
        }
    }

  private def effectAsyncChannel[C <: Channel, A](
    channel: C
  )(op: C => CompletionHandler[A, Any] => Any): IO[IOException, A] =
    IO.effectAsyncInterrupt { k =>
      op(channel)(completionHandlerCallback(k))
      Left(IO.effect(channel.close()).ignore)
    }

  private final class Connection(
    address: SocketAddress,
    readBuffer: ByteBuffer,
    writeBuffer: ByteBuffer,
    logger: Logger[String]
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
        _       <- logger.info("Connected to the redis server.")
      } yield channel
      Managed.fromAutoCloseable(make).refineToOrDie[IOException]
    }

    override val connect: Managed[IOException, ReadWriteBytes] =
      openChannel.map { channel =>
        val readChunk =
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

        @inline
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
          def read: Stream[IOException, Byte] = Stream.repeatEffectChunkOption(readChunk)

          def write(chunk: Chunk[Byte]): IO[IOException, Unit] = writeChunk(chunk)
        }
      }
  }
}
