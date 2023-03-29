/*
 * Copyright 2021 John A. De Goes and the ZIO contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package zio.redis

import zio._
import zio.redis.SingleNodeExecutor._
import zio.redis.internal.{RedisConnection, RespCommand, RespValue}

final class SingleNodeExecutor private (
  connection: RedisConnection,
  requests: Queue[Request],
  responses: Queue[Promise[RedisError, RespValue]]
) extends RedisExecutor {

  // TODO NodeExecutor doesn't throw connection errors, timeout errors, it is hanging forever
  def execute(command: RespCommand): IO[RedisError, RespValue] =
    Promise
      .make[RedisError, RespValue]
      .flatMap(promise => requests.offer(Request(command.args.map(_.value), promise)) *> promise.await)

  /**
   * Opens a connection to the server and launches send and receive operations. All failures are retried by opening a
   * new connection. Only exits by interruption or defect.
   */
  private val run: IO[RedisError, AnyVal] =
    ZIO.logTrace(s"$this Executable sender and reader has been started") *>
      (send.repeat[Any, Long](Schedule.forever) race receive)
        .tapError(e => ZIO.logWarning(s"Reconnecting due to error: $e") *> drainWith(e))
        .retryWhile(True)
        .tapError(e => ZIO.logError(s"Executor exiting: $e"))

  private def drainWith(e: RedisError): UIO[Unit] = responses.takeAll.flatMap(ZIO.foreachDiscard(_)(_.fail(e)))

  private def send: IO[RedisError.IOError, Option[Unit]] =
    requests.takeBetween(1, RequestQueueSize).flatMap { reqs =>
      val buffer = ChunkBuilder.make[Byte]()
      val it     = reqs.iterator

      while (it.hasNext) {
        val req = it.next()
        buffer ++= RespValue.Array(req.command).serialize
      }

      val bytes = buffer.result()

      connection
        .write(bytes)
        .mapError(RedisError.IOError(_))
        .tapBoth(
          e => ZIO.foreachDiscard(reqs)(_.promise.fail(e)),
          _ => ZIO.foreachDiscard(reqs)(req => responses.offer(req.promise))
        )
    }

  private def receive: IO[RedisError, Unit] =
    connection.read
      .mapError(RedisError.IOError(_))
      .via(RespValue.Decoder)
      .collectSome
      .foreach(response => responses.take.flatMap(_.succeed(response)))

}

object SingleNodeExecutor {
  lazy val layer: ZLayer[RedisConfig, RedisError.IOError, RedisExecutor] =
    RedisConnection.layer >>> makeLayer

  lazy val local: ZLayer[Any, RedisError.IOError, RedisExecutor] =
    RedisConnection.local >>> makeLayer

  final case class Request(command: Chunk[RespValue.BulkString], promise: Promise[RedisError, RespValue])

  private final val True: Any => Boolean = _ => true

  private final val RequestQueueSize = 16

  private[redis] def create(connection: RedisConnection): URIO[Scope, SingleNodeExecutor] =
    for {
      requests  <- Queue.bounded[Request](RequestQueueSize)
      responses <- Queue.unbounded[Promise[RedisError, RespValue]]
      executor   = new SingleNodeExecutor(connection, requests, responses)
      _         <- executor.run.forkScoped
      _         <- logScopeFinalizer(s"$executor Node Executor is closed")
    } yield executor

  private def makeLayer: ZLayer[RedisConnection, RedisError.IOError, RedisExecutor] =
    ZLayer.scoped(ZIO.serviceWithZIO[RedisConnection](create))
}
