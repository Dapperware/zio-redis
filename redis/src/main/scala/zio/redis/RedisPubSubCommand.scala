package zio.redis

import zio.redis.Output.ArbitraryOutput
import zio.schema.Schema
import zio.schema.codec.BinaryCodec
import zio.stream._
import zio.{Chunk, IO, ZIO, ZLayer}

private[redis] final case class RedisPubSubCommand(command: PubSubCommand, codec: BinaryCodec, executor: RedisPubSub) {
  def run[R: Schema](): IO[RedisError, Chunk[Stream[RedisError, R]]] = {
    val codecLayer = ZLayer.succeed(codec)
    executor
      .execute(command)
      .provideLayer(codecLayer)
      .map(
        _.map(
          _.mapZIO(msg =>
            ZIO
              .attempt(ArbitraryOutput[R]().unsafeDecode(msg)(codec))
              .refineToOrDie[RedisError]
          )
        )
      )
  }
}
