---
id: index
title: "Introduction to ZIO Redis"
sidebar_label: "ZIO Redis"
---

@PROJECT_BADGES@

## Introduction

[ZIO Redis](https://github.com/zio/zio-redis) is a ZIO-native Redis client.
It aims to provide a **type-safe** and **performant** API for accessing Redis
instances.

## Installation

To use ZIO Redis, add the following line to your `build.sbt`:

```scala
libraryDependencies += "dev.zio" %% "zio-redis" % "@VERSION@"
```

## Example

To execute our ZIO Redis effect, we should provide the `RedisExecutor` layer to that effect. To create this layer we
should also provide the following layers:

- **RedisConfig** — Using default one, will connect to the `localhost:6379` Redis instance.
- **BinaryCodec** — In this example, we are going to use the built-in `ProtobufCodec` codec from zio-schema project.

To run this example we should put following dependencies in our `build.sbt` file:

```scala
libraryDependencies ++= Seq(
  "dev.zio" %% "zio-redis" % "@VERSION@",
  "dev.zio" %% "zio-schema-protobuf" % "1.6.1"
)
```

```scala mdoc:compile-only
import zio._
import zio.redis._
import zio.redis.options.NonNegativeLong
import zio.schema._
import zio.schema.codec._

object ZIORedisExample extends ZIOAppDefault {
  
  object ProtobufCodecSupplier extends CodecSupplier {
    def get[A: Schema]: BinaryCodec[A] = ProtobufCodec.protobufCodec
  }
  
  val myApp: ZIO[Redis, RedisError, Unit] =
    for {
      redis <- ZIO.service[Redis]
      _     <- redis.set("myKey", 8L, expireAt = Some(SetExpire.Seconds(NonNegativeLong(60))))
      v     <- redis.get("myKey").returning[Long]
      _     <- Console.printLine(s"Value of myKey: $v").orDie
      _     <- redis.hSet("myHash", ("k1", 6), ("k2", 2))
      _     <- redis.rPush("myList", 1, 2, 3, 4)
      _     <- redis.sAdd("mySet", "a", "b", "a", "c")
    } yield ()

  override def run = 
    myApp.provide(Redis.local, ZLayer.succeed[CodecSupplier](ProtobufCodecSupplier))
}
```

## Testing

To test you can use the embedded redis instance by adding to your build:

```scala
libraryDependencies := "dev.zio" %% "zio-redis-embedded" % "@VERSION@"
```

Then you can supply `EmbeddedRedis.layer.orDie` as your `RedisConfig` and you're good to go!

```scala mdoc:compile-only
import zio._
import zio.redis._
import zio.redis.embedded.EmbeddedRedis
import zio.schema.{DeriveSchema, Schema}
import zio.schema.codec.{BinaryCodec, ProtobufCodec}
import zio.test._
import zio.test.Assertion._
import java.util.UUID

object EmbeddedRedisSpec extends ZIOSpecDefault {
  object ProtobufCodecSupplier extends CodecSupplier {
    def get[A: Schema]: BinaryCodec[A] = ProtobufCodec.protobufCodec
  }
  
  final case class Item private (id: UUID, name: String, quantity: Int)
  object Item {
    implicit val itemSchema: Schema[Item] = DeriveSchema.gen[Item]
  }
  
  def spec = suite("EmbeddedRedis should")(
    test("set and get values") {
      for {
        redis <- ZIO.service[Redis]
        item   = Item(UUID.randomUUID, "foo", 2)
        _     <- redis.set(s"item.${item.id.toString}", item)
        found <- redis.get(s"item.${item.id.toString}").returning[Item]
      } yield assert(found)(isSome(equalTo(item)))
    }
  ).provideShared(
    EmbeddedRedis.layer,
    ZLayer.succeed[CodecSupplier](ProtobufCodecSupplier),
    Redis.singleNode
  ) @@ TestAspect.silentLogging
}
```

## Resources

- [ZIO Redis](https://www.youtube.com/watch?v=yqFt3b3RBkI) by Dejan Mijic — Redis is one of the most commonly used
  in-memory data structure stores. In this talk, Dejan will introduce ZIO Redis, a purely functional, strongly typed
  client library backed by ZIO, with excellent performance and extensive support for nearly all of Redis' features. He
  will explain the library design using the bottom-up approach - from communication protocol to public APIs. Finally, he
  will wrap the talk by demonstrating the client's usage and discussing its performance characteristics.
