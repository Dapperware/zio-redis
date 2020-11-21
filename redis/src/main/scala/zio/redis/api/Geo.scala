package zio.redis.api

import zio.redis.Input._
import zio.redis.Output._
import zio.redis._
import zio.{ Chunk, ZIO }

trait Geo {
  import Geo._

  final def geoAdd(a: String, b: (LongLat, String), bs: (LongLat, String)*): ZIO[RedisExecutor, RedisError, Long] =
    GeoAdd.run((a, (b, bs.toList)))

  final def geoDist(
    a: String,
    b: String,
    c: String,
    d: Option[RadiusUnit] = None
  ): ZIO[RedisExecutor, RedisError, Option[Double]] = GeoDist.run((a, b, c, d))

  final def geoHash(a: String, b: String, bs: String*): ZIO[RedisExecutor, RedisError, Chunk[String]] =
    GeoHash.run((a, (b, bs.toList)))

  final def geoPos(a: String, b: String, bs: String*): ZIO[RedisExecutor, RedisError, Chunk[LongLat]] =
    GeoPos.run((a, (b, bs.toList)))

  final def geoRadius(
    a: String,
    b: LongLat,
    c: Double,
    d: RadiusUnit,
    e: Option[WithCoord] = None,
    f: Option[WithDist] = None,
    g: Option[WithHash] = None,
    h: Option[Count] = None,
    i: Option[Order] = None,
    j: Option[Store] = None,
    k: Option[StoreDist] = None
  ): ZIO[RedisExecutor, RedisError, Chunk[GeoView]] = GeoRadius.run((a, b, c, d, e, f, g, h, i, j, k))

  final def geoRadiusByMember(
    a: String,
    b: String,
    c: Double,
    d: RadiusUnit,
    e: Option[WithCoord] = None,
    f: Option[WithDist] = None,
    g: Option[WithHash] = None,
    h: Option[Count] = None,
    i: Option[Order] = None,
    j: Option[Store] = None,
    k: Option[StoreDist] = None
  ): ZIO[RedisExecutor, RedisError, Chunk[GeoView]] = GeoRadiusByMember.run((a, b, c, d, e, f, g, h, i, j, k))
}

private[redis] object Geo {
  final val GeoAdd =
    RedisCommand("GEOADD", Tuple2(StringInput, NonEmptyList(Tuple2(LongLatInput, StringInput))), LongOutput)

  final val GeoDist =
    RedisCommand(
      "GEODIST",
      Tuple4(StringInput, StringInput, StringInput, OptionalInput(RadiusUnitInput)),
      OptionalOutput(DoubleOutput)
    )

  final val GeoHash = RedisCommand("GEOHASH", Tuple2(StringInput, NonEmptyList(StringInput)), ChunkOutput)

  final val GeoPos = RedisCommand("GEOPOS", Tuple2(StringInput, NonEmptyList(StringInput)), GeoOutput)

  final val GeoRadius =
    RedisCommand(
      "GEORADIUS",
      Tuple11(
        StringInput,
        LongLatInput,
        DoubleInput,
        RadiusUnitInput,
        OptionalInput(WithCoordInput),
        OptionalInput(WithDistInput),
        OptionalInput(WithHashInput),
        OptionalInput(CountInput),
        OptionalInput(OrderInput),
        OptionalInput(StoreInput),
        OptionalInput(StoreDistInput)
      ),
      GeoRadiusOutput
    )

  final val GeoRadiusByMember =
    RedisCommand(
      "GEORADIUSBYMEMBER",
      Tuple11(
        StringInput,
        StringInput,
        DoubleInput,
        RadiusUnitInput,
        OptionalInput(WithCoordInput),
        OptionalInput(WithDistInput),
        OptionalInput(WithHashInput),
        OptionalInput(CountInput),
        OptionalInput(OrderInput),
        OptionalInput(StoreInput),
        OptionalInput(StoreDistInput)
      ),
      GeoRadiusOutput
    )
}
