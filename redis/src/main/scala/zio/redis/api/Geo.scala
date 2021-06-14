package zio.redis.api

import zio.redis.Input._
import zio.redis.Output._
import zio.redis._
import zio.schema.Schema
import zio.{ Chunk, ZIO }

trait Geo {
  import Geo._

  /**
   *  Adds the specified geospatial `items` (latitude, longitude, name) to the specified `key`.
   *
   *  @param key sorted set where the items will be stored
   *  @param item tuple of (latitude, longitude, name) to add
   *  @param items additional items
   *  @return number of new elements added to the sorted set.
   */
  final def geoAdd[K: Schema, M: Schema](
    key: K,
    item: (LongLat, M),
    items: (LongLat, M)*
  ): ZIO[RedisExecutor, RedisError, Long] = {
    val command = RedisCommand(
      GeoAdd,
      Tuple2(ArbitraryInput[K](), NonEmptyList(Tuple2(LongLatInput, ArbitraryInput[M]()))),
      LongOutput
    )
    command.run((key, (item, items.toList)))
  }

  /**
   *  Return the distance between two members in the geospatial index represented by the sorted set.
   *
   *  @param key sorted set of geospatial members
   *  @param member1 member in set
   *  @param member2 member in set
   *  @param radiusUnit Unit of distance ("m", "km", "ft", "mi")
   *  @return distance between the two specified members in the specified unit or None if either member is missing.
   */
  final def geoDist[K: Schema, M: Schema](
    key: K,
    member1: M,
    member2: M,
    radiusUnit: Option[RadiusUnit] = None
  ): ZIO[RedisExecutor, RedisError, Option[Double]] = {
    val command = RedisCommand(
      GeoDist,
      Tuple4(ArbitraryInput[K](), ArbitraryInput[M](), ArbitraryInput[M](), OptionalInput(RadiusUnitInput)),
      OptionalOutput(DoubleOutput)
    )
    command.run((key, member1, member2, radiusUnit))
  }

  /**
   *  Return valid Geohash strings representing the position of one or more elements in a sorted set value representing
   *  a geospatial index.
   *
   *  @param key sorted set of geospatial members
   *  @param member member in set
   *  @param members additional members
   *  @return chunk of geohashes, where value is `None` if a member is not in the set.
   */
  final def geoHash[K: Schema, M: Schema](
    key: K,
    member: M,
    members: M*
  ): ZIO[RedisExecutor, RedisError, Chunk[Option[String]]] = {
    val command = RedisCommand(
      GeoHash,
      Tuple2(ArbitraryInput[K](), NonEmptyList(ArbitraryInput[M]())),
      ChunkOutput(OptionalOutput(MultiStringOutput))
    )
    command.run((key, (member, members.toList)))
  }

  /**
   *  Return the positions (longitude, latitude) of all the specified members of the geospatial index represented by the
   *  sorted set at `key`.
   *
   *  @param key sorted set of geospatial members
   *  @param member member in the set
   *  @param members additional members
   *  @return chunk of positions, where value is `None` if a member is not in the set.
   */
  final def geoPos[K: Schema, M: Schema](
    key: K,
    member: M,
    members: M*
  ): ZIO[RedisExecutor, RedisError, Chunk[Option[LongLat]]] = {
    val command = RedisCommand(GeoPos, Tuple2(ArbitraryInput[K](), NonEmptyList(ArbitraryInput[M]())), GeoOutput)
    command.run((key, (member, members.toList)))
  }

  /**
   *  Return geospatial members of a sorted set which are within the area specified with a *center location* and the
   *  *maximum distance from the center*.
   *
   *  @param key sorted set of geospatial members
   *  @param center position
   *  @param radius distance from the center
   *  @param radiusUnit Unit of distance ("m", "km", "ft", "mi")
   *  @param withCoord flag to include the position of each member in the result
   *  @param withDist flag to include the distance of each member from the center in the result
   *  @param withHash flag to include raw geohash sorted set score of each member in the result
   *  @param count limit the results to the first N matching items
   *  @param order sort returned items in the given `Order`
   *  @return chunk of members within the specified are.
   */
  final def geoRadius[K: Schema](
    key: K,
    center: LongLat,
    radius: Double,
    radiusUnit: RadiusUnit,
    withCoord: Option[WithCoord] = None,
    withDist: Option[WithDist] = None,
    withHash: Option[WithHash] = None,
    count: Option[Count] = None,
    order: Option[Order] = None
  ): ZIO[RedisExecutor, RedisError, Chunk[GeoView]] = {
    val command = RedisCommand(
      GeoRadius,
      Tuple9(
        ArbitraryInput[K](),
        LongLatInput,
        DoubleInput,
        RadiusUnitInput,
        OptionalInput(WithCoordInput),
        OptionalInput(WithDistInput),
        OptionalInput(WithHashInput),
        OptionalInput(CountInput),
        OptionalInput(OrderInput)
      ),
      GeoRadiusOutput
    )
    command.run((key, center, radius, radiusUnit, withCoord, withDist, withHash, count, order))
  }

  /**
   * Similar to geoRadius, but store the results to the argument passed to store and return the number of
   * elements stored. Added as a separate Scala function because it returns a Long instead of the list of results.
   * Find the geospatial members of a sorted set which are within the area specified with a *center location* and the
   * *maximum distance from the center*.
   *
   *  @param key sorted set of geospatial members
   *  @param center position
   *  @param radius distance from the center
   *  @param store sorted set where the results and/or distances should be stored
   *  @param radiusUnit Unit of distance ("m", "km", "ft", "mi")
   *  @param withCoord flag to include the position of each member in the result
   *  @param withDist flag to include the distance of each member from the center in the result
   *  @param withHash flag to include raw geohash sorted set score of each member in the result
   *  @param count limit the results to the first N matching items
   *  @param order sort returned items in the given `Order`
   *  @return chunk of members within the specified are.
   *
   *  We expect at least one of Option[Store] and Option[StoreDist] to be passed here.
   */
  final def geoRadiusStore[K: Schema](
    key: K,
    center: LongLat,
    radius: Double,
    radiusUnit: RadiusUnit,
    store: StoreOptions,
    withCoord: Option[WithCoord] = None,
    withDist: Option[WithDist] = None,
    withHash: Option[WithHash] = None,
    count: Option[Count] = None,
    order: Option[Order] = None
  ): ZIO[RedisExecutor, RedisError, Long] = {
    val command = RedisCommand(
      GeoRadius,
      Tuple11(
        ArbitraryInput[K](),
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
      LongOutput
    )
    command.run(
      (key, center, radius, radiusUnit, withCoord, withDist, withHash, count, order, store.store, store.storeDist)
    )
  }

  /**
   *  Return geospatial members of a sorted set which are within the area specified with an *existing member* in the set
   *  and the *maximum distance from the location of that member*.
   *
   *  @param key sorted set of geospatial members
   *  @param member member in the set
   *  @param radius distance from the member
   *  @param radiusUnit Unit of distance ("m", "km", "ft", "mi")
   *  @param withCoord flag to include the position of each member in the result
   *  @param withDist flag to include the distance of each member from the center in the result
   *  @param withHash flag to include raw geohash sorted set score of each member in the result
   *  @param count limit the results to the first N matching items
   *  @param order sort returned items in the given `Order`
   *               number should be stored
   *  @return chunk of members within the specified area, or an error if the member is not in the set.
   */
  final def geoRadiusByMember[K: Schema, M: Schema](
    key: K,
    member: M,
    radius: Double,
    radiusUnit: RadiusUnit,
    withCoord: Option[WithCoord] = None,
    withDist: Option[WithDist] = None,
    withHash: Option[WithHash] = None,
    count: Option[Count] = None,
    order: Option[Order] = None
  ): ZIO[RedisExecutor, RedisError, Chunk[GeoView]] = {
    val command = RedisCommand(
      GeoRadiusByMember,
      Tuple9(
        ArbitraryInput[K](),
        ArbitraryInput[M](),
        DoubleInput,
        RadiusUnitInput,
        OptionalInput(WithCoordInput),
        OptionalInput(WithDistInput),
        OptionalInput(WithHashInput),
        OptionalInput(CountInput),
        OptionalInput(OrderInput)
      ),
      GeoRadiusOutput
    )
    command.run((key, member, radius, radiusUnit, withCoord, withDist, withHash, count, order))
  }

  /**
   * Similar to geoRadiusByMember, but store the results to the argument passed to store and return the number of
   * elements stored. Added as a separate Scala function because it returns a Long instead of the list of results.
   * Find geospatial members of a sorted set which are within the area specified with an *existing member* in the set
   * and the *maximum distance from the location of that member*.
   *
   *  @param key sorted set of geospatial members
   *  @param member member in the set
   *  @param radius distance from the member
   *  @param radiusUnit Unit of distance ("m", "km", "ft", "mi")
   *  @param store sorted set where the results and/or distances should be stored
   *  @param withCoord flag to include the position of each member in the result
   *  @param withDist flag to include the distance of each member from the center in the result
   *  @param withHash flag to include raw geohash sorted set score of each member in the result
   *  @param count limit the results to the first N matching items
   *  @param order sort returned items in the given `Order`
   *  @return chunk of members within the specified area, or an error if the member is not in the set.
   *
   *  We expect at least one of Option[Store] and Option[StoreDist] to be passed here.
   */
  final def geoRadiusByMemberStore[K: Schema, M: Schema](
    key: K,
    member: M,
    radius: Double,
    radiusUnit: RadiusUnit,
    store: StoreOptions,
    withCoord: Option[WithCoord] = None,
    withDist: Option[WithDist] = None,
    withHash: Option[WithHash] = None,
    count: Option[Count] = None,
    order: Option[Order] = None
  ): ZIO[RedisExecutor, RedisError, Long] = {
    val command = RedisCommand(
      GeoRadiusByMember,
      Tuple11(
        ArbitraryInput[K](),
        ArbitraryInput[M](),
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
      LongOutput
    )
    command.run(
      (key, member, radius, radiusUnit, withCoord, withDist, withHash, count, order, store.store, store.storeDist)
    )
  }
}

private[redis] object Geo {
  final val GeoAdd                 = "GEOADD"
  final val GeoDist                = "GEODIST"
  final val GeoHash                = "GEOHASH"
  final val GeoPos                 = "GEOPOS"
  final val GeoRadius              = "GEORADIUS"
  final val GeoRadiusByMember      = "GEORADIUSBYMEMBER"
}
