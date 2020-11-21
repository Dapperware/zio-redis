package zio.redis.api

import java.time.Duration

import scala.util.matching.Regex

import zio.redis.Input._
import zio.redis.Output._
import zio.redis._
import zio.{ Chunk, ZIO }

trait SortedSets {
  import SortedSets._

  final def bzPopMax(a: Duration, b: String, bs: String*): ZIO[RedisExecutor, RedisError, Chunk[String]] =
    BzPopMax.run((a, (b, bs.toList)))

  final def bzPopMin(a: Duration, b: String, bs: String*): ZIO[RedisExecutor, RedisError, Chunk[String]] =
    BzPopMin.run((a, (b, bs.toList)))

  final def zAdd(a: String, b: Option[Update] = None, c: Option[Changed] = None)(
    d: MemberScore,
    ds: MemberScore*
  ): ZIO[RedisExecutor, RedisError, Long] = ZAdd.run((a, b, c, (d, ds.toList)))

  final def zAddWithIncr(a: String, b: Option[Update] = None, c: Option[Changed] = None)(
    d: Increment,
    e: MemberScore,
    es: MemberScore*
  ): ZIO[RedisExecutor, RedisError, Option[Double]] = ZAddWithIncr.run((a, b, c, d, (e, es.toList)))

  final def zCard(a: String): ZIO[RedisExecutor, RedisError, Long] = ZCard.run(a)

  final def zCount(a: String, b: Range): ZIO[RedisExecutor, RedisError, Long] = ZCount.run((a, b))

  final def zIncrBy(a: String, b: Long, c: String): ZIO[RedisExecutor, RedisError, Double] = ZIncrBy.run((a, b, c))

  final def zInterStore(a: String, b: Long, c: String, cs: String*)(
    d: Option[Aggregate] = None,
    e: Option[::[Double]] = None
  ): ZIO[RedisExecutor, RedisError, Long] = ZInterStore.run((a, b, (c, cs.toList), d, e))

  final def zLexCount(a: String, b: LexRange): ZIO[RedisExecutor, RedisError, Long] = ZLexCount.run((a, b))

  final def zPopMax(a: String, b: Option[Long] = None): ZIO[RedisExecutor, RedisError, Chunk[String]] =
    ZPopMax.run((a, b))

  final def zPopMin(a: String, b: Option[Long] = None): ZIO[RedisExecutor, RedisError, Chunk[String]] =
    ZPopMin.run((a, b))

  final def zRange(a: String, b: Range, c: Option[WithScores] = None): ZIO[RedisExecutor, RedisError, Chunk[String]] =
    ZRange.run((a, b, c))

  final def zRangeByLex(
    a: String,
    b: LexRange,
    c: Option[Limit] = None
  ): ZIO[RedisExecutor, RedisError, Chunk[String]] =
    ZRangeByLex.run((a, b, c))

  final def zRangeByScore(
    a: String,
    b: ScoreRange,
    c: Option[WithScores] = None,
    d: Option[Limit] = None
  ): ZIO[RedisExecutor, RedisError, Chunk[String]] = ZRangeByScore.run((a, b, c, d))

  final def zRank(a: String, b: String): ZIO[RedisExecutor, RedisError, Option[Long]] = ZRank.run((a, b))

  final def zRem(a: String, b: String, bs: String*): ZIO[RedisExecutor, RedisError, Long] =
    ZRem.run((a, (b, bs.toList)))

  final def zRemRangeByLex(a: String, b: LexRange): ZIO[RedisExecutor, RedisError, Long] = ZRemRangeByLex.run((a, b))

  final def zRemRangeByRank(a: String, b: Range): ZIO[RedisExecutor, RedisError, Long] = ZRemRangeByRank.run((a, b))

  final def zRemRangeByScore(a: String, b: ScoreRange): ZIO[RedisExecutor, RedisError, Long] =
    ZRemRangeByScore.run((a, b))

  final def zRevRange(
    a: String,
    b: Range,
    c: Option[WithScores] = None
  ): ZIO[RedisExecutor, RedisError, Chunk[String]] =
    ZRevRange.run((a, b, c))

  final def zRevRangeByLex(
    a: String,
    b: LexRange,
    c: Option[Limit] = None
  ): ZIO[RedisExecutor, RedisError, Chunk[String]] =
    ZRevRangeByLex.run((a, b, c))

  final def zRevRangeByScore(
    a: String,
    b: ScoreRange,
    c: Option[WithScores] = None,
    d: Option[Limit] = None
  ): ZIO[RedisExecutor, RedisError, Chunk[String]] =
    ZRevRangeByScore.run((a, b, c, d))

  final def zRevRank(a: String, b: String): ZIO[RedisExecutor, RedisError, Option[Long]] = ZRevRank.run((a, b))

  final def zScan(
    a: String,
    b: Long,
    c: Option[Regex] = None,
    d: Option[Count] = None
  ): ZIO[RedisExecutor, RedisError, (String, Chunk[String])] = ZScan.run((a, b, c, d))

  final def zScore(a: String, b: String): ZIO[RedisExecutor, RedisError, Option[Double]] = ZScore.run((a, b))

  final def zUnionStore(a: String, b: Long, c: String, cs: String*)(
    d: Option[::[Double]] = None,
    e: Option[Aggregate] = None
  ): ZIO[RedisExecutor, RedisError, Long] = ZUnionStore.run((a, b, (c, cs.toList), d, e))
}

private[redis] object SortedSets {
  final val BzPopMax =
    RedisCommand("BZPOPMAX", Tuple2(DurationSecondsInput, NonEmptyList(StringInput)), ChunkOutput)
  final val BzPopMin =
    RedisCommand("BZPOPMIN", Tuple2(DurationSecondsInput, NonEmptyList(StringInput)), ChunkOutput)

  final val ZAdd =
    RedisCommand(
      "ZADD",
      Tuple4(
        StringInput,
        OptionalInput(UpdateInput),
        OptionalInput(ChangedInput),
        NonEmptyList(MemberScoreInput)
      ),
      LongOutput
    )

  final val ZAddWithIncr =
    RedisCommand(
      "ZADD",
      Tuple5(
        StringInput,
        OptionalInput(UpdateInput),
        OptionalInput(ChangedInput),
        IncrementInput,
        NonEmptyList(MemberScoreInput)
      ),
      OptionalOutput(DoubleOutput)
    )

  final val ZCard   = RedisCommand("ZCARD", StringInput, LongOutput)
  final val ZCount  = RedisCommand("ZCOUNT", Tuple2(StringInput, RangeInput), LongOutput)
  final val ZIncrBy = RedisCommand("ZINCRBY", Tuple3(StringInput, LongInput, StringInput), DoubleOutput)

  final val ZInterStore =
    RedisCommand(
      "ZINTERSTORE",
      Tuple5(
        StringInput,
        LongInput,
        NonEmptyList(StringInput),
        OptionalInput(AggregateInput),
        OptionalInput(WeightsInput)
      ),
      LongOutput
    )

  final val ZLexCount = RedisCommand("ZLEXCOUNT", Tuple2(StringInput, LexRangeInput), LongOutput)
  final val ZPopMax   = RedisCommand("ZPOPMAX", Tuple2(StringInput, OptionalInput(LongInput)), ChunkOutput)
  final val ZPopMin   = RedisCommand("ZPOPMIN", Tuple2(StringInput, OptionalInput(LongInput)), ChunkOutput)

  final val ZRange =
    RedisCommand("ZRANGE", Tuple3(StringInput, RangeInput, OptionalInput(WithScoresInput)), ChunkOutput)

  final val ZRangeByLex =
    RedisCommand("ZRANGEBYLEX", Tuple3(StringInput, LexRangeInput, OptionalInput(LimitInput)), ChunkOutput)

  final val ZRangeByScore =
    RedisCommand(
      "ZRANGEBYSCORE",
      Tuple4(StringInput, ScoreRangeInput, OptionalInput(WithScoresInput), OptionalInput(LimitInput)),
      ChunkOutput
    )

  final val ZRank            = RedisCommand("ZRANK", Tuple2(StringInput, StringInput), OptionalOutput(LongOutput))
  final val ZRem             = RedisCommand("ZREM", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)
  final val ZRemRangeByLex   = RedisCommand("ZREMRANGEBYLEX", Tuple2(StringInput, LexRangeInput), LongOutput)
  final val ZRemRangeByRank  = RedisCommand("ZREMRANGEBYRANK", Tuple2(StringInput, RangeInput), LongOutput)
  final val ZRemRangeByScore = RedisCommand("ZREMRANGEBYSCORE", Tuple2(StringInput, ScoreRangeInput), LongOutput)

  final val ZRevRange =
    RedisCommand("ZREVRANGE", Tuple3(StringInput, RangeInput, OptionalInput(WithScoresInput)), ChunkOutput)

  final val ZRevRangeByLex =
    RedisCommand("ZREVRANGEBYLEX", Tuple3(StringInput, LexRangeInput, OptionalInput(LimitInput)), ChunkOutput)

  final val ZRevRangeByScore =
    RedisCommand(
      "ZREVRANGEBYSCORE",
      Tuple4(StringInput, ScoreRangeInput, OptionalInput(WithScoresInput), OptionalInput(LimitInput)),
      ChunkOutput
    )

  final val ZRevRank = RedisCommand("ZREVRANK", Tuple2(StringInput, StringInput), OptionalOutput(LongOutput))

  final val ZScan =
    RedisCommand(
      "ZSCAN",
      Tuple4(StringInput, LongInput, OptionalInput(RegexInput), OptionalInput(CountInput)),
      ScanOutput
    )

  final val ZScore = RedisCommand("ZSCORE", Tuple2(StringInput, StringInput), OptionalOutput(DoubleOutput))

  final val ZUnionStore =
    RedisCommand(
      "ZUNIONSTORE",
      Tuple5(
        StringInput,
        LongInput,
        NonEmptyList(StringInput),
        OptionalInput(WeightsInput),
        OptionalInput(AggregateInput)
      ),
      LongOutput
    )
}
