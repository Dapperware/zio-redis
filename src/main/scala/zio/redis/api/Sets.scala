package zio.redis.api

import zio.redis.RedisCommand
import zio.redis.Input._
import zio.redis.Output._

trait Sets {
  final val sAdd        = RedisCommand("SADD", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)
  final val sCard       = RedisCommand("SCARD", StringInput, LongOutput)
  final val sDiff       = RedisCommand("SDIFF", NonEmptyList(StringInput), ChunkOutput)
  final val sDiffStore  = RedisCommand("SDIFFSTORE", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)
  final val sInter      = RedisCommand("SINTER", NonEmptyList(StringInput), ChunkOutput)
  final val sInterStore = RedisCommand("SINTERSTORE", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)
  final val sIsMember   = RedisCommand("SISMEMBER", Tuple2(StringInput, StringInput), BoolOutput)
  final val sMembers    = RedisCommand("SMEMBERS", StringInput, ChunkOutput)
  final val sMove       = RedisCommand("SMOVE", Tuple3(StringInput, StringInput, StringInput), BoolOutput)
  final val sPop        = RedisCommand("SPOP", Tuple2(StringInput, OptionalInput(LongInput)), OptionalOutput(StringOutput))

  // TODO: can have 2 different outputs depending on whether or not count is provided
  final val sRandMember = RedisCommand("SRANDMEMBER", Tuple2(StringInput, OptionalInput(LongInput)), ChunkOutput)

  final val sRem = RedisCommand("SREM", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)

  final val sScan = RedisCommand(
    "SSCAN",
    Tuple4(LongInput, OptionalInput(RegexInput), OptionalInput(LongInput), OptionalInput(StringInput)),
    ScanOutput
  )

  final val sUnion      = RedisCommand("SUNION", NonEmptyList(StringInput), ChunkOutput)
  final val sUnionStore = RedisCommand("SUNIONSTORE", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)
}
