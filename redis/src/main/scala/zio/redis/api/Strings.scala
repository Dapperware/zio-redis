package zio.redis.api

import java.time.Instant

import zio.duration._
import zio.redis.Input._
import zio.redis.Output._
import zio.redis._
import zio.schema.Schema
import zio.{ Chunk, ZIO }

trait Strings {
  import Strings._

  /**
   * Append a value to a key.
   *
   * @param key Key of the string to add the value to
   * @param value Value to append to the string
   * @return Returns the length of the string after the append operation.
   */
  final def append[K: Schema, V: Schema](key: K, value: V): ZIO[RedisExecutor, RedisError, Long] = {
    val command = RedisCommand(Append, Tuple2(ArbitraryInput[K](), ArbitraryInput[V]()), LongOutput)
    command.run((key, value))
  }

  /**
   * Count set bits in a string.
   *
   * @param key Key of the string of which to count the bits
   * @param range Range of bytes to count
   * @return Returns the number of bits set to 1.
   */
  final def bitCount[K: Schema](key: K, range: Option[Range] = None): ZIO[RedisExecutor, RedisError, Long] = {
    val command = RedisCommand(BitCount, Tuple2(ArbitraryInput[K](), OptionalInput(RangeInput)), LongOutput)
    command.run((key, range))
  }

  /**
   * Perform arbitrary bitfield integer operations on strings.
   *
   * @param key Key of the string to operate on
   * @param bitFieldCommand First command to apply
   * @param bitFieldCommands Subsequent commands to apply
   * @return Returns an optional long result of each command applied.
   */
  final def bitField[K: Schema](
    key: K,
    bitFieldCommand: BitFieldCommand,
    bitFieldCommands: BitFieldCommand*
  ): ZIO[RedisExecutor, RedisError, Chunk[Option[Long]]] = {
    val command = RedisCommand(
      BitField,
      Tuple2(ArbitraryInput[K](), NonEmptyList(BitFieldCommandInput)),
      ChunkOutput(OptionalOutput(LongOutput))
    )
    command.run((key, (bitFieldCommand, bitFieldCommands.toList)))
  }

  /**
   * Perform bitwise operations between strings.
   *
   * @param operation Bit operation to apply
   * @param destKey Key of destination string to store the result
   * @param srcKey First source key to apply the operation to
   * @param srcKeys Subsequent source keys to apply the operation to
   * @return Returns size of the string stored in the destination key, that is equal to the size of the longest input
   *         string.
   */
  final def bitOp[D: Schema, S: Schema](
    operation: BitOperation,
    destKey: D,
    srcKey: S,
    srcKeys: S*
  ): ZIO[RedisExecutor, RedisError, Long] = {
    val command =
      RedisCommand(BitOp, Tuple3(BitOperationInput, ArbitraryInput[D](), NonEmptyList(ArbitraryInput[S]())), LongOutput)
    command.run((operation, destKey, (srcKey, srcKeys.toList)))
  }

  /**
   * Find first bit set or clear in a string.
   *
   * @param key Key of the string to search within
   * @param bit Whether to search for a set bit or a cleared bit
   * @param range Range of bytes to search
   * @return Returns the position of the first bit set to 1 or 0 according to the request.
   */
  final def bitPos[K: Schema](
    key: K,
    bit: Boolean,
    range: Option[BitPosRange] = None
  ): ZIO[RedisExecutor, RedisError, Long] = {
    val command =
      RedisCommand(BitPos, Tuple3(ArbitraryInput[K](), BoolInput, OptionalInput(BitPosRangeInput)), LongOutput)
    command.run((key, bit, range))
  }

  /**
   * Decrement the integer value of a key by one.
   *
   * @param key Key to decrement
   * @return Returns the value of key after the decrement.
   */
  final def decr[K: Schema](key: K): ZIO[RedisExecutor, RedisError, Long] = {
    val command = RedisCommand(Decr, ArbitraryInput[K](), LongOutput)
    command.run(key)
  }

  /**
   * Decrement the integer value of a key by the given number.
   *
   * @param key Key of the integer value to decrement
   * @param decrement Amount to decrement by
   * @return Returns the value of key after the decrement.
   */
  final def decrBy[K: Schema](key: K, decrement: Long): ZIO[RedisExecutor, RedisError, Long] = {
    val command = RedisCommand(DecrBy, Tuple2(ArbitraryInput[K](), LongInput), LongOutput)
    command.run((key, decrement))
  }

  /**
   * Get the value of a key.
   *
   * @param key Key to get the value of
   * @return Returns the value of the string or None if it does not exist.
   */
  final def get[K: Schema](key: K): ResultBuilder[Option] =
    new ResultBuilder[Option] {
      def returning[R: Schema]: ZIO[RedisExecutor, RedisError, Option[R]] =
        RedisCommand(Get, ArbitraryInput[K](), OptionalOutput(ArbitraryOutput[R]())).run(key)
    }

  /**
   * Returns the bit value at offset in the string value stored at key.
   *
   * @param key Key of the string to get the bit from
   * @param offset Offset to the bit
   * @return Returns the bit value stored at offset.
   */
  final def getBit[K: Schema](key: K, offset: Long): ZIO[RedisExecutor, RedisError, Long] = {
    val command = RedisCommand(GetBit, Tuple2(ArbitraryInput[K](), LongInput), LongOutput)
    command.run((key, offset))
  }

  /**
   * Get a substring of the string stored at key.
   *
   * @param key Key of the string to get a substring of
   * @param range Range of the substring
   * @return Returns the substring.
   */
  final def getRange[K: Schema](key: K, range: Range): ResultBuilder[Option] =
    new ResultBuilder[Option] {
      def returning[R: Schema]: ZIO[RedisExecutor, RedisError, Option[R]] =
        RedisCommand(GetRange, Tuple2(ArbitraryInput[K](), RangeInput), OptionalOutput(ArbitraryOutput[R]()))
          .run((key, range))
    }

  /**
   * Set the string value of a key and return its old value.
   *
   * @param key Key of string to set
   * @param value New value of the string
   * @return Returns the previous value of the string or None if it did not previously have a value.
   */
  final def getSet[K: Schema, V: Schema](key: K, value: V): ResultBuilder[Option] =
    new ResultBuilder[Option] {
      def returning[R: Schema]: ZIO[RedisExecutor, RedisError, Option[R]] =
        RedisCommand(GetSet, Tuple2(ArbitraryInput[K](), ArbitraryInput[V]()), OptionalOutput(ArbitraryOutput[R]()))
          .run((key, value))
    }

  /**
   * Get the string value of a key and delete it on success (if and only if the key's value type is a string).
   *
   * @param key Key to get the value of
   * @return Returns the value of the string or None if it did not previously have a value.
   */
  final def getDel[K: Schema](key: K): ResultBuilder[Option] =
    new ResultBuilder[Option] {
      def returning[R: Schema]: ZIO[RedisExecutor, RedisError, Option[R]] =
        RedisCommand(GetDel, ArbitraryInput[K](), OptionalOutput(ArbitraryOutput[R]())).run(key)
    }

  /**
   * Get the value of key and set its expiration.
   *
   * @param key Key to get the value of
   * @param expire The option which can modify command behavior. e.g. use `Expire.SetExpireSeconds` set the specified
   *               expire time in seconds
   * @param expireTime Time in seconds/milliseconds until the string should expire
   * @return Returns the value of the string or None if it did not previously have a value.
   */
  final def getEx[K: Schema](key: K, expire: Expire, expireTime: Duration): ResultBuilder[Option] =
    new ResultBuilder[Option] {
      def returning[R: Schema]: ZIO[RedisExecutor, RedisError, Option[R]] =
        RedisCommand(GetEx, GetExInput[K](), OptionalOutput(ArbitraryOutput[R]())).run((key, expire, expireTime))
    }

  /**
   * Get the value of key and set its expiration.
   *
   * @param key Key to get the value of
   * @param expiredAt The option which can modify command behavior. e.g. use `Expire.SetExpireAtSeconds` set the
   *                  specified Unix time at which the key will expire in seconds
   * @param timestamp an absolute Unix timestamp (seconds/milliseconds since January 1, 1970)
   * @return Returns the value of the string or None if it did not previously have a value.
   */
  final def getEx[K: Schema](key: K, expiredAt: ExpiredAt, timestamp: Instant): ResultBuilder[Option] =
    new ResultBuilder[Option] {
      def returning[R: Schema]: ZIO[RedisExecutor, RedisError, Option[R]] =
        RedisCommand(GetEx, GetExAtInput[K](), OptionalOutput(ArbitraryOutput[R]())).run((key, expiredAt, timestamp))
    }

  /**
   * Get the value of key and remove the time to live associated with the key.
   *
   * @param key Key to get the value of
   * @param persist if true, remove the time to live associated with the key, otherwise not
   * @return Returns the value of the string or None if it did not previously have a value.
   */
  final def getEx[K: Schema](key: K, persist: Boolean): ResultBuilder[Option] =
    new ResultBuilder[Option] {
      def returning[R: Schema]: ZIO[RedisExecutor, RedisError, Option[R]] =
        RedisCommand(GetEx, GetExPersistInput[K](), OptionalOutput(ArbitraryOutput[R]())).run((key, persist))
    }

  /**
   * Increment the integer value of a key by one.
   *
   * @param key Key of the string to increment
   * @return Returns the value of key after the increment.
   */
  final def incr[K: Schema](key: K): ZIO[RedisExecutor, RedisError, Long] = {
    val command = RedisCommand(Incr, ArbitraryInput[K](), LongOutput)
    command.run(key)
  }

  /**
   * Increment the integer value of a key by the given amount.
   *
   * @param key Key of the value to increment
   * @param increment Amount to increment the value by
   * @return Returns the value of key after the increment.
   */
  final def incrBy[K: Schema](key: K, increment: Long): ZIO[RedisExecutor, RedisError, Long] = {
    val command =
      RedisCommand(IncrBy, Tuple2(ArbitraryInput[K](), LongInput), LongOutput)
    command.run((key, increment))
  }

  /**
   * Increment the float value of a key by the given amount.
   *
   * @param key Key of the value to increment
   * @param increment Amount to increment the value by
   * @return Returns the value of key after the increment.
   */
  final def incrByFloat[K: Schema, R: Schema](key: K, increment: Double): ZIO[RedisExecutor, RedisError, R] = {
    val command = RedisCommand(IncrByFloat, Tuple2(ArbitraryInput[K](), DoubleInput), ArbitraryOutput[R]())
    command.run((key, increment))
  }

  /**
   * Get all the values of the given keys.
   *
   * @param key First key to get
   * @param keys Subsequent keys to get
   * @return Returns the values of the given keys.
   */
  final def mGet[K: Schema, R: Schema](key: K, keys: K*): ZIO[RedisExecutor, RedisError, Chunk[Option[R]]] = {
    val command =
      RedisCommand(MGet, NonEmptyList(ArbitraryInput[K]()), ChunkOutput(OptionalOutput(ArbitraryOutput[R]())))
    command.run((key, keys.toList))
  }

  /**
   * Set multiple keys to multiple values.
   *
   * @param keyValue Tuple of key and value, first one to set
   * @param keyValues Subsequent tuples of key values
   */
  final def mSet[K: Schema, V: Schema](keyValue: (K, V), keyValues: (K, V)*): ZIO[RedisExecutor, RedisError, Unit] = {
    val command = RedisCommand(MSet, NonEmptyList(Tuple2(ArbitraryInput[K](), ArbitraryInput[V]())), UnitOutput)
    command.run((keyValue, keyValues.toList))
  }

  /**
   * Set multiple keys to multiple values only if none of the keys exist.
   *
   * @param keyValue First key value to set
   * @param keyValues Subsequent key values to set
   * @return 1 if the all the keys were set. 0 if no key was set (at least one key already existed).
   */
  final def mSetNx[K: Schema, V: Schema](
    keyValue: (K, V),
    keyValues: (K, V)*
  ): ZIO[RedisExecutor, RedisError, Boolean] = {
    val command = RedisCommand(MSetNx, NonEmptyList(Tuple2(ArbitraryInput[K](), ArbitraryInput[V]())), BoolOutput)
    command.run((keyValue, keyValues.toList))
  }

  /**
   * Set the value and expiration in milliseconds of a key.
   *
   * @param key Key of the string to set the expiry time on
   * @param milliseconds Time in milliseconds until the string should expire
   * @param value Value to set
   */
  final def pSetEx[K: Schema, V: Schema](
    key: K,
    milliseconds: Duration,
    value: V
  ): ZIO[RedisExecutor, RedisError, Unit] = {
    val command =
      RedisCommand(PSetEx, Tuple3(ArbitraryInput[K](), DurationMillisecondsInput, ArbitraryInput[V]()), UnitOutput)
    command.run((key, milliseconds, value))
  }

  /**
   * Set the string value of a key.
   *
   * @param key Key of the string to set
   * @param value Value to set
   * @param expireTime Time until the string expires
   * @param update Update can be Update.SetExisting which only sets the key if it exists, or Update.SetNew which nly sets the key if it does not exist
   * @param keepTtl When set any previously set expire time remains unchanged
   * @return true if set was executed correctly, false otherwise.
   */
  final def set[K: Schema, V: Schema](
    key: K,
    value: V,
    expireTime: Option[Duration] = None,
    update: Option[Update] = None,
    keepTtl: Option[KeepTtl] = None
  ): ZIO[RedisExecutor, RedisError, Boolean] = {
    val input = Tuple5(
      ArbitraryInput[K](),
      ArbitraryInput[V](),
      OptionalInput(DurationTtlInput),
      OptionalInput(UpdateInput),
      OptionalInput(KeepTtlInput)
    )
    val command = RedisCommand(Set, input, SetOutput)
    command.run((key, value, expireTime, update, keepTtl))
  }

  /**
   * Sets or clears the bit at offset in the string value stored at key.
   *
   * @param key Key of the string to set or clear bits
   * @param offset Offset at which to set or clear the bit
   * @param value True if bit should be set, False if it should be cleared
   * @return Returns the original bit value stored at offset.
   */
  final def setBit[K: Schema](key: K, offset: Long, value: Boolean): ZIO[RedisExecutor, RedisError, Boolean] = {
    val command = RedisCommand(SetBit, Tuple3(ArbitraryInput[K](), LongInput, BoolInput), BoolOutput)
    command.run((key, offset, value))
  }

  /**
   * Set the value and expiration of a key.
   *
   * @param key Key of the value to update
   * @param expiration Expiration time for the value
   * @param value New value to set
   */
  final def setEx[K: Schema, V: Schema](
    key: K,
    expiration: Duration,
    value: V
  ): ZIO[RedisExecutor, RedisError, Unit] = {
    val command =
      RedisCommand(SetEx, Tuple3(ArbitraryInput[K](), DurationSecondsInput, ArbitraryInput[V]()), UnitOutput)
    command.run((key, expiration, value))
  }

  /**
   * Set the value of a key, only if the key does not exist.
   *
   * @param key Key of the value to set if the key does not exist
   * @param value Value to set
   * @return Returns 1 if the key was set. 0 if the key was not set.
   */
  final def setNx[K: Schema, V: Schema](key: K, value: V): ZIO[RedisExecutor, RedisError, Boolean] = {
    val command = RedisCommand(SetNx, Tuple2(ArbitraryInput[K](), ArbitraryInput[V]()), BoolOutput)
    command.run((key, value))
  }

  /**
   * Overwrite part of a string at key starting at the specified offset.
   *
   * @param key Key of the string to overwite
   * @param offset Offset to start writing
   * @param value Value to overwrite with
   * @return Returns the length of the string after it was modified by the command.
   */
  final def setRange[K: Schema, V: Schema](key: K, offset: Long, value: V): ZIO[RedisExecutor, RedisError, Long] = {
    val command = RedisCommand(SetRange, Tuple3(ArbitraryInput[K](), LongInput, ArbitraryInput[V]()), LongOutput)
    command.run((key, offset, value))
  }

  /**
   * Get the length of a value stored in a key.
   *
   * @param key Key of the string to get the length of
   * @return Returns the length of the string.
   */
  final def strLen[K: Schema](key: K): ZIO[RedisExecutor, RedisError, Long] = {
    val command = RedisCommand(StrLen, ArbitraryInput[K](), LongOutput)
    command.run(key)
  }

  /**
   * Get the longest common subsequence of values stored in the given keys.
   *
   * @param command type of value it (possible values are Strings and Keys)
   * @param keyA first value that will contain subsequence
   * @param keyB second value that will contain subsequence
   * @param lcsQueryType modifier that will affect the output
   * @return Without modifiers returns the string representing the longest common substring. When LEN is given the
   *         command returns the length of the longest common substring. When IDX is given the command returns an array
   *         with the LCS length and all the ranges in both the strings, start and end offset for each string, where
   *         there are matches. When withMatchLen is given each array representing a match will also have the length of
   *         the match (see examples).
   */
  final def stralgoLcs[K: Schema](
    command: StrAlgoLCS,
    keyA: K,
    keyB: K,
    lcsQueryType: Option[StrAlgoLcsQueryType] = None
  ): ZIO[RedisExecutor, RedisError, LcsOutput] = {
    val redisCommand = RedisCommand(
      StrAlgoLcs,
      Tuple4(
        ArbitraryInput[String](),
        ArbitraryInput[K](),
        ArbitraryInput[K](),
        OptionalInput(StralgoLcsQueryTypeInput)
      ),
      StrAlgoLcsOutput
    )
    redisCommand.run((command.stringify, keyA, keyB, lcsQueryType))
  }
}

private[redis] object Strings {
  final val Append      = "APPEND"
  final val BitCount    = "BITCOUNT"
  final val BitField    = "BITFIELD"
  final val BitOp       = "BITOP"
  final val BitPos      = "BITPOS"
  final val Decr        = "DECR"
  final val DecrBy      = "DECRBY"
  final val Get         = "GET"
  final val GetBit      = "GETBIT"
  final val GetRange    = "GETRANGE"
  final val GetSet      = "GETSET"
  final val Incr        = "INCR"
  final val IncrBy      = "INCRBY"
  final val IncrByFloat = "INCRBYFLOAT"
  final val MGet        = "MGET"
  final val MSet        = "MSET"
  final val MSetNx      = "MSETNX"
  final val PSetEx      = "PSETEX"
  final val Set         = "SET"
  final val SetBit      = "SETBIT"
  final val SetEx       = "SETEX"
  final val SetNx       = "SETNX"
  final val SetRange    = "SETRANGE"
  final val StrLen      = "STRLEN"
  final val StrAlgoLcs  = "STRALGO LCS"
  final val GetDel      = "GETDEL"
  final val GetEx       = "GETEX"
}
