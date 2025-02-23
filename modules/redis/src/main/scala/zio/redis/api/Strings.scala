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

package zio.redis.api

import zio._
import zio.redis.Input._
import zio.redis.Output._
import zio.redis.ResultBuilder._
import zio.redis._
import zio.redis.internal.{RedisCommand, RedisEnvironment}
import zio.schema.Schema

trait Strings[G[+_]] extends RedisEnvironment[G] {
  import Strings._

  /**
   * Append a value to a key.
   *
   * @param key
   *   Key of the string to add the value to
   * @param value
   *   Value to append to the string
   * @return
   *   Returns the length of the string after the append operation.
   */
  final def append[K: Schema, V: Schema](key: K, value: V): G[Long] = {
    val command =
      RedisCommand(Append, Tuple2(ArbitraryKeyInput[K](), ArbitraryValueInput[V]()), LongOutput)
    command.run((key, value))
  }

  /**
   * Count set bits in a string.
   *
   * @param key
   *   Key of the string of which to count the bits
   * @param range
   *   Range of bytes to count
   * @return
   *   Returns the number of bits set to 1.
   */
  final def bitCount[K: Schema](key: K, range: Option[Range] = None): G[Long] = {
    val command =
      RedisCommand(BitCount, Tuple2(ArbitraryKeyInput[K](), OptionalInput(RangeInput)), LongOutput)
    command.run((key, range))
  }

  /**
   * Perform arbitrary bitfield integer operations on strings.
   *
   * @param key
   *   Key of the string to operate on
   * @param bitFieldCommand
   *   First command to apply
   * @param bitFieldCommands
   *   Subsequent commands to apply
   * @return
   *   Returns an optional long result of each command applied.
   */
  final def bitField[K: Schema](
    key: K,
    bitFieldCommand: BitFieldCommand,
    bitFieldCommands: BitFieldCommand*
  ): G[Chunk[Option[Long]]] = {
    val command = RedisCommand(
      BitField,
      Tuple2(ArbitraryKeyInput[K](), NonEmptyList(BitFieldCommandInput)),
      ChunkOutput(OptionalOutput(LongOutput))
    )
    command.run((key, (bitFieldCommand, bitFieldCommands.toList)))
  }

  /**
   * Perform bitwise operations between strings.
   *
   * @param operation
   *   Bit operation to apply
   * @param destKey
   *   Key of destination string to store the result
   * @param srcKey
   *   First source key to apply the operation to
   * @param srcKeys
   *   Subsequent source keys to apply the operation to
   * @return
   *   Returns size of the string stored in the destination key, that is equal to the size of the longest input string.
   */
  final def bitOp[D: Schema, S: Schema](
    operation: BitOperation,
    destKey: D,
    srcKey: S,
    srcKeys: S*
  ): G[Long] = {
    val command =
      RedisCommand(
        BitOp,
        Tuple3(BitOperationInput, ArbitraryValueInput[D](), NonEmptyList(ArbitraryValueInput[S]())),
        LongOutput
      )
    command.run((operation, destKey, (srcKey, srcKeys.toList)))
  }

  /**
   * Find first bit set or clear in a string.
   *
   * @param key
   *   Key of the string to search within
   * @param bit
   *   Whether to search for a set bit or a cleared bit
   * @param range
   *   Range of bytes to search
   * @return
   *   Returns the position of the first bit set to 1 or 0 according to the request.
   */
  final def bitPos[K: Schema](
    key: K,
    bit: Boolean,
    range: Option[BitPosRange] = None
  ): G[Long] = {
    val command =
      RedisCommand(BitPos, Tuple3(ArbitraryKeyInput[K](), BoolInput, OptionalInput(BitPosRangeInput)), LongOutput)
    command.run((key, bit, range))
  }

  /**
   * Decrement the integer value of a key by one.
   *
   * @param key
   *   Key to decrement
   * @return
   *   Returns the value of key after the decrement.
   */
  final def decr[K: Schema](key: K): G[Long] = {
    val command = RedisCommand(Decr, ArbitraryKeyInput[K](), LongOutput)
    command.run(key)
  }

  /**
   * Decrement the integer value of a key by the given number.
   *
   * @param key
   *   Key of the integer value to decrement
   * @param decrement
   *   Amount to decrement by
   * @return
   *   Returns the value of key after the decrement.
   */
  final def decrBy[K: Schema](key: K, decrement: Long): G[Long] = {
    val command = RedisCommand(DecrBy, Tuple2(ArbitraryKeyInput[K](), LongInput), LongOutput)
    command.run((key, decrement))
  }

  /**
   * Get the value of a key.
   *
   * @param key
   *   Key to get the value of
   * @return
   *   Returns the value of the string or None if it does not exist.
   */
  final def get[K: Schema](key: K): ResultBuilder1[Option, G] =
    new ResultBuilder1[Option, G] {
      def returning[R: Schema]: G[Option[R]] =
        RedisCommand(Get, ArbitraryKeyInput[K](), OptionalOutput(ArbitraryOutput[R]())).run(key)
    }

  /**
   * Returns the bit value at offset in the string value stored at key.
   *
   * @param key
   *   Key of the string to get the bit from
   * @param offset
   *   Offset to the bit
   * @return
   *   Returns the bit value stored at offset.
   */
  final def getBit[K: Schema](key: K, offset: Long): G[Long] = {
    val command = RedisCommand(GetBit, Tuple2(ArbitraryKeyInput[K](), LongInput), LongOutput)
    command.run((key, offset))
  }

  /**
   * Get the string value of a key and delete it on success (if and only if the key's value type is a string).
   *
   * @param key
   *   Key to get the value of
   * @return
   *   Returns the value of the string or None if it did not previously have a value.
   */
  final def getDel[K: Schema](key: K): ResultBuilder1[Option, G] =
    new ResultBuilder1[Option, G] {
      def returning[R: Schema]: G[Option[R]] =
        RedisCommand(GetDel, ArbitraryKeyInput[K](), OptionalOutput(ArbitraryOutput[R]())).run(key)
    }

  /**
   * Get the value of key and set its expiration.
   *
   * @param key
   *   Key to get the value of
   * @param expire
   *   The option which can modify command behavior. e.g. use `Expire.SetExpireSeconds` set the specified expire time in
   *   seconds
   * @return
   *   Returns the value of the string or None if it did not previously have a value.
   */
  final def getEx[K: Schema](key: K, expire: GetExpire): ResultBuilder1[Option, G] =
    new ResultBuilder1[Option, G] {
      def returning[R: Schema]: G[Option[R]] =
        RedisCommand(GetEx, GetExInput[K](), OptionalOutput(ArbitraryOutput[R]())).run((key, expire))
    }

  /**
   * Get a substring of the string stored at key.
   *
   * @param key
   *   Key of the string to get a substring of
   * @param range
   *   Range of the substring
   * @return
   *   Returns the substring.
   */
  final def getRange[K: Schema](key: K, range: Range): ResultBuilder1[Option, G] =
    new ResultBuilder1[Option, G] {
      def returning[R: Schema]: G[Option[R]] =
        RedisCommand(GetRange, Tuple2(ArbitraryKeyInput[K](), RangeInput), OptionalOutput(ArbitraryOutput[R]()))
          .run((key, range))
    }

  /**
   * Set the string value of a key and return its old value.
   *
   * @param key
   *   Key of string to set
   * @param value
   *   New value of the string
   * @return
   *   Returns the previous value of the string or None if it did not previously have a value.
   */
  final def getSet[K: Schema, V: Schema](key: K, value: V): ResultBuilder1[Option, G] =
    new ResultBuilder1[Option, G] {
      def returning[R: Schema]: G[Option[R]] =
        RedisCommand(
          GetSet,
          Tuple2(ArbitraryKeyInput[K](), ArbitraryValueInput[V]()),
          OptionalOutput(ArbitraryOutput[R]())
        ).run((key, value))
    }

  /**
   * Increment the integer value of a key by one.
   *
   * @param key
   *   Key of the string to increment
   * @return
   *   Returns the value of key after the increment.
   */
  final def incr[K: Schema](key: K): G[Long] = {
    val command = RedisCommand(Incr, ArbitraryKeyInput[K](), LongOutput)
    command.run(key)
  }

  /**
   * Increment the integer value of a key by the given amount.
   *
   * @param key
   *   Key of the value to increment
   * @param increment
   *   Amount to increment the value by
   * @return
   *   Returns the value of key after the increment.
   */
  final def incrBy[K: Schema](key: K, increment: Long): G[Long] = {
    val command =
      RedisCommand(IncrBy, Tuple2(ArbitraryKeyInput[K](), LongInput), LongOutput)
    command.run((key, increment))
  }

  /**
   * Increment the float value of a key by the given amount.
   *
   * @param key
   *   Key of the value to increment
   * @param increment
   *   Amount to increment the value by
   * @return
   *   Returns the value of key after the increment.
   */
  final def incrByFloat[K: Schema](key: K, increment: Double): G[Double] = {
    val command = RedisCommand(IncrByFloat, Tuple2(ArbitraryKeyInput[K](), DoubleInput), DoubleOutput)
    command.run((key, increment))
  }

  /**
   * Get the longest common subsequence of values stored in the given keys.
   *
   * @param keyA
   *   first value that will contain subsequence
   * @param keyB
   *   second value that will contain subsequence
   * @param lcsQueryType
   *   modifier that will affect the output
   * @return
   *   Without modifiers returns the string representing the longest common substring. When LEN is given the command
   *   returns the length of the longest common substring. When IDX is given the command returns an array with the LCS
   *   length and all the ranges in both the strings, start and end offset for each string, where there are matches.
   *   When withMatchLen is given each array representing a match will also have the length of the match (see examples).
   */
  final def lcs[K: Schema](keyA: K, keyB: K, lcsQueryType: Option[LcsQueryType] = None): G[Lcs] = {
    val redisCommand = RedisCommand(
      Lcs,
      Tuple3(
        ArbitraryKeyInput[K](),
        ArbitraryKeyInput[K](),
        OptionalInput(LcsQueryTypeInput)
      ),
      LcsOutput
    )
    redisCommand.run((keyA, keyB, lcsQueryType))
  }

  /**
   * Get all the values of the given keys.
   *
   * @param key
   *   First key to get
   * @param keys
   *   Subsequent keys to get
   * @return
   *   Returns the values of the given keys.
   */
  final def mGet[K: Schema](
    key: K,
    keys: K*
  ): ResultBuilder1[({ type lambda[x] = Chunk[Option[x]] })#lambda, G] =
    new ResultBuilder1[({ type lambda[x] = Chunk[Option[x]] })#lambda, G] {
      def returning[V: Schema]: G[Chunk[Option[V]]] = {
        val command =
          RedisCommand(MGet, NonEmptyList(ArbitraryKeyInput[K]()), ChunkOutput(OptionalOutput(ArbitraryOutput[V]())))
        command.run((key, keys.toList))
      }
    }

  /**
   * Set multiple keys to multiple values.
   *
   * @param keyValue
   *   Tuple of key and value, first one to set
   * @param keyValues
   *   Subsequent tuples of key values
   */
  final def mSet[K: Schema, V: Schema](keyValue: (K, V), keyValues: (K, V)*): G[Unit] = {
    val command =
      RedisCommand(MSet, NonEmptyList(Tuple2(ArbitraryKeyInput[K](), ArbitraryValueInput[V]())), UnitOutput)
    command.run((keyValue, keyValues.toList))
  }

  /**
   * Set multiple keys to multiple values only if none of the keys exist.
   *
   * @param keyValue
   *   First key value to set
   * @param keyValues
   *   Subsequent key values to set
   * @return
   *   1 if the all the keys were set. 0 if no key was set (at least one key already existed).
   */
  final def mSetNx[K: Schema, V: Schema](
    keyValue: (K, V),
    keyValues: (K, V)*
  ): G[Boolean] = {
    val command =
      RedisCommand(MSetNx, NonEmptyList(Tuple2(ArbitraryKeyInput[K](), ArbitraryValueInput[V]())), BoolOutput)
    command.run((keyValue, keyValues.toList))
  }

  /**
   * Set the string value of a key.
   *
   * @param key
   *   Key of the string to set
   * @param value
   *   Value to set
   * @param update
   *   Update can be Update.SetExisting which only sets the key if it exists, or Update.SetNew which nly sets the key if
   *   it does not exist
   * @param expireAt
   *   When set any previously set expire time remains unchanged
   * @return
   *   true if set was executed correctly, false otherwise.
   */
  final def set[K: Schema, V: Schema](
    key: K,
    value: V,
    update: Option[Update] = None,
    expireAt: Option[SetExpire] = None
  ): G[Boolean] = {
    val input   = Tuple4(
      ArbitraryKeyInput[K](),
      ArbitraryValueInput[V](),
      OptionalInput(UpdateInput),
      OptionalInput(SetExpireInput)
    )
    val command = RedisCommand(Set, input, SetOutput)
    command.run((key, value, update, expireAt))
  }

  /**
   * Sets or clears the bit at offset in the string value stored at key.
   *
   * @param key
   *   Key of the string to set or clear bits
   * @param offset
   *   Offset at which to set or clear the bit
   * @param value
   *   True if bit should be set, False if it should be cleared
   * @return
   *   Returns the original bit value stored at offset.
   */
  final def setBit[K: Schema](key: K, offset: Long, value: Boolean): G[Boolean] = {
    val command =
      RedisCommand(SetBit, Tuple3(ArbitraryKeyInput[K](), LongInput, BoolInput), BoolOutput)
    command.run((key, offset, value))
  }

  /**
   * Set the string value of a key with a 'GET' option.
   *
   * @param key
   *   Key of the string to set
   * @param value
   *   Value to set
   * @param update
   *   Update can be Update.SetExisting which only sets the key if it exists, or Update.SetNew which nly sets the key if
   *   it does not exist
   * @param expireAt
   *   When set any previously set expire time remains unchanged
   * @return
   *   the old value stored at key, or None if key did not exist
   */
  final def setGet[K: Schema, V: Schema](
    key: K,
    value: V,
    update: Option[Update] = None,
    expireAt: Option[SetExpire] = None
  ): G[Option[V]] = {
    val input   = Tuple5(
      ArbitraryKeyInput[K](),
      ArbitraryValueInput[V](),
      OptionalInput(UpdateInput),
      OptionalInput(SetExpireInput),
      GetKeywordInput
    )
    val command = RedisCommand(Set, input, OptionalOutput(ArbitraryOutput[V]()))
    command.run((key, value, update, expireAt, GetKeyword))
  }

  /**
   * Overwrite part of a string at key starting at the specified offset.
   *
   * @param key
   *   Key of the string to overwrite
   * @param offset
   *   Offset to start writing
   * @param value
   *   Value to overwrite with
   * @return
   *   Returns the length of the string after it was modified by the command.
   */
  final def setRange[K: Schema, V: Schema](key: K, offset: Long, value: V): G[Long] = {
    val command =
      RedisCommand(SetRange, Tuple3(ArbitraryKeyInput[K](), LongInput, ArbitraryValueInput[V]()), LongOutput)
    command.run((key, offset, value))
  }

  /**
   * Get the length of a value stored in a key.
   *
   * @param key
   *   Key of the string to get the length of
   * @return
   *   Returns the length of the string.
   */
  final def strLen[K: Schema](key: K): G[Long] = {
    val command = RedisCommand(StrLen, ArbitraryKeyInput[K](), LongOutput)
    command.run(key)
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
  final val GetDel      = "GETDEL"
  final val GetEx       = "GETEX"
  final val GetRange    = "GETRANGE"
  final val GetSet      = "GETSET"
  final val Incr        = "INCR"
  final val IncrBy      = "INCRBY"
  final val IncrByFloat = "INCRBYFLOAT"
  final val Lcs         = "LCS"
  final val MGet        = "MGET"
  final val MSet        = "MSET"
  final val MSetNx      = "MSETNX"
  final val Set         = "SET"
  final val SetBit      = "SETBIT"
  final val SetRange    = "SETRANGE"
  final val StrLen      = "STRLEN"
}
