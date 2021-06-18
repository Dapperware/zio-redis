package zio.redis.api

import zio.redis.Input._
import zio.redis.Output._
import zio.redis._
import zio.schema.Schema
import zio.{ Chunk, ZIO }

trait Hashes {
  import Hashes._

  /**
   * Removes the specified fields from the hash stored at `key`.
   *
   * @param key of the hash that should be removed
   * @param field field to remove
   * @param fields additional fields
   * @return number of fields removed from the hash.
   */
  final def hDel[K: Schema, F: Schema](key: K, field: F, fields: F*): ZIO[RedisExecutor, RedisError, Long] = {
    val command = RedisCommand(HDel, Tuple2(ArbitraryInput[K](), NonEmptyList(ArbitraryInput[F]())), LongOutput)
    command.run((key, (field, fields.toList)))
  }

  /**
   * Returns if `field` is an existing field in the hash stored at `key`.
   *
   * @param key of the hash that should be inspected
   * @param field field to inspect
   * @return true if the field exists, otherwise false.
   */
  final def hExists[K: Schema, F: Schema](key: K, field: F): ZIO[RedisExecutor, RedisError, Boolean] = {
    val command = RedisCommand(HExists, Tuple2(ArbitraryInput[K](), ArbitraryInput[F]()), BoolOutput)
    command.run((key, field))
  }

  /**
   * Returns the value associated with `field` in the hash stored at `key`.
   *
   * @param key of the hash whose field should be read
   * @param field which value should be returned
   * @return value stored in the field, if any.
   */
  final def hGet[K: Schema, F: Schema](key: K, field: F): ResultBuilder[Option] =
    new ResultBuilder[Option] {
      def returning[V: Schema]: ZIO[RedisExecutor, RedisError, Option[V]] =
        RedisCommand(HGet, Tuple2(ArbitraryInput[K](), ArbitraryInput[F]()), OptionalOutput(ArbitraryOutput[V]()))
          .run((key, field))
    }

  // TODO: support F[_, _] result builders
  /**
   * Returns all fields and values of the hash stored at `key`.
   *
   * @param key of the hash that should be read
   * @return map of `field -> value` pairs under the key.
   */
  final def hGetAll[K: Schema, F: Schema, V: Schema](key: K): ZIO[RedisExecutor, RedisError, Map[F, V]] = {
    val command = RedisCommand(HGetAll, ArbitraryInput[K](), KeyValueOutput(ArbitraryOutput[F](), ArbitraryOutput[V]()))
    command.run(key)
  }

  /**
   * Increments the number stored at `field` in the hash stored at `key` by `increment`. If field does not exist the
   * value is set to `increment`.
   *
   * @param key of the hash that should be updated
   * @param field field containing an integer, or a new field
   * @param increment integer to increment with
   * @return integer value after incrementing, or error if the field is not an integer.
   */
  final def hIncrBy[K: Schema, F: Schema](key: K, field: F, increment: Long): ZIO[RedisExecutor, RedisError, Long] = {
    val command = RedisCommand(HIncrBy, Tuple3(ArbitraryInput[K](), ArbitraryInput[F](), LongInput), LongOutput)
    command.run((key, field, increment))
  }

  /**
   * Increment the specified `field` of a hash stored at `key`, and representing a floating point number by the
   * specified `increment`.
   *
   * @param key of the hash that should be updated
   * @param field field containing a float, or a new field
   * @param increment float to increment with
   * @return float value after incrementing, or error if the field is not a float.
   */
  final def hIncrByFloat[K: Schema, F: Schema](
    key: K,
    field: F,
    increment: Double
  ): ZIO[RedisExecutor, RedisError, Double] = {
    val command =
      RedisCommand(HIncrByFloat, Tuple3(ArbitraryInput[K](), ArbitraryInput[F](), DoubleInput), DoubleOutput)
    command.run((key, field, increment))
  }

  /**
   * Returns all field names in the hash stored at `key`.
   *
   * @param key of the hash whose fields should be returned
   * @return chunk of field names.
   */
  final def hKeys[K: Schema](key: K): ResultBuilder[Chunk] =
    new ResultBuilder[Chunk] {
      def returning[F: Schema]: ZIO[RedisExecutor, RedisError, Chunk[F]] =
        RedisCommand(HKeys, ArbitraryInput[K](), ChunkOutput(ArbitraryOutput[F]())).run(key)
    }

  /**
   * Returns the number of fields contained in the hash stored at `key`.
   *
   * @param key of the hash whose fields should be counted
   * @return number of fields.
   */
  final def hLen[K: Schema](key: K): ZIO[RedisExecutor, RedisError, Long] = {
    val command = RedisCommand(HLen, ArbitraryInput[K](), LongOutput)
    command.run(key)
  }

  /**
   * Returns the values associated with the specified `fields` in the hash stored at `key`.
   *
   * @param key of the hash whose values should be read
   * @param field fields to retrieve
   * @param fields additional fields
   * @return chunk of values, where value is `None` if the field is not in the hash.
   */
  final def hmGet[K: Schema, F: Schema](
    key: K,
    field: F,
    fields: F*
  ): ResultBuilder[({ type lambda[+x] = Chunk[Option[x]] })#lambda] =
    new ResultBuilder[({ type lambda[+x] = Chunk[Option[x]] })#lambda] {
      def returning[V: Schema]: ZIO[RedisExecutor, RedisError, Chunk[Option[V]]] = {
        val command = RedisCommand(
          HmGet,
          Tuple2(ArbitraryInput[K](), NonEmptyList(ArbitraryInput[F]())),
          ChunkOutput(OptionalOutput(ArbitraryOutput[V]()))
        )
        command.run((key, (field, fields.toList)))
      }
    }

  /**
   *  Sets the specified `field -> value` pairs in the hash stored at `key`.
   *  Deprecated: As per Redis 4.0.0, HMSET is considered deprecated. Please use `hSet` instead.
   *
   *  @param key of the hash whose value should be set
   *  @param pair mapping of a field to value
   *  @param pairs additional pairs
   *  @return unit if fields are successfully set.
   */
  final def hmSet[K: Schema, F: Schema, V: Schema](
    key: K,
    pair: (F, V),
    pairs: (F, V)*
  ): ZIO[RedisExecutor, RedisError, Unit] = {
    val command = RedisCommand(
      HmSet,
      Tuple2(ArbitraryInput[K](), NonEmptyList(Tuple2(ArbitraryInput[F](), ArbitraryInput[V]()))),
      UnitOutput
    )
    command.run((key, (pair, pairs.toList)))
  }

  // TODO: how to handle F[(A, B)]
  /**
   * Iterates `fields` of Hash types and their associated values using a cursor-based iterator
   *
   * @param key of the hash that should be scanned
   * @param cursor integer representing iterator
   * @param pattern regular expression matching keys to scan
   * @param count approximate number of elements to return (see https://redis.io/commands/scan#the-count-option)
   * @return pair containing the next cursor and list of elements scanned.
   */
  final def hScan[K: Schema, F: Schema, V: Schema](
    key: K,
    cursor: Long,
    pattern: Option[String] = None,
    count: Option[Count] = None
  ): ZIO[RedisExecutor, RedisError, (Long, Chunk[(F, V)])] = {
    val command = RedisCommand(
      HScan,
      Tuple4(ArbitraryInput[K](), LongInput, OptionalInput(PatternInput), OptionalInput(CountInput)),
      Tuple2Output(ArbitraryOutput[Long](), ChunkTuple2Output(ArbitraryOutput[F](), ArbitraryOutput[V]()))
    )
    command.run((key, cursor, pattern.map(Pattern), count))
  }

  /**
   * Sets `field -> value` pairs in the hash stored at `key`
   *
   * @param key of the hash whose value should be set
   * @param pair mapping of a field to value
   * @param pairs additional pairs
   * @return number of fields added.
   */
  final def hSet[K: Schema, F: Schema, V: Schema](
    key: K,
    pair: (F, V),
    pairs: (F, V)*
  ): ZIO[RedisExecutor, RedisError, Long] = {
    val command = RedisCommand(
      HSet,
      Tuple2(ArbitraryInput[K](), NonEmptyList(Tuple2(ArbitraryInput[F](), ArbitraryInput[V]()))),
      LongOutput
    )
    command.run((key, (pair, pairs.toList)))
  }

  /**
   * Sets `field` in the hash stored at `key` to `value`, only if `field` does not yet exist
   *
   * @param key of the hash whose value should be set
   * @param field for which a value should be set
   * @param value that should be set
   * @return true if `field` is a new field and value was set, otherwise false.
   */
  final def hSetNx[K: Schema, F: Schema, V: Schema](
    key: K,
    field: F,
    value: V
  ): ZIO[RedisExecutor, RedisError, Boolean] = {
    val command =
      RedisCommand(HSetNx, Tuple3(ArbitraryInput[K](), ArbitraryInput[F](), ArbitraryInput[V]()), BoolOutput)
    command.run((key, field, value))
  }

  /**
   * Returns the string length of the value associated with `field` in the hash stored at `key`
   *
   * @param key of the hash that should be read
   * @param field which value length should be returned
   * @return string length of the value in field, or zero if either field or key do not exist.
   */
  final def hStrLen[K: Schema, F: Schema](key: K, field: F): ZIO[RedisExecutor, RedisError, Long] = {
    val command = RedisCommand(HStrLen, Tuple2(ArbitraryInput[K](), ArbitraryInput[F]()), LongOutput)
    command.run((key, field))
  }

  /**
   * Returns all values in the hash stored at `key`
   *
   * @param key of the hash which values should be read
   * @return list of values in the hash, or an empty list when `key` does not exist.
   */
  final def hVals[K: Schema](key: K): ResultBuilder[Chunk] =
    new ResultBuilder[Chunk] {
      def returning[V: Schema]: ZIO[RedisExecutor, RedisError, Chunk[V]] =
        RedisCommand(HVals, ArbitraryInput[K](), ChunkOutput(ArbitraryOutput[V]())).run(key)
    }

  /**
   * Returns a random field from the hash value stored at `key`
   *
   * @param key of the hash which fields should be read
   * @return random field in the hash or `None` when `key` does not exist.
   */
  final def hRandField[K: Schema](key: K): ResultBuilder[Option] =
    new ResultBuilder[Option] {
      def returning[V: Schema]: ZIO[RedisExecutor, RedisError, Option[V]] =
        RedisCommand(HRandField, ArbitraryInput[K](), OptionalOutput(ArbitraryOutput[V]())).run(key)
    }

  /**
   * Returns an array of at most `count` distinct fields randomly. If called with a negative `count`, the command
   * will return exactly `count` fields, allowing repeats. If `withValues` is true it will return fields
   * with his values intercalated.
   *
   * @param key of the hash which fields should be read
   * @param count maximum number of different fields to return if positive or the exact number, in absolute value
   *              if negative
   * @param withValues when true includes the respective values of the randomly selected hash fields
   * @return a list of fields or fields and values if `withValues` is true.
   */
  final def hRandField[K: Schema](key: K, count: Long, withValues: Boolean = false): ResultBuilder[Chunk] =
    new ResultBuilder[Chunk] {
      def returning[V: Schema]: ZIO[RedisExecutor, RedisError, Chunk[V]] = {
        val command = RedisCommand(
          HRandField,
          Tuple3(ArbitraryInput[K](), LongInput, OptionalInput(StringInput)),
          ChunkOutput(ArbitraryOutput[V]())
        )
        command.run((key, count, if (withValues) Some("WITHVALUES") else None))
      }
    }
}

private[redis] object Hashes {
  final val HDel         = "HDEL"
  final val HExists      = "HEXISTS"
  final val HGet         = "HGET"
  final val HGetAll      = "HGETALL"
  final val HIncrBy      = "HINCRBY"
  final val HIncrByFloat = "HINCRBYFLOAT"
  final val HKeys        = "HKEYS"
  final val HLen         = "HLEN"
  final val HmGet        = "HMGET"
  final val HmSet        = "HMSET"
  final val HScan        = "HSCAN"
  final val HSet         = "HSET"
  final val HSetNx       = "HSETNX"
  final val HStrLen      = "HSTRLEN"
  final val HVals        = "HVALS"
  final val HRandField   = "HRANDFIELD"
}
