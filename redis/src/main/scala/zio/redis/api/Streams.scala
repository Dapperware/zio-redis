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
import zio.redis.ResultBuilder._
import zio.redis._
import zio.schema.Schema

trait Streams extends commands.Streams {
  import StreamInfoWithFull._
  import XGroupCommand._

  /**
   * Removes one or multiple messages from the pending entries list (PEL) of a stream consumer group.
   *
   * @param key
   *   ID of the stream
   * @param group
   *   ID of the consumer group
   * @param id
   *   ID of the message
   * @param ids
   *   IDs of the rest of the messages
   * @return
   *   the number of messages successfully acknowledged.
   */
  final def xAck[SK: Schema, G: Schema, I: Schema](key: SK, group: G, id: I, ids: I*): IO[RedisError, Long] =
    _xAck[SK, G, I].run((key, group, (id, ids.toList)))

  /**
   * Appends the specified stream entry to the stream at the specified key.
   *
   * @param key
   *   ID of the stream
   * @param id
   *   ID of the message
   * @param pair
   *   field and value pair
   * @param pairs
   *   rest of the field and value pairs
   * @return
   *   ID of the added entry.
   */
  final def xAdd[SK: Schema, I: Schema, K: Schema, V: Schema](
    key: SK,
    id: I,
    pair: (K, V),
    pairs: (K, V)*
  ): ResultBuilder1[Id] =
    new ResultBuilder1[Id] {
      def returning[R: Schema]: IO[RedisError, Id[R]] =
        _xAdd[SK, I, K, V, R].run((key, None, id, (pair, pairs.toList)))
    }

  /**
   * An introspection command used in order to retrieve different information about the stream.
   *
   * @param key
   *   ID of the stream
   * @return
   *   General information about the stream stored at the specified key.
   */
  final def xInfoStream[SK: Schema](key: SK): ResultBuilder3[StreamInfo] =
    new ResultBuilder3[StreamInfo] {
      def returning[RI: Schema, RK: Schema, RV: Schema]: IO[RedisError, StreamInfo[RI, RK, RV]] =
        _xInfoStream[SK, RI, RK, RV].run(key)
    }

  /**
   * Returns the entire state of the stream, including entries, groups, consumers and PELs.
   *
   * @param key
   *   ID of the stream
   * @return
   *   General information about the stream stored at the specified key.
   */
  final def xInfoStreamFull[SK: Schema](key: SK): ResultBuilder3[FullStreamInfo] = new ResultBuilder3[FullStreamInfo] {
    def returning[RI: Schema, RK: Schema, RV: Schema]: IO[RedisError, FullStreamInfo[RI, RK, RV]] =
      _xInfoStreamFull[SK, RI, RK, RV].run((key, "FULL"))
  }

  /**
   * Returns the entire state of the stream, including entries, groups, consumers and PELs.
   *
   * @param key
   *   ID of the stream
   * @param count
   *   limit the amount of stream/PEL entries that are returned (The first <count> entries are returned)
   * @return
   *   General information about the stream stored at the specified key.
   */
  final def xInfoStreamFull[SK: Schema](key: SK, count: Long): ResultBuilder3[FullStreamInfo] =
    new ResultBuilder3[FullStreamInfo] {
      def returning[RI: Schema, RK: Schema, RV: Schema]: IO[RedisError, FullStreamInfo[RI, RK, RV]] =
        _xInfoStreamFullWithCount[SK, RI, RK, RV].run((key, "FULL", Count(count)))
    }

  /**
   * An introspection command used in order to retrieve different information about the group.
   *
   * @param key
   *   ID of the stream
   * @return
   *   List of consumer groups associated with the stream stored at the specified key.
   */
  final def xInfoGroups[SK: Schema](key: SK): IO[RedisError, Chunk[StreamGroupsInfo]] = _xInfoGroups[SK].run(key)

  /**
   * An introspection command used in order to retrieve different information about the consumers.
   *
   * @param key
   *   ID of the stream
   * @param group
   *   ID of the consumer group
   * @return
   *   List of every consumer in a specific consumer group.
   */
  final def xInfoConsumers[SK: Schema, SG: Schema](key: SK, group: SG): IO[RedisError, Chunk[StreamConsumersInfo]] =
    _xInfoConsumers[SK, SG].run((key, group))

  /**
   * Appends the specified stream entry to the stream at the specified key while limiting the size of the stream.
   *
   * @param key
   *   ID of the stream
   * @param id
   *   ID of the message
   * @param count
   *   maximum number of elements in a stream
   * @param approximate
   *   flag that indicates if a stream should be limited to the exact number of elements
   * @param pair
   *   field and value pair
   * @param pairs
   *   rest of the field and value pairs
   * @return
   *   ID of the added entry.
   */
  final def xAddWithMaxLen[SK: Schema, I: Schema, K: Schema, V: Schema](
    key: SK,
    id: I,
    count: Long,
    approximate: Boolean = false
  )(
    pair: (K, V),
    pairs: (K, V)*
  ): ResultBuilder1[Id] =
    new ResultBuilder1[Id] {
      def returning[R: Schema]: IO[RedisError, Id[R]] =
        _xAddWithMaxLen[SK, I, K, V, R].run((key, Some(StreamMaxLen(approximate, count)), id, (pair, pairs.toList)))
    }

  /**
   * Changes the ownership of a pending message.
   *
   * @param key
   *   ID of the stream
   * @param group
   *   ID of the consumer group
   * @param consumer
   *   ID of the consumer
   * @param minIdleTime
   *   minimum idle time of a message
   * @param idle
   *   idle time (last time it was delivered) of the message that will be set
   * @param time
   *   same as idle but instead of a relative amount of milliseconds, it sets the idle time to a specific Unix time (in
   *   milliseconds)
   * @param retryCount
   *   retry counter of a message that will be set
   * @param force
   *   flag that indicates that a message doesn't have to be in a pending entries list (PEL)
   * @param id
   *   ID of a message
   * @param ids
   *   IDs of the rest of the messages
   * @return
   *   messages successfully claimed.
   */
  final def xClaim[SK: Schema, SG: Schema, SC: Schema, I: Schema](
    key: SK,
    group: SG,
    consumer: SC,
    minIdleTime: Duration,
    idle: Option[Duration] = None,
    time: Option[Duration] = None,
    retryCount: Option[Long] = None,
    force: Boolean = false
  )(id: I, ids: I*): ResultBuilder2[({ type lambda[x, y] = StreamEntries[I, x, y] })#lambda] =
    new ResultBuilder2[({ type lambda[x, y] = StreamEntries[I, x, y] })#lambda] {
      def returning[RK: Schema, RV: Schema]: IO[RedisError, StreamEntries[I, RK, RV]] = {
        val withForce = if (force) Some(WithForce) else None

        _xClaim[SK, SG, SC, I, RK, RV].run(
          (key, group, consumer, minIdleTime, (id, ids.toList), idle, time, retryCount, withForce)
        )
      }
    }

  /**
   * Changes the ownership of a pending message.
   *
   * @param key
   *   ID of the stream
   * @param group
   *   ID of the consumer group
   * @param consumer
   *   ID of the consumer
   * @param minIdleTime
   *   minimum idle time of a message
   * @param idle
   *   idle time (last time it was delivered) of the message that will be set
   * @param time
   *   same as idle but instead of a relative amount of milliseconds, it sets the idle time to a specific Unix time (in
   *   milliseconds)
   * @param retryCount
   *   retry counter of a message that will be set
   * @param force
   *   flag that indicates that a message doesn't have to be in a pending entries list (PEL)
   * @param id
   *   ID of a message
   * @param ids
   *   IDs of the rest of the messages
   * @return
   *   IDs of the messages that are successfully claimed.
   */
  final def xClaimWithJustId[SK: Schema, SG: Schema, SC: Schema, I: Schema](
    key: SK,
    group: SG,
    consumer: SC,
    minIdleTime: Duration,
    idle: Option[Duration] = None,
    time: Option[Duration] = None,
    retryCount: Option[Long] = None,
    force: Boolean = false
  )(id: I, ids: I*): ResultBuilder1[Chunk] =
    new ResultBuilder1[Chunk] {
      def returning[R: Schema]: IO[RedisError, Chunk[R]] = {
        val withForce = if (force) Some(WithForce) else None

        _xClaimWithJustId[SK, SG, SC, I, R].run(
          (
            key,
            group,
            consumer,
            minIdleTime,
            (id, ids.toList),
            idle,
            time,
            retryCount,
            withForce,
            WithJustId
          )
        )
      }
    }

  /**
   * Removes the specified entries from a stream.
   *
   * @param key
   *   ID of the stream
   * @param id
   *   ID of the message
   * @param ids
   *   IDs of the rest of the messages
   * @return
   *   the number of entries deleted.
   */
  final def xDel[SK: Schema, I: Schema](key: SK, id: I, ids: I*): IO[RedisError, Long] =
    _xDel[SK, I].run((key, (id, ids.toList)))

  /**
   * Create a new consumer group associated with a stream.
   *
   * @param key
   *   ID of the stream
   * @param group
   *   ID of the consumer group
   * @param id
   *   ID of the last item in the stream to consider already delivered
   * @param mkStream
   *   ID of the last item in the stream to consider already delivered
   */
  final def xGroupCreate[SK: Schema, SG: Schema, I: Schema](
    key: SK,
    group: SG,
    id: I,
    mkStream: Boolean = false
  ): IO[RedisError, Unit] =
    _xGroupCreate[SK, SG, I].run(Create(key, group, id, mkStream))

  /**
   * Set the consumer group last delivered ID to something else.
   *
   * @param key
   *   ID of the stream
   * @param group
   *   ID of the consumer group
   * @param id
   *   last delivered ID to set
   */
  final def xGroupSetId[SK: Schema, SG: Schema, I: Schema](key: SK, group: SG, id: I): IO[RedisError, Unit] =
    _xGroupSetId[SK, SG, I].run(SetId(key, group, id))

  /**
   * Destroy a consumer group.
   *
   * @param key
   *   ID of the stream
   * @param group
   *   ID of the consumer group
   * @return
   *   flag that indicates if the deletion was successful.
   */
  final def xGroupDestroy[SK: Schema, SG: Schema](key: SK, group: SG): IO[RedisError, Boolean] =
    _xGroupDestroy[SK, SG].run(Destroy(key, group))

  /**
   * Create a new consumer associated with a consumer group.
   *
   * @param key
   *   ID of the stream
   * @param group
   *   ID of the consumer group
   * @param consumer
   *   ID of the consumer
   * @return
   *   the number of created consumer groups.
   */
  final def xGroupCreateConsumer[SK: Schema, SG: Schema, SC: Schema](
    key: SK,
    group: SG,
    consumer: SC
  ): IO[RedisError, Boolean] =
    _xGroupCreateConsumer[SK, SG, SC].run(CreateConsumer(key, group, consumer))

  /**
   * Remove a specific consumer from a consumer group.
   *
   * @param key
   *   ID of the stream
   * @param group
   *   ID of the consumer group
   * @param consumer
   *   ID of the consumer
   * @return
   *   the number of pending messages that the consumer had before it was deleted.
   */
  final def xGroupDelConsumer[SK: Schema, SG: Schema, SC: Schema](
    key: SK,
    group: SG,
    consumer: SC
  ): IO[RedisError, Long] =
    _xGroupDelConsumer[SK, SG, SC].run(DelConsumer(key, group, consumer))

  /**
   * Fetches the number of entries inside a stream.
   *
   * @param key
   *   ID of the stream
   * @return
   *   the number of entries inside a stream.
   */
  final def xLen[SK: Schema](key: SK): IO[RedisError, Long] = _xLen[SK].run(key)

  /**
   * Inspects the list of pending messages.
   *
   * @param key
   *   ID of the stream
   * @param group
   *   ID of the consumer group
   * @return
   *   summary about the pending messages in a given consumer group.
   */
  final def xPending[SK: Schema, SG: Schema](key: SK, group: SG): IO[RedisError, PendingInfo] =
    _xPending[SK, SG].run((key, group, None))

  /**
   * Inspects the list of pending messages.
   *
   * @param key
   *   ID of the stream
   * @param group
   *   ID of the consumer group
   * @param start
   *   start of the range of IDs
   * @param end
   *   end of the range of IDs
   * @param count
   *   maximum number of messages returned
   * @param consumer
   *   ID of the consumer
   * @param idle
   *   idle time of a pending message by which message are filtered
   * @return
   *   detailed information for each message in the pending entries list.
   */
  final def xPending[SK: Schema, SG: Schema, I: Schema, SC: Schema](
    key: SK,
    group: SG,
    start: I,
    end: I,
    count: Long,
    consumer: Option[SC] = None,
    idle: Option[Duration] = None
  ): IO[RedisError, Chunk[PendingMessage]] =
    _xPendingMessages[SK, SG, I, SC].run((key, group, idle, start, end, count, consumer))

  /**
   * Fetches the stream entries matching a given range of IDs.
   *
   * @param key
   *   ID of the stream
   * @param start
   *   start of the range of IDs
   * @param end
   *   end of the range of IDs
   * @return
   *   the complete entries with IDs matching the specified range.
   */
  final def xRange[SK: Schema, I: Schema](
    key: SK,
    start: I,
    end: I
  ): ResultBuilder2[({ type lambda[x, y] = StreamEntries[I, x, y] })#lambda] =
    new ResultBuilder2[({ type lambda[x, y] = StreamEntries[I, x, y] })#lambda] {
      def returning[RK: Schema, RV: Schema]: IO[RedisError, StreamEntries[I, RK, RV]] =
        _xRange[SK, I, RK, RV].run((key, start, end, None))
    }

  /**
   * Fetches the stream entries matching a given range of IDs.
   *
   * @param key
   *   ID of the stream
   * @param start
   *   start of the range of IDs
   * @param end
   *   end of the range of IDs
   * @param count
   *   maximum number of entries returned
   * @return
   *   the complete entries with IDs matching the specified range.
   */
  final def xRange[SK: Schema, I: Schema](
    key: SK,
    start: I,
    end: I,
    count: Long
  ): ResultBuilder2[({ type lambda[x, y] = StreamEntries[I, x, y] })#lambda] =
    new ResultBuilder2[({ type lambda[x, y] = StreamEntries[I, x, y] })#lambda] {
      def returning[RK: Schema, RV: Schema]: IO[RedisError, StreamEntries[I, RK, RV]] =
        _xRangeWithCount[SK, I, RK, RV].run((key, start, end, Some(Count(count))))
    }

  /**
   * Read data from one or multiple streams.
   *
   * @param count
   *   maximum number of elements returned per stream
   * @param block
   *   duration for which we want to block before timing out
   * @param stream
   *   pair that contains stream ID and the last ID that the consumer received for that stream
   * @param streams
   *   rest of the pairs
   * @return
   *   complete entries with an ID greater than the last received ID per stream.
   */
  final def xRead[SK: Schema, I: Schema](count: Option[Long] = None, block: Option[Duration] = None)(
    stream: (SK, I),
    streams: (SK, I)*
  ): ResultBuilder2[({ type lambda[x, y] = StreamChunks[SK, I, x, y] })#lambda] =
    new ResultBuilder2[({ type lambda[x, y] = StreamChunks[SK, I, x, y] })#lambda] {
      def returning[RK: Schema, RV: Schema]: IO[RedisError, StreamChunks[SK, I, RK, RV]] =
        _xRead[SK, I, RK, RV].run((count.map(Count(_)), block, (stream, Chunk.fromIterable(streams))))
    }

  /**
   * Read data from one or multiple streams using consumer group.
   *
   * @param group
   *   ID of the consumer group
   * @param consumer
   *   ID of the consumer
   * @param count
   *   maximum number of elements returned per stream
   * @param block
   *   duration for which we want to block before timing out
   * @param noAck
   *   flag that indicates that the read messages shouldn't be added to the pending entries list (PEL)
   * @param stream
   *   pair that contains stream ID and the last ID that the consumer received for that stream
   * @param streams
   *   rest of the pairs
   * @return
   *   complete entries with an ID greater than the last received ID per stream.
   */
  final def xReadGroup[SG: Schema, SC: Schema, SK: Schema, I: Schema](
    group: SG,
    consumer: SC,
    count: Option[Long] = None,
    block: Option[Duration] = None,
    noAck: Boolean = false
  )(
    stream: (SK, I),
    streams: (SK, I)*
  ): ResultBuilder2[({ type lambda[x, y] = StreamChunks[SK, I, x, y] })#lambda] =
    new ResultBuilder2[({ type lambda[x, y] = StreamChunks[SK, I, x, y] })#lambda] {
      def returning[RK: Schema, RV: Schema]: IO[RedisError, StreamChunks[SK, I, RK, RV]] = {
        val noAckOpt = if (noAck) Some(NoAck) else None
        _xReadGroup[SG, SC, SK, I, RK, RV].run(
          (group, consumer, count.map(Count(_)), block, noAckOpt, (stream, Chunk.fromIterable(streams)))
        )
      }
    }

  /**
   * Fetches the stream entries matching a given range of IDs in the reverse order.
   *
   * @param key
   *   ID of the stream
   * @param end
   *   end of the range of IDs
   * @param start
   *   start of the range of IDs
   * @return
   *   the complete entries with IDs matching the specified range in the reverse order.
   */
  final def xRevRange[SK: Schema, I: Schema](
    key: SK,
    end: I,
    start: I
  ): ResultBuilder2[({ type lambda[x, y] = StreamEntries[I, x, y] })#lambda] =
    new ResultBuilder2[({ type lambda[x, y] = StreamEntries[I, x, y] })#lambda] {
      def returning[RK: Schema, RV: Schema]: IO[RedisError, StreamEntries[I, RK, RV]] =
        _xRevRange[SK, I, RK, RV].run((key, end, start, None))
    }

  /**
   * Fetches the stream entries matching a given range of IDs in the reverse order.
   *
   * @param key
   *   ID of the stream
   * @param end
   *   end of the range of IDs
   * @param start
   *   start of the range of IDs
   * @param count
   *   maximum number of entries returned
   * @return
   *   the complete entries with IDs matching the specified range in the reverse order.
   */
  final def xRevRange[SK: Schema, I: Schema](
    key: SK,
    end: I,
    start: I,
    count: Long
  ): ResultBuilder2[({ type lambda[x, y] = StreamEntries[I, x, y] })#lambda] =
    new ResultBuilder2[({ type lambda[x, y] = StreamEntries[I, x, y] })#lambda] {
      def returning[RK: Schema, RV: Schema]: IO[RedisError, StreamEntries[I, RK, RV]] =
        _xRevRangeWithCount[SK, I, RK, RV].run((key, end, start, Some(Count(count))))
    }

  /**
   * Trims the stream to a given number of items, evicting older items (items with lower IDs) if needed.
   *
   * @param key
   *   ID of the stream
   * @param count
   *   stream length
   * @param approximate
   *   flag that indicates if the stream length should be exactly count or few tens of entries more
   * @return
   *   the number of entries deleted from the stream.
   */
  final def xTrim[SK: Schema](key: SK, count: Long, approximate: Boolean = false): IO[RedisError, Long] =
    _xTrim[SK].run((key, StreamMaxLen(approximate, count)))
}
