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

trait Streams[G[+_]] extends RedisEnvironment[G] {
  import StreamInfoWithFull._
  import Streams._
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
  final def xAck[SK: Schema, GG: Schema, I: Schema](
    key: SK,
    group: GG,
    id: I,
    ids: I*
  ): G[Long] = {
    val command = RedisCommand(
      XAck,
      Tuple3(ArbitraryKeyInput[SK](), ArbitraryValueInput[GG](), NonEmptyList(ArbitraryValueInput[I]())),
      LongOutput
    )
    command.run((key, group, (id, ids.toList)))
  }

  /**
   * Appends the specified stream entry to the stream at the specified key.
   *
   * @param key
   *   ID of the stream
   * @param id
   *   ID of the message
   * @param noMakeStream
   *   The creation of stream's key can be disabled with the noMakeStream option
   * @param pair
   *   field and value pair
   * @param pairs
   *   rest of the field and value pairs
   * @return
   *   ID of the added entry.
   */
  final def xAdd[SK: Schema, I: Schema, K: Schema, V: Schema](key: SK, id: I, noMakeStream: Boolean = false)(
    pair: (K, V),
    pairs: (K, V)*
  ): ResultBuilder1[Option, G] =
    new ResultBuilder1[Option, G] {
      def returning[R: Schema]: G[Option[Id[R]]] = {
        val command = RedisCommand(
          XAdd,
          Tuple4(
            ArbitraryKeyInput[SK](),
            OptionalInput(NoMkStreamInput),
            ArbitraryValueInput[I](),
            NonEmptyList(Tuple2(ArbitraryKeyInput[K](), ArbitraryValueInput[V]()))
          ),
          OptionalOutput(ArbitraryOutput[R]())
        )

        val noMkStreamOpt = if (noMakeStream) Some(NoMkStream) else None
        command.run((key, noMkStreamOpt, id, (pair, pairs.toList)))
      }
    }

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
   * @param limit
   *  limit parameter
   * @param noMakeStream
   *  the creation of stream's key can be disabled with the NOMKSTREAM option.
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
    approximate: Boolean = false,
    limit: Option[Long] = None,
    noMakeStream: Boolean = false
  )(
    pair: (K, V),
    pairs: (K, V)*
  ): ResultBuilder1[Option, G] =
    new ResultBuilder1[Option, G] {
      def returning[R: Schema]: G[Option[Id[R]]] = {
        val noMkStreamOpt = if (noMakeStream) Some(NoMkStream) else None

        if (approximate) {
          val command = RedisCommand(
            XAdd,
            Tuple5(
              ArbitraryKeyInput[SK](),
              OptionalInput(NoMkStreamInput),
              OptionalInput(MaxLenApproxInput),
              ArbitraryValueInput[I](),
              NonEmptyList(Tuple2(ArbitraryKeyInput[K](), ArbitraryValueInput[V]()))
            ),
            OptionalOutput(ArbitraryOutput[R]())
          )

          command.run((key, noMkStreamOpt, Some(CappedStreamType.MaxLenApprox(count, limit)), id, (pair, pairs.toList)))
        } else {
          val command = RedisCommand(
            XAdd,
            Tuple5(
              ArbitraryKeyInput[SK](),
              OptionalInput(NoMkStreamInput),
              OptionalInput(MaxLenExactInput),
              ArbitraryValueInput[I](),
              NonEmptyList(Tuple2(ArbitraryKeyInput[K](), ArbitraryValueInput[V]()))
            ),
            OptionalOutput(ArbitraryOutput[R]())
          )

          command.run((key, noMkStreamOpt, Some(CappedStreamType.MaxLenExact(count)), id, (pair, pairs.toList)))
        }
      }
    }

  /**
   * Appends the specified stream entry to the stream at the specified key while limiting the size of the stream.
   *
   * @param key
   *   ID of the stream
   * @param id
   *   ID of the message
   * @param minId
   *   Evicts entries with IDs lower than minId, where minId is a stream ID.
   * @param approximate
   *   flag that indicates if a stream should be limited to the exact number of elements
   * @param limit
   *  limit parameter
   * @param noMakeStream
   *  the creation of stream's key can be disabled with the NOMKSTREAM option.
   * @param pair
   *   field and value pair
   * @param pairs
   *   rest of the field and value pairs
   * @return
   *   ID of the added entry.
   */
  final def xAddWithMinId[SK: Schema, I: Schema, K: Schema, V: Schema](
    key: SK,
    id: I,
    minId: I,
    approximate: Boolean = false,
    limit: Option[Long] = None,
    noMakeStream: Boolean = false
  )(
    pair: (K, V),
    pairs: (K, V)*
  ): ResultBuilder1[Option, G] =
    new ResultBuilder1[Option, G] {
      def returning[R: Schema]: G[Option[Id[R]]] = {
        val noMkStreamOpt = if (noMakeStream) Some(NoMkStream) else None

        if (approximate) {
          val command = RedisCommand(
            XAdd,
            Tuple5(
              ArbitraryKeyInput[SK](),
              OptionalInput(NoMkStreamInput),
              OptionalInput(MinIdApproxInput[I]()),
              ArbitraryValueInput[I](),
              NonEmptyList(Tuple2(ArbitraryKeyInput[K](), ArbitraryValueInput[V]()))
            ),
            OptionalOutput(ArbitraryOutput[R]())
          )

          command.run((key, noMkStreamOpt, Some(CappedStreamType.MinIdApprox(minId, limit)), id, (pair, pairs.toList)))
        } else {
          val command = RedisCommand(
            XAdd,
            Tuple5(
              ArbitraryKeyInput[SK](),
              OptionalInput(NoMkStreamInput),
              OptionalInput(MinIdExactInput[I]()),
              ArbitraryValueInput[I](),
              NonEmptyList(Tuple2(ArbitraryKeyInput[K](), ArbitraryValueInput[V]()))
            ),
            OptionalOutput(ArbitraryOutput[R]())
          )

          command.run((key, noMkStreamOpt, Some(CappedStreamType.MinIdExact(minId)), id, (pair, pairs.toList)))
        }
      }
    }

  /**
   * Transfers ownership of pending stream entries that match the specified criteria.
   *
   * @param key
   *   ID of the stream
   * @param group
   *   ID of the consumer group
   * @param consumer
   *   ID of the consumer
   * @param minIdleTime
   *   minimum idle time of a message
   * @param count
   *   the upper limit of the number of entries that attempts to claim
   * @param start
   *   command will claim only IDs that are equal or greater than this ID
   * @return
   *   messages successfully claimed.
   */
  final def xAutoClaim[SK: Schema, SG: Schema, SC: Schema, I: Schema](
    key: SK,
    group: SG,
    consumer: SC,
    minIdleTime: Duration,
    count: Option[Count] = None
  )(start: I): ResultBuilder3[({ type lambda[x, y, z] = StreamClaimedEntries[x, I, y, z] })#lambda, G] =
    new ResultBuilder3[({ type lambda[x, y, z] = StreamClaimedEntries[x, I, y, z] })#lambda, G] {
      def returning[RSI: Schema, RK: Schema, RV: Schema]: G[StreamClaimedEntries[RSI, I, RK, RV]] = {
        val command = RedisCommand(
          XAutoClaim,
          Tuple6(
            ArbitraryKeyInput[SK](),
            ArbitraryValueInput[SG](),
            ArbitraryValueInput[SC](),
            DurationMillisecondsInput,
            ArbitraryValueInput[I](),
            OptionalInput(CountInput)
          ),
          StreamClaimedOutput[RSI, I, RK, RV]()
        )
        command.run((key, group, consumer, minIdleTime, start, count))
      }
    }

  /**
   * Transfers ownership of pending stream entries that match the specified criteria.
   *
   * @param key
   *   ID of the stream
   * @param group
   *   ID of the consumer group
   * @param consumer
   *   ID of the consumer
   * @param minIdleTime
   *   minimum idle time of a message
   * @param count
   *   the upper limit of the number of entries that attempts to claim
   * @param start
   *   command will claim only IDs that are equal or greater than this ID
   * @return
   *   IDs of the messages that are successfully claimed.
   */
  final def xAutoClaimWithJustId[SK: Schema, SG: Schema, SC: Schema, I: Schema](
    key: SK,
    group: SG,
    consumer: SC,
    minIdleTime: Duration,
    count: Option[Count] = None
  )(start: I): ResultBuilder1[({ type lambda[x] = StreamClaimedIdChunk[x, I] })#lambda, G] =
    new ResultBuilder1[({ type lambda[x] = StreamClaimedIdChunk[x, I] })#lambda, G] {
      def returning[R: Schema]: G[StreamClaimedIdChunk[R, I]] = {
        val command = RedisCommand(
          XAutoClaim,
          Tuple7(
            ArbitraryKeyInput[SK](),
            ArbitraryValueInput[SG](),
            ArbitraryValueInput[SC](),
            DurationMillisecondsInput,
            ArbitraryValueInput[I](),
            OptionalInput(CountInput),
            WithJustIdInput
          ),
          StreamClaimedIdOutput[R, I]()
        )
        command.run((key, group, consumer, minIdleTime, start, count, WithJustId))
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
    force: Boolean = false,
    lastId: Option[I] = None
  )(id: I, ids: I*): ResultBuilder2[({ type lambda[x, y] = StreamEntries[I, x, y] })#lambda, G] =
    new ResultBuilder2[({ type lambda[x, y] = StreamEntries[I, x, y] })#lambda, G] {
      def returning[RK: Schema, RV: Schema]: G[StreamEntries[I, RK, RV]] = {
        val command =
          RedisCommand(
            XClaim,
            Tuple10(
              ArbitraryKeyInput[SK](),
              ArbitraryValueInput[SG](),
              ArbitraryValueInput[SC](),
              DurationMillisecondsInput,
              NonEmptyList(ArbitraryValueInput[I]()),
              OptionalInput(IdleInput),
              OptionalInput(TimeInput),
              OptionalInput(RetryCountInput),
              OptionalInput(WithForceInput),
              Input.OptionalInput(LastIdInput[I]())
            ),
            StreamEntriesOutput[I, RK, RV]()
          )

        val forceOpt = if (force) Some(WithForce) else None
        command.run(
          (key, group, consumer, minIdleTime, (id, ids.toList), idle, time, retryCount, forceOpt, lastId.map(LastId(_)))
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
    force: Boolean = false,
    lastId: Option[I] = None
  )(id: I, ids: I*): ResultBuilder1[Chunk, G] =
    new ResultBuilder1[Chunk, G] {
      def returning[R: Schema]: G[Chunk[R]] = {
        val command  = RedisCommand(
          XClaim,
          Tuple11(
            ArbitraryKeyInput[SK](),
            ArbitraryValueInput[SG](),
            ArbitraryValueInput[SC](),
            DurationMillisecondsInput,
            NonEmptyList(ArbitraryValueInput[I]()),
            OptionalInput(IdleInput),
            OptionalInput(TimeInput),
            OptionalInput(RetryCountInput),
            OptionalInput(WithForceInput),
            WithJustIdInput,
            OptionalInput(LastIdInput[I]())
          ),
          ChunkOutput(ArbitraryOutput[R]())
        )
        val forceOpt = if (force) Some(WithForce) else None
        command.run(
          (
            key,
            group,
            consumer,
            minIdleTime,
            (id, ids.toList),
            idle,
            time,
            retryCount,
            forceOpt,
            WithJustId,
            lastId.map(LastId(_))
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
  final def xDel[SK: Schema, I: Schema](key: SK, id: I, ids: I*): G[Long] = {
    val command =
      RedisCommand(XDel, Tuple2(ArbitraryKeyInput[SK](), NonEmptyList(ArbitraryValueInput[I]())), LongOutput)
    command.run((key, (id, ids.toList)))
  }

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
   * @param entriesRead
   *   Enable consumer group lag tracking
   */
  final def xGroupCreate[SK: Schema, SG: Schema, I: Schema](
    key: SK,
    group: SG,
    id: I,
    mkStream: Boolean = false,
    entriesRead: Option[Long] = None
  ): G[Unit] = {
    val command =
      RedisCommand(
        XGroup,
        Tuple3(XGroupCreateInput[SK, SG, I](), OptionalInput(MkStreamInput), OptionalInput(WithEntriesReadInput)),
        UnitOutput
      )
    command.run(
      (Create(key, group, id), if (mkStream) Some(MkStream) else None, entriesRead.map(WithEntriesRead(_)))
    )
  }

  /**
   * Create a new consumer group associated with a stream.
   *
   * @param key
   *   ID of the stream
   * @param group
   *   ID of the consumer group
   * @param mkStream
   *   ID of the last item in the stream to consider already delivered
   * @param entriesRead
   *   Enable consumer group lag tracking
   */
  final def xGroupCreateLastEntry[SK: Schema, SG: Schema](
    key: SK,
    group: SG,
    mkStream: Boolean = false,
    entriesRead: Option[Long] = None
  ): G[Unit] = xGroupCreate(key, group, "$", mkStream, entriesRead)

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
  ): G[Boolean] = {
    val command = RedisCommand(XGroup, XGroupCreateConsumerInput[SK, SG, SC](), BoolOutput)
    command.run(CreateConsumer(key, group, consumer))
  }

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
  ): G[Long] = {
    val command = RedisCommand(XGroup, XGroupDelConsumerInput[SK, SG, SC](), LongOutput)
    command.run(DelConsumer(key, group, consumer))
  }

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
  final def xGroupDestroy[SK: Schema, SG: Schema](key: SK, group: SG): G[Boolean] =
    RedisCommand(XGroup, XGroupDestroyInput[SK, SG](), BoolOutput).run(Destroy(key, group))

  /**
   * Set the consumer group last delivered ID to something else.
   *
   * @param key
   *   ID of the stream
   * @param group
   *   ID of the consumer group
   * @param id
   *   last delivered ID to set
   * @param entriesRead
   *   Enable consumer group lag tracking
   */
  final def xGroupSetId[SK: Schema, SG: Schema, I: Schema](
    key: SK,
    group: SG,
    id: I,
    entriesRead: Option[Long] = None
  ): G[Unit] = {
    val command =
      RedisCommand(XGroup, Tuple2(XGroupSetIdInput[SK, SG, I](), OptionalInput(WithEntriesReadInput)), UnitOutput)
    command.run((SetId(key, group, id), entriesRead.map(WithEntriesRead(_))))
  }

  /**
   * Set the consumer group last delivered ID to something else.
   *
   * @param key
   *   ID of the stream
   * @param group
   *   ID of the consumer group
   * @param id
   *   last delivered ID to set
   * @param entriesRead
   *   Enable consumer group lag tracking
   */
  final def xGroupSetIdLastEntry[SK: Schema, SG: Schema](
    key: SK,
    group: SG,
    entriesRead: Option[Long] = None
  ): G[Unit] = xGroupSetId(key, group, "$", entriesRead)

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
  final def xInfoConsumers[SK: Schema, SG: Schema](
    key: SK,
    group: SG
  ): G[Chunk[StreamConsumersInfo]] = {
    val command =
      RedisCommand(
        XInfoConsumers,
        Tuple2(ArbitraryKeyInput[SK](), ArbitraryValueInput[SG]()),
        StreamConsumersInfoOutput
      )
    command.run((key, group))
  }

  /**
   * An introspection command used in order to retrieve different information about the group.
   *
   * @param key
   *   ID of the stream
   * @return
   *   List of consumer groups associated with the stream stored at the specified key.
   */
  final def xInfoGroups[SK: Schema](key: SK): G[Chunk[StreamGroupsInfo]] = {
    val command = RedisCommand(XInfoGroups, ArbitraryKeyInput[SK](), StreamGroupsInfoOutput)
    command.run(key)
  }

  /**
   * An introspection command used in order to retrieve different information about the stream.
   *
   * @param key
   *   ID of the stream
   * @return
   *   General information about the stream stored at the specified key.
   */
  final def xInfoStream[SK: Schema](
    key: SK
  ): ResultBuilder3[StreamInfo, G] = new ResultBuilder3[StreamInfo, G] {
    def returning[RI: Schema, RK: Schema, RV: Schema]: G[StreamInfo[RI, RK, RV]] = {
      val command = RedisCommand(XInfoStream, ArbitraryKeyInput[SK](), StreamInfoOutput[RI, RK, RV]())
      command.run(key)
    }
  }

  /**
   * Returns the entire state of the stream, including entries, groups, consumers and PELs.
   *
   * @param key
   *   ID of the stream
   * @return
   *   General information about the stream stored at the specified key.
   */
  final def xInfoStreamFull[SK: Schema](
    key: SK
  ): ResultBuilder3[FullStreamInfo, G] = new ResultBuilder3[FullStreamInfo, G] {
    def returning[RI: Schema, RK: Schema, RV: Schema]: G[FullStreamInfo[RI, RK, RV]] = {
      val command = RedisCommand(
        XInfoStream,
        Tuple2(ArbitraryKeyInput[SK](), ArbitraryValueInput[String]()),
        StreamInfoFullOutput[RI, RK, RV]()
      )
      command.run((key, "FULL"))
    }
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
  final def xInfoStreamFull[SK: Schema](
    key: SK,
    count: Long
  ): ResultBuilder3[FullStreamInfo, G] = new ResultBuilder3[FullStreamInfo, G] {
    def returning[RI: Schema, RK: Schema, RV: Schema]: G[FullStreamInfo[RI, RK, RV]] = {
      val command = RedisCommand(
        XInfoStream,
        Tuple3(ArbitraryKeyInput[SK](), ArbitraryValueInput[String](), CountInput),
        StreamInfoFullOutput[RI, RK, RV]()
      )
      command.run((key, "FULL", Count(count)))
    }
  }

  /**
   * Fetches the number of entries inside a stream.
   *
   * @param key
   *   ID of the stream
   * @return
   *   the number of entries inside a stream.
   */
  final def xLen[SK: Schema](key: SK): G[Long] = {
    val command = RedisCommand(XLen, ArbitraryKeyInput[SK](), LongOutput)
    command.run(key)
  }

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
  final def xPending[SK: Schema, SG: Schema](key: SK, group: SG): G[PendingInfo] = {
    val command = RedisCommand(
      XPending,
      Tuple3(ArbitraryKeyInput[SK](), ArbitraryValueInput[SG](), OptionalInput(IdleInput)),
      XPendingOutput
    )
    command.run((key, group, None))
  }

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
  ): G[Chunk[PendingMessage]] = {
    val command = RedisCommand(
      XPending,
      Tuple7(
        ArbitraryKeyInput[SK](),
        ArbitraryValueInput[SG](),
        OptionalInput(IdleInput),
        ArbitraryValueInput[I](),
        ArbitraryValueInput[I](),
        LongInput,
        OptionalInput(ArbitraryValueInput[SC]())
      ),
      PendingMessagesOutput
    )
    command.run((key, group, idle, start, end, count, consumer))
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
   * @return
   *   the complete entries with IDs matching the specified range.
   */
  final def xRange[SK: Schema, I: Schema](
    key: SK,
    start: I,
    end: I
  ): ResultBuilder2[({ type lambda[x, y] = StreamEntries[I, x, y] })#lambda, G] =
    new ResultBuilder2[({ type lambda[x, y] = StreamEntries[I, x, y] })#lambda, G] {
      def returning[RK: Schema, RV: Schema]: G[StreamEntries[I, RK, RV]] = {
        val command = RedisCommand(
          XRange,
          Tuple4(
            ArbitraryKeyInput[SK](),
            ArbitraryValueInput[I](),
            ArbitraryValueInput[I](),
            OptionalInput(CountInput)
          ),
          StreamEntriesOutput[I, RK, RV]()
        )
        command.run((key, start, end, None))
      }
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
  ): ResultBuilder2[({ type lambda[x, y] = StreamEntries[I, x, y] })#lambda, G] =
    new ResultBuilder2[({ type lambda[x, y] = StreamEntries[I, x, y] })#lambda, G] {
      def returning[RK: Schema, RV: Schema]: G[StreamEntries[I, RK, RV]] = {
        val command = RedisCommand(
          XRange,
          Tuple4(
            ArbitraryKeyInput[SK](),
            ArbitraryValueInput[I](),
            ArbitraryValueInput[I](),
            OptionalInput(CountInput)
          ),
          StreamEntriesOutput[I, RK, RV]()
        )
        command.run((key, start, end, Some(Count(count))))
      }
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
  final def xRead[SK: Schema, I: Schema](
    count: Option[Long] = None,
    block: Option[Duration] = None
  )(
    stream: (SK, I),
    streams: (SK, I)*
  ): ResultBuilder2[({ type lambda[x, y] = StreamChunks[SK, I, x, y] })#lambda, G] =
    new ResultBuilder2[({ type lambda[x, y] = StreamChunks[SK, I, x, y] })#lambda, G] {
      def returning[RK: Schema, RV: Schema]: G[StreamChunks[SK, I, RK, RV]] = {
        val command = RedisCommand(
          XRead,
          Tuple3(OptionalInput(CountInput), OptionalInput(BlockInput), StreamsInput[SK, I]()),
          ChunkOutput(StreamOutput[SK, I, RK, RV]())
        )
        command.run((count.map(Count), block, (stream, Chunk.fromIterable(streams))))
      }
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
  ): ResultBuilder2[({ type lambda[x, y] = StreamChunks[SK, I, x, y] })#lambda, G] =
    new ResultBuilder2[({ type lambda[x, y] = StreamChunks[SK, I, x, y] })#lambda, G] {
      def returning[RK: Schema, RV: Schema]: G[StreamChunks[SK, I, RK, RV]] = {
        val command =
          RedisCommand(
            XReadGroup,
            Tuple6(
              ArbitraryValueInput[SG](),
              ArbitraryValueInput[SC](),
              OptionalInput(CountInput),
              OptionalInput(BlockInput),
              OptionalInput(NoAckInput),
              StreamsInput[SK, I]()
            ),
            ChunkOutput(StreamOutput[SK, I, RK, RV]())
          )

        val noAckOpt = if (noAck) Some(NoAck) else None
        command.run((group, consumer, count.map(Count), block, noAckOpt, (stream, Chunk.fromIterable(streams))))
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
  ): ResultBuilder2[({ type lambda[x, y] = StreamEntries[I, x, y] })#lambda, G] =
    new ResultBuilder2[({ type lambda[x, y] = StreamEntries[I, x, y] })#lambda, G] {
      def returning[RK: Schema, RV: Schema]: G[StreamEntries[I, RK, RV]] = {
        val command = RedisCommand(
          XRevRange,
          Tuple4(
            ArbitraryKeyInput[SK](),
            ArbitraryValueInput[I](),
            ArbitraryValueInput[I](),
            OptionalInput(CountInput)
          ),
          StreamEntriesOutput[I, RK, RV]()
        )
        command.run((key, end, start, None))
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
  ): ResultBuilder2[({ type lambda[x, y] = StreamEntries[I, x, y] })#lambda, G] =
    new ResultBuilder2[({ type lambda[x, y] = StreamEntries[I, x, y] })#lambda, G] {
      def returning[RK: Schema, RV: Schema]: G[StreamEntries[I, RK, RV]] = {
        val command = RedisCommand(
          XRevRange,
          Tuple4(
            ArbitraryKeyInput[SK](),
            ArbitraryValueInput[I](),
            ArbitraryValueInput[I](),
            OptionalInput(CountInput)
          ),
          StreamEntriesOutput[I, RK, RV]()
        )
        command.run((key, end, start, Some(Count(count))))
      }
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
  final def xTrimWithMaxLen[SK: Schema](
    key: SK,
    count: Long,
    approximate: Boolean = false,
    limit: Option[Long] = None
  ): G[Long] =
    if (approximate) {
      val command = RedisCommand(XTrim, Tuple2(ArbitraryKeyInput[SK](), MaxLenApproxInput), LongOutput)
      command.run((key, CappedStreamType.MaxLenApprox(count, limit)))
    } else {
      val command = RedisCommand(XTrim, Tuple2(ArbitraryKeyInput[SK](), MaxLenExactInput), LongOutput)
      command.run((key, CappedStreamType.MaxLenExact(count)))
    }

  /**
   * Trims the stream to a given number of items, evicting older items (items with lower IDs) if needed.
   *
   * @param key
   *   ID of the stream
   * @param minId
   *   minimal stream id
   * @param approximate
   *   flag that indicates if the stream length should be exactly count or few tens of entries more
   * @return
   *   the number of entries deleted from the stream.
   */
  final def xTrimWithMinId[SK: Schema, I: Schema](
    key: SK,
    minId: I,
    approximate: Boolean = false,
    limit: Option[Long] = None
  ): G[Long] =
    if (approximate) {
      val command = RedisCommand(XTrim, Tuple2(ArbitraryKeyInput[SK](), MinIdApproxInput[I]()), LongOutput)
      command.run((key, CappedStreamType.MinIdApprox(minId, limit)))
    } else {
      val command = RedisCommand(XTrim, Tuple2(ArbitraryKeyInput[SK](), MinIdExactInput[I]()), LongOutput)
      command.run((key, CappedStreamType.MinIdExact(minId)))
    }
}

private object Streams {
  final val XAck           = "XACK"
  final val XAdd           = "XADD"
  final val XAutoClaim     = "XAUTOCLAIM"
  final val XClaim         = "XCLAIM"
  final val XDel           = "XDEL"
  final val XGroup         = "XGROUP"
  final val XInfoConsumers = "XINFO CONSUMERS"
  final val XInfoGroups    = "XINFO GROUPS"
  final val XInfoStream    = "XINFO STREAM"
  final val XLen           = "XLEN"
  final val XPending       = "XPENDING"
  final val XRange         = "XRANGE"
  final val XRead          = "XREAD"
  final val XReadGroup     = "XREADGROUP GROUP"
  final val XRevRange      = "XREVRANGE"
  final val XTrim          = "XTRIM"
}
