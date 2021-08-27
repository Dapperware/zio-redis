package zio.redis.api

import zio.duration._
import zio.redis.Input._
import zio.redis.Output.{ StreamGroupsInfoOutput, _ }
import zio.redis._
import zio.schema.Schema
import zio.{ Chunk, ZIO }

trait Streams {
  import Streams._
  import XGroupCommand._

  /**
   * Removes one or multiple messages from the pending entries list (PEL) of a stream consumer group.
   *
   * @param key ID of the stream
   * @param group ID of the consumer group
   * @param id ID of the message
   * @param ids IDs of the rest of the messages
   * @return the number of messages successfully acknowledged.
   */
  final def xAck[SK: Schema, G: Schema, I: Schema](
    key: SK,
    group: G,
    id: I,
    ids: I*
  ): ZIO[RedisExecutor, RedisError, Long] = {
    val command = RedisCommand(
      XAck,
      Tuple3(ArbitraryInput[SK](), ArbitraryInput[G](), NonEmptyList(ArbitraryInput[I]())),
      LongOutput
    )
    command.run((key, group, (id, ids.toList)))
  }

  /**
   * Appends the specified stream entry to the stream at the specified key.
   *
   * @param key ID of the stream
   * @param id ID of the message
   * @param pair field and value pair
   * @param pairs rest of the field and value pairs
   * @return ID of the added entry.
   */
  final def xAdd[SK: Schema, I: Schema, K: Schema, V: Schema](
    key: SK,
    id: I,
    pair: (K, V),
    pairs: (K, V)*
  ): ResultBuilder[Id] =
    new ResultBuilder[Id] {
      def returning[R: Schema]: ZIO[RedisExecutor, RedisError, Id[R]] = {
        val command = RedisCommand(
          XAdd,
          Tuple4(
            ArbitraryInput[SK](),
            OptionalInput(StreamMaxLenInput),
            ArbitraryInput[I](),
            NonEmptyList(Tuple2(ArbitraryInput[K](), ArbitraryInput[V]()))
          ),
          ArbitraryOutput[R]()
        )
        command.run((key, None, id, (pair, pairs.toList)))
      }
    }

  // TODO: how to handle F[_, _, _]
  /**
   * An introspection command used in order to retrieve different information about the stream.
   *
   * @param key ID of the stream
   * @return General information about the stream stored at the specified key.
   */
  final def xInfoStream[SK: Schema, RI: Schema, RK: Schema, RV: Schema](
    key: SK
  ): ZIO[RedisExecutor, RedisError, StreamInfo[RI, RK, RV]] = {
    val command = RedisCommand(XInfoStream, ArbitraryInput[SK](), StreamInfoOutput[RI, RK, RV]())
    command.run(key)
  }

  // TODO: how to handle F[_, _, _]
  /**
   * Returns the entire state of the stream, including entries, groups, consumers and PELs.
   *
   * @param key ID of the stream
   * @return General information about the stream stored at the specified key.
   */
  final def xInfoStreamFull[SK: Schema, RI: Schema, RK: Schema, RV: Schema](
    key: SK
  ): ZIO[RedisExecutor, RedisError, StreamInfoWithFull.FullStreamInfo[RI, RK, RV]] = {
    val command = RedisCommand(
      XInfoStream,
      Tuple2(ArbitraryInput[SK](), ArbitraryInput[String]()),
      StreamInfoFullOutput[RI, RK, RV]()
    )
    command.run((key, "FULL"))
  }

  // TODO: how to handle F[_, _, _]
  /**
   * Returns the entire state of the stream, including entries, groups, consumers and PELs.
   *
   * @param key ID of the stream
   * @param count limit the amount of stream/PEL entries that are returned (The first <count> entries are returned)
   * @return General information about the stream stored at the specified key.
   */
  final def xInfoStreamFull[SK: Schema, RI: Schema, RK: Schema, RV: Schema](
    key: SK,
    count: Long
  ): ZIO[RedisExecutor, RedisError, StreamInfoWithFull.FullStreamInfo[RI, RK, RV]] = {
    val command = RedisCommand(
      XInfoStream,
      Tuple3(ArbitraryInput[SK](), ArbitraryInput[String](), CountInput),
      StreamInfoFullOutput[RI, RK, RV]()
    )
    command.run((key, "FULL", Count(count)))
  }

  /**
   * An introspection command used in order to retrieve different information about the group.
   *
   * @param key ID of the stream
   * @return List of consumer groups associated with the stream stored at the specified key.
   */
  final def xInfoGroups[SK: Schema](key: SK): ZIO[RedisExecutor, RedisError, Chunk[StreamGroupsInfo]] = {
    val command = RedisCommand(XInfoGroups, ArbitraryInput[SK](), StreamGroupsInfoOutput)
    command.run(key)
  }

  /**
   * An introspection command used in order to retrieve different information about the consumers.
   *
   * @param key ID of the stream
   * @param group ID of the consumer group
   * @return List of every consumer in a specific consumer group.
   */
  final def xInfoConsumers[SK: Schema, SG: Schema](
    key: SK,
    group: SG
  ): ZIO[RedisExecutor, RedisError, Chunk[StreamConsumersInfo]] = {
    val command =
      RedisCommand(XInfoConsumers, Tuple2(ArbitraryInput[SK](), ArbitraryInput[SG]()), StreamConsumersInfoOutput)
    command.run((key, group))
  }

  /**
   * Appends the specified stream entry to the stream at the specified key while limiting the size of the stream.
   *
   * @param key ID of the stream
   * @param id ID of the message
   * @param count maximum number of elements in a stream
   * @param approximate flag that indicates if a stream should be limited to the exact number of elements
   * @param pair field and value pair
   * @param pairs rest of the field and value pairs
   * @return ID of the added entry.
   */
  final def xAddWithMaxLen[SK: Schema, I: Schema, K: Schema, V: Schema](
    key: SK,
    id: I,
    count: Long,
    approximate: Boolean = false
  )(
    pair: (K, V),
    pairs: (K, V)*
  ): ResultBuilder[Id] =
    new ResultBuilder[Id] {
      def returning[R: Schema]: ZIO[RedisExecutor, RedisError, Id[R]] = {
        val command = RedisCommand(
          XAdd,
          Tuple4(
            ArbitraryInput[SK](),
            OptionalInput(StreamMaxLenInput),
            ArbitraryInput[I](),
            NonEmptyList(Tuple2(ArbitraryInput[K](), ArbitraryInput[V]()))
          ),
          ArbitraryOutput[R]()
        )
        command.run((key, Some(StreamMaxLen(approximate, count)), id, (pair, pairs.toList)))
      }
    }

  // TODO: how to handle this output
  /**
   * Changes the ownership of a pending message.
   *
   * @param key ID of the stream
   * @param group ID of the consumer group
   * @param consumer ID of the consumer
   * @param minIdleTime minimum idle time of a message
   * @param idle idle time (last time it was delivered) of the message that will be set
   * @param time same as idle but instead of a relative amount of milliseconds, it sets the idle time to a specific Unix
   *             time (in milliseconds)
   * @param retryCount retry counter of a message that will be set
   * @param force flag that indicates that a message doesn't have to be in a pending entries list (PEL)
   * @param id ID of a message
   * @param ids IDs of the rest of the messages
   * @return messages successfully claimed.
   */
  final def xClaim[SK: Schema, SG: Schema, SC: Schema, I: Schema, RK: Schema, RV: Schema](
    key: SK,
    group: SG,
    consumer: SC,
    minIdleTime: Duration,
    idle: Option[Duration] = None,
    time: Option[Duration] = None,
    retryCount: Option[Long] = None,
    force: Boolean = false
  )(id: I, ids: I*): ZIO[RedisExecutor, RedisError, Map[I, Map[RK, RV]]] = {
    val command = RedisCommand(
      XClaim,
      Tuple9(
        ArbitraryInput[SK](),
        ArbitraryInput[SG](),
        ArbitraryInput[SC](),
        DurationMillisecondsInput,
        NonEmptyList(ArbitraryInput[I]()),
        OptionalInput(IdleInput),
        OptionalInput(TimeInput),
        OptionalInput(RetryCountInput),
        OptionalInput(WithForceInput)
      ),
      StreamOutput[I, RK, RV]()
    )
    val forceOpt = if (force) Some(WithForce) else None
    command.run((key, group, consumer, minIdleTime, (id, ids.toList), idle, time, retryCount, forceOpt))
  }

  /**
   * Changes the ownership of a pending message.
   *
   * @param key ID of the stream
   * @param group ID of the consumer group
   * @param consumer ID of the consumer
   * @param minIdleTime minimum idle time of a message
   * @param idle idle time (last time it was delivered) of the message that will be set
   * @param time same as idle but instead of a relative amount of milliseconds, it sets the idle time to a specific Unix
   *             time (in milliseconds)
   * @param retryCount retry counter of a message that will be set
   * @param force flag that indicates that a message doesn't have to be in a pending entries list (PEL)
   * @param id ID of a message
   * @param ids IDs of the rest of the messages
   * @return IDs of the messages that are successfully claimed.
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
  )(id: I, ids: I*): ResultBuilder[Chunk] =
    new ResultBuilder[Chunk] {
      def returning[R: Schema]: ZIO[RedisExecutor, RedisError, Chunk[R]] = {
        val command = RedisCommand(
          XClaim,
          Tuple10(
            ArbitraryInput[SK](),
            ArbitraryInput[SG](),
            ArbitraryInput[SC](),
            DurationMillisecondsInput,
            NonEmptyList(ArbitraryInput[I]()),
            OptionalInput(IdleInput),
            OptionalInput(TimeInput),
            OptionalInput(RetryCountInput),
            OptionalInput(WithForceInput),
            WithJustIdInput
          ),
          ChunkOutput(ArbitraryOutput[R]())
        )
        val forceOpt = if (force) Some(WithForce) else None
        command.run((key, group, consumer, minIdleTime, (id, ids.toList), idle, time, retryCount, forceOpt, WithJustId))
      }
    }

  /**
   * Removes the specified entries from a stream.
   *
   * @param key ID of the stream
   * @param id ID of the message
   * @param ids IDs of the rest of the messages
   * @return the number of entries deleted.
   */
  final def xDel[SK: Schema, I: Schema](key: SK, id: I, ids: I*): ZIO[RedisExecutor, RedisError, Long] = {
    val command = RedisCommand(XDel, Tuple2(ArbitraryInput[SK](), NonEmptyList(ArbitraryInput[I]())), LongOutput)
    command.run((key, (id, ids.toList)))
  }

  /**
   * Create a new consumer group associated with a stream.
   *
   * @param key ID of the stream
   * @param group ID of the consumer group
   * @param id ID of the last item in the stream to consider already delivered
   * @param mkStream ID of the last item in the stream to consider already delivered
   */
  final def xGroupCreate[SK: Schema, SG: Schema, I: Schema](
    key: SK,
    group: SG,
    id: I,
    mkStream: Boolean = false
  ): ZIO[RedisExecutor, RedisError, Unit] = {
    val command = RedisCommand(XGroup, XGroupCreateInput[SK, SG, I](), UnitOutput)
    command.run(Create(key, group, id, mkStream))
  }

  /**
   * Set the consumer group last delivered ID to something else.
   *
   * @param key ID of the stream
   * @param group ID of the consumer group
   * @param id last delivered ID to set
   */
  final def xGroupSetId[SK: Schema, SG: Schema, I: Schema](
    key: SK,
    group: SG,
    id: I
  ): ZIO[RedisExecutor, RedisError, Unit] = {
    val command = RedisCommand(XGroup, XGroupSetIdInput[SK, SG, I](), UnitOutput)
    command.run(SetId(key, group, id))
  }

  /**
   * Destroy a consumer group.
   *
   * @param key ID of the stream
   * @param group ID of the consumer group
   * @return flag that indicates if the deletion was successful.
   */
  final def xGroupDestroy[SK: Schema, SG: Schema](key: SK, group: SG): ZIO[RedisExecutor, RedisError, Boolean] =
    RedisCommand(XGroup, XGroupDestroyInput[SK, SG](), BoolOutput).run(Destroy(key, group))

  /**
   * Create a new consumer associated with a consumer group.
   *
   * @param key ID of the stream
   * @param group ID of the consumer group
   * @param consumer ID of the consumer
   * @return the number of created consumer groups.
   */
  final def xGroupCreateConsumer[SK: Schema, SG: Schema, SC: Schema](
    key: SK,
    group: SG,
    consumer: SC
  ): ZIO[RedisExecutor, RedisError, Boolean] = {
    val command = RedisCommand(XGroup, XGroupCreateConsumerInput[SK, SG, SC](), BoolOutput)
    command.run(CreateConsumer(key, group, consumer))
  }

  /**
   * Remove a specific consumer from a consumer group.
   *
   * @param key ID of the stream
   * @param group ID of the consumer group
   * @param consumer ID of the consumer
   * @return the number of pending messages that the consumer had before it was deleted.
   */
  final def xGroupDelConsumer[SK: Schema, SG: Schema, SC: Schema](
    key: SK,
    group: SG,
    consumer: SC
  ): ZIO[RedisExecutor, RedisError, Long] = {
    val command = RedisCommand(XGroup, XGroupDelConsumerInput[SK, SG, SC](), LongOutput)
    command.run(DelConsumer(key, group, consumer))
  }

  /**
   * Fetches the number of entries inside a stream.
   *
   * @param key ID of the stream
   * @return the number of entries inside a stream.
   */
  final def xLen[SK: Schema](key: SK): ZIO[RedisExecutor, RedisError, Long] = {
    val command = RedisCommand(XLen, ArbitraryInput[SK](), LongOutput)
    command.run(key)
  }

  /**
   * Inspects the list of pending messages.
   *
   * @param key ID of the stream
   * @param group ID of the consumer group
   * @return summary about the pending messages in a given consumer group.
   */
  final def xPending[SK: Schema, SG: Schema](key: SK, group: SG): ZIO[RedisExecutor, RedisError, PendingInfo] = {
    val command = RedisCommand(
      XPending,
      Tuple3(ArbitraryInput[SK](), ArbitraryInput[SG](), OptionalInput(IdleInput)),
      XPendingOutput
    )
    command.run((key, group, None))
  }

  /**
   * Inspects the list of pending messages.
   *
   * @param key ID of the stream
   * @param group ID of the consumer group
   * @param start start of the range of IDs
   * @param end end of the range of IDs
   * @param count maximum number of messages returned
   * @param consumer ID of the consumer
   * @param idle idle time of a pending message by which message are filtered
   * @return detailed information for each message in the pending entries list.
   */
  final def xPending[SK: Schema, SG: Schema, I: Schema, SC: Schema](
    key: SK,
    group: SG,
    start: I,
    end: I,
    count: Long,
    consumer: Option[SC] = None,
    idle: Option[Duration] = None
  ): ZIO[RedisExecutor, RedisError, Chunk[PendingMessage]] = {
    val command = RedisCommand(
      XPending,
      Tuple7(
        ArbitraryInput[SK](),
        ArbitraryInput[SG](),
        OptionalInput(IdleInput),
        ArbitraryInput[I](),
        ArbitraryInput[I](),
        LongInput,
        OptionalInput(ArbitraryInput[SC]())
      ),
      PendingMessagesOutput
    )
    command.run((key, group, idle, start, end, count, consumer))
  }

  // TODO: how to handle this output
  /**
   * Fetches the stream entries matching a given range of IDs.
   *
   * @param key ID of the stream
   * @param start start of the range of IDs
   * @param end end of the range of IDs
   * @return the complete entries with IDs matching the specified range.
   */
  final def xRange[SK: Schema, I: Schema, RK: Schema, RV: Schema](
    key: SK,
    start: I,
    end: I
  ): ZIO[RedisExecutor, RedisError, Map[I, Map[RK, RV]]] = {
    val command = RedisCommand(
      XRange,
      Tuple4(ArbitraryInput[SK](), ArbitraryInput[I](), ArbitraryInput[I](), OptionalInput(CountInput)),
      StreamOutput[I, RK, RV]()
    )
    command.run((key, start, end, None))
  }

  // TODO: how to handle this output
  /**
   * Fetches the stream entries matching a given range of IDs.
   *
   * @param key ID of the stream
   * @param start start of the range of IDs
   * @param end end of the range of IDs
   * @param count maximum number of entries returned
   * @return the complete entries with IDs matching the specified range.
   */
  final def xRange[SK: Schema, I: Schema, RK: Schema, RV: Schema](
    key: SK,
    start: I,
    end: I,
    count: Long
  ): ZIO[RedisExecutor, RedisError, Map[I, Map[RK, RV]]] = {
    val command = RedisCommand(
      XRange,
      Tuple4(ArbitraryInput[SK](), ArbitraryInput[I](), ArbitraryInput[I](), OptionalInput(CountInput)),
      StreamOutput[I, RK, RV]()
    )
    command.run((key, start, end, Some(Count(count))))
  }

  // TODO: how to handle this output
  /**
   * Read data from one or multiple streams.
   *
   * @param count maximum number of elements returned per stream
   * @param block duration for which we want to block before timing out
   * @param stream pair that contains stream ID and the last ID that the consumer received for that stream
   * @param streams rest of the pairs
   * @return complete entries with an ID greater than the last received ID per stream.
   */
  final def xRead[SK: Schema, I: Schema, RK: Schema, RV: Schema](
    count: Option[Long] = None,
    block: Option[Duration] = None
  )(
    stream: (SK, I),
    streams: (SK, I)*
  ): ZIO[RedisExecutor, RedisError, Map[SK, Map[I, Map[RK, RV]]]] = {
    val command = RedisCommand(
      XRead,
      Tuple3(OptionalInput(CountInput), OptionalInput(BlockInput), StreamsInput[SK, I]()),
      KeyValueTwoOutput(ArbitraryOutput[SK](), StreamOutput[I, RK, RV]())
    )
    command.run((count.map(Count), block, (stream, Chunk.fromIterable(streams))))
  }

  // TODO: how to handle this output
  /**
   * Read data from one or multiple streams using consumer group.
   *
   * @param group ID of the consumer group
   * @param consumer ID of the consumer
   * @param count maximum number of elements returned per stream
   * @param block duration for which we want to block before timing out
   * @param noAck flag that indicates that the read messages shouldn't be added to the pending entries list (PEL)
   * @param stream pair that contains stream ID and the last ID that the consumer received for that stream
   * @param streams rest of the pairs
   * @return complete entries with an ID greater than the last received ID per stream.
   */
  final def xReadGroup[SG: Schema, SC: Schema, SK: Schema, I: Schema, RK: Schema, RV: Schema](
    group: SG,
    consumer: SC,
    count: Option[Long] = None,
    block: Option[Duration] = None,
    noAck: Boolean = false
  )(
    stream: (SK, I),
    streams: (SK, I)*
  ): ZIO[RedisExecutor, RedisError, Map[SK, Map[I, Map[RK, RV]]]] = {
    val command = RedisCommand(
      XReadGroup,
      Tuple6(
        ArbitraryInput[SG](),
        ArbitraryInput[SC](),
        OptionalInput(CountInput),
        OptionalInput(BlockInput),
        OptionalInput(NoAckInput),
        StreamsInput[SK, I]()
      ),
      KeyValueTwoOutput(ArbitraryOutput[SK](), StreamOutput[I, RK, RV]())
    )
    val noAckOpt = if (noAck) Some(NoAck) else None
    command.run((group, consumer, count.map(Count), block, noAckOpt, (stream, Chunk.fromIterable(streams))))
  }

  // TODO: how to handle this output
  /**
   * Fetches the stream entries matching a given range of IDs in the reverse order.
   *
   * @param key ID of the stream
   * @param end end of the range of IDs
   * @param start start of the range of IDs
   * @return the complete entries with IDs matching the specified range in the reverse order.
   */
  final def xRevRange[SK: Schema, I: Schema, RK: Schema, RV: Schema](
    key: SK,
    end: I,
    start: I
  ): ZIO[RedisExecutor, RedisError, Map[I, Map[RK, RV]]] = {
    val command = RedisCommand(
      XRevRange,
      Tuple4(ArbitraryInput[SK](), ArbitraryInput[I](), ArbitraryInput[I](), OptionalInput(CountInput)),
      StreamOutput[I, RK, RV]()
    )
    command.run((key, end, start, None))
  }

  // TODO: how to handle this output
  /**
   * Fetches the stream entries matching a given range of IDs in the reverse order.
   *
   * @param key ID of the stream
   * @param end end of the range of IDs
   * @param start start of the range of IDs
   * @param count maximum number of entries returned
   * @return the complete entries with IDs matching the specified range in the reverse order.
   */
  final def xRevRange[SK: Schema, I: Schema, RK: Schema, RV: Schema](
    key: SK,
    end: I,
    start: I,
    count: Long
  ): ZIO[RedisExecutor, RedisError, Map[I, Map[RK, RV]]] = {
    val command = RedisCommand(
      XRevRange,
      Tuple4(ArbitraryInput[SK](), ArbitraryInput[I](), ArbitraryInput[I](), OptionalInput(CountInput)),
      StreamOutput[I, RK, RV]()
    )
    command.run((key, end, start, Some(Count(count))))
  }

  /**
   * Trims the stream to a given number of items, evicting older items (items with lower IDs) if needed.
   *
   * @param key ID of the stream
   * @param count stream length
   * @param approximate flag that indicates if the stream length should be exactly count or few tens of entries more
   * @return the number of entries deleted from the stream.
   */
  final def xTrim[SK: Schema](
    key: SK,
    count: Long,
    approximate: Boolean = false
  ): ZIO[RedisExecutor, RedisError, Long] = {
    val command = RedisCommand(XTrim, Tuple2(ArbitraryInput[SK](), StreamMaxLenInput), LongOutput)
    command.run((key, StreamMaxLen(approximate, count)))
  }
}

private object Streams {
  final val XAck           = "XACK"
  final val XAdd           = "XADD"
  final val XClaim         = "XCLAIM"
  final val XDel           = "XDEL"
  final val XGroup         = "XGROUP"
  final val XInfoStream    = "XINFO STREAM"
  final val XInfoGroups    = "XINFO GROUPS"
  final val XInfoConsumers = "XINFO CONSUMERS"
  final val XLen           = "XLEN"
  final val XPending       = "XPENDING"
  final val XRange         = "XRANGE"
  final val XRead          = "XREAD"
  final val XReadGroup     = "XREADGROUP GROUP"
  final val XRevRange      = "XREVRANGE"
  final val XTrim          = "XTRIM"
}
