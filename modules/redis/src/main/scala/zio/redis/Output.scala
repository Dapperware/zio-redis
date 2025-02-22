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

package zio.redis

import zio._
import zio.redis.internal.PubSub.{PushMessage, SubscriptionKey}
import zio.redis.internal.RespValue
import zio.redis.options.Cluster.{Node, Partition, SlotRange}
import zio.schema.Schema
import zio.schema.codec.BinaryCodec

import java.nio.charset.StandardCharsets

sealed trait Output[+A] { self =>
  protected def tryDecode(respValue: RespValue): A

  final def map[B](f: A => B): Output[B] =
    new Output[B] {
      protected def tryDecode(respValue: RespValue): B = f(self.tryDecode(respValue))
    }

  private[redis] final def unsafeDecode(respValue: RespValue): A =
    respValue match {
      case error: RespValue.Error => throw error.asRedisError
      case success                => tryDecode(success)
    }
}

object Output {
  import RedisError._

  def apply[A](implicit output: Output[A]): Output[A] = output

  final case class ArbitraryOutput[A]()(implicit codec: BinaryCodec[A]) extends Output[A] {
    protected def tryDecode(respValue: RespValue): A =
      respValue match {
        case RespValue.BulkString(s) => codec.decode(s).fold(e => throw CodecError(e.message), identity)
        case other                   => throw ProtocolError(s"$other isn't a bulk string")
      }
  }

  case object BoolOutput extends Output[Boolean] {
    protected def tryDecode(respValue: RespValue): Boolean =
      respValue match {
        case RespValue.Integer(0) => false
        case RespValue.Integer(1) => true
        case other                => throw ProtocolError(s"$other isn't a boolean")
      }
  }

  case object BulkStringOutput extends Output[Chunk[Byte]] {
    protected def tryDecode(respValue: RespValue): Chunk[Byte] =
      respValue match {
        case RespValue.BulkString(value) => value
        case other                       => throw ProtocolError(s"$other isn't a bulk string")
      }
  }

  final case class ChunkOutput[+A](output: Output[A]) extends Output[Chunk[A]] {
    protected def tryDecode(respValue: RespValue): Chunk[A] =
      respValue match {
        case RespValue.NullArray     => Chunk.empty
        case RespValue.Array(values) => values.map(output.tryDecode)
        case other                   => throw ProtocolError(s"$other isn't an array")
      }
  }

  final case class ChunkTuple2Output[+A, +B](_1: Output[A], _2: Output[B]) extends Output[Chunk[(A, B)]] {
    protected def tryDecode(respValue: RespValue): Chunk[(A, B)] =
      respValue match {
        case RespValue.NullArray =>
          Chunk.empty
        case RespValue.Array(values) if values.length % 2 == 0 =>
          Chunk.fromIterator(values.grouped(2).map(g => _1.tryDecode(g(0)) -> _2.tryDecode(g(1))))
        case array @ RespValue.Array(_) =>
          throw ProtocolError(s"$array doesn't have an even number of elements")
        case other =>
          throw ProtocolError(s"$other isn't an array")
      }
  }

  case object ClusterPartitionNodeOutput extends Output[Node] {
    protected def tryDecode(respValue: RespValue): Node =
      respValue match {
        case RespValue.NullArray     => throw ProtocolError(s"Array must not be empty")
        case RespValue.Array(values) =>
          val host   = MultiStringOutput.unsafeDecode(values(0))
          val port   = LongOutput.unsafeDecode(values(1))
          val nodeId = MultiStringOutput.unsafeDecode(values(2))
          Node(nodeId, RedisUri(host, port.toInt))
        case other                   => throw ProtocolError(s"$other isn't an array")
      }
  }

  case object ClusterPartitionOutput extends Output[Partition] {
    protected def tryDecode(respValue: RespValue): Partition =
      respValue match {
        case RespValue.NullArray     => throw ProtocolError(s"Array must not be empty")
        case RespValue.Array(values) =>
          val start  = LongOutput.unsafeDecode(values(0))
          val end    = LongOutput.unsafeDecode(values(1))
          val master = ClusterPartitionNodeOutput.unsafeDecode(values(2))
          val slaves = values.drop(3).map(ClusterPartitionNodeOutput.unsafeDecode)
          Partition(SlotRange(start, end), master, slaves)
        case other                   => throw ProtocolError(s"$other isn't an array")
      }
  }

  case object DoubleOutput extends Output[Double] {
    protected def tryDecode(respValue: RespValue): Double =
      respValue match {
        case RespValue.BulkString(bytes) => decodeDouble(bytes)
        case other                       => throw ProtocolError(s"$other isn't a double.")
      }
  }

  case object DoubleOrInfinity extends Output[Double] {
    protected def tryDecode(respValue: RespValue): Double =
      respValue match {
        case RespValue.BulkString(bytes) => decodeDouble(bytes, withInfinity = true)
        case other                       => throw ProtocolError(s"$other isn't a double or an infinity.")
      }
  }

  private object DurationOutput extends Output[Long] {
    protected def tryDecode(respValue: RespValue): Long =
      respValue match {
        case RespValue.Integer(-2L) => throw ProtocolError("Key not found.")
        case RespValue.Integer(-1L) => throw ProtocolError("Key has no expire.")
        case RespValue.Integer(n)   => n
        case other                  => throw ProtocolError(s"$other isn't a duration.")
      }
  }

  final val DurationMillisecondsOutput: Output[Duration] = DurationOutput.map(_.milliseconds)

  final val DurationSecondsOutput: Output[Duration] = DurationOutput.map(_.seconds)

  case object GeoOutput extends Output[Chunk[Option[LongLat]]] {
    protected def tryDecode(respValue: RespValue): Chunk[Option[LongLat]] =
      respValue match {
        case RespValue.NullArray       =>
          Chunk.empty
        case RespValue.Array(elements) =>
          elements.map {
            case RespValue.NullArray                                                          => None
            case RespValue.ArrayValues(RespValue.BulkString(long), RespValue.BulkString(lat)) =>
              Some(LongLat(decodeDouble(long), decodeDouble(lat)))
            case other                                                                        =>
              throw ProtocolError(s"$other was not a longitude,latitude pair")
          }
        case other                     =>
          throw ProtocolError(s"$other isn't geo output")
      }
  }

  case object GeoRadiusOutput extends Output[Chunk[GeoView]] {
    protected def tryDecode(respValue: RespValue): Chunk[GeoView] =
      respValue match {
        case RespValue.Array(elements) =>
          elements.map {
            case s @ RespValue.BulkString(_)                                       =>
              GeoView(s.asString, None, None, None)
            case RespValue.ArrayValues(name @ RespValue.BulkString(_), infos @ _*) =>
              val distance = infos.collectFirst { case RespValue.BulkString(bytes) => decodeDouble(bytes) }

              val hash = infos.collectFirst { case RespValue.Integer(i) => i }

              val position = infos.collectFirst {
                case RespValue.ArrayValues(RespValue.BulkString(long), RespValue.BulkString(lat)) =>
                  LongLat(decodeDouble(long), decodeDouble(lat))
              }

              GeoView(name.asString, distance, hash, position)

            case other => throw ProtocolError(s"$other is not a geo radious output")
          }

        case other => throw ProtocolError(s"$other is not an array")
      }
  }

  case object KeyElemOutput extends Output[Option[(String, String)]] {
    protected def tryDecode(respValue: RespValue): Option[(String, String)] =
      respValue match {
        case RespValue.NullArray                                                             =>
          None
        case RespValue.ArrayValues(a @ RespValue.BulkString(_), b @ RespValue.BulkString(_)) =>
          Some((a.asString, b.asString))
        case other                                                                           => throw ProtocolError(s"$other isn't blPop output")
      }
  }

  final case class KeyValueOutput[K, V](outK: Output[K], outV: Output[V]) extends Output[Map[K, V]] {
    protected def tryDecode(respValue: RespValue): Map[K, V] =
      respValue match {
        case RespValue.NullArray =>
          Map.empty[K, V]
        case RespValue.Array(elements) if elements.length % 2 == 0 =>
          val output = collection.mutable.Map.empty[K, V]
          val len    = elements.length
          var pos    = 0

          while (pos < len) {
            val key   = outK.unsafeDecode(elements(pos))
            val value = outV.unsafeDecode(elements(pos + 1))
            output += key -> value
            pos += 2
          }

          output.toMap
        case array @ RespValue.Array(_) =>
          throw ProtocolError(s"$array doesn't have an even number of elements")
        case other =>
          throw ProtocolError(s"$other isn't an array")
      }
  }

  case object LongOutput extends Output[Long] {
    protected def tryDecode(respValue: RespValue): Long =
      respValue match {
        case RespValue.Integer(v) => v
        case other                => throw ProtocolError(s"$other isn't an integer")
      }
  }

  final case class MultiStringChunkOutput[+A](output: Output[A]) extends Output[Chunk[A]] {
    protected def tryDecode(respValue: RespValue): Chunk[A] =
      respValue match {
        case RespValue.NullBulkString    => Chunk.empty
        case s @ RespValue.BulkString(_) => Chunk.single(output.tryDecode(s))
        case RespValue.Array(elements)   => elements.map(output.tryDecode)
        case other                       => throw ProtocolError(s"$other isn't a string nor an array")
      }
  }

  case object MultiStringOutput extends Output[String] {
    protected def tryDecode(respValue: RespValue): String =
      respValue match {
        case s @ RespValue.BulkString(_) => s.asString
        case other                       => throw ProtocolError(s"$other isn't a bulk string")
      }
  }

  final case class OptionalOutput[+A](output: Output[A]) extends Output[Option[A]] {
    protected def tryDecode(respValue: RespValue): Option[A] =
      respValue match {
        case RespValue.NullBulkString | RespValue.NullArray => None
        case other                                          => Some(output.tryDecode(other))
      }
  }

  case object PendingMessagesOutput extends Output[Chunk[PendingMessage]] {
    protected def tryDecode(respValue: RespValue): Chunk[PendingMessage] =
      respValue match {
        case RespValue.Array(messages) =>
          messages.collect {
            case RespValue.Array(
                  Seq(
                    id @ RespValue.BulkString(_),
                    owner @ RespValue.BulkString(_),
                    RespValue.Integer(lastDelivered),
                    RespValue.Integer(counter)
                  )
                ) =>
              PendingMessage(id.asString, owner.asString, lastDelivered.millis, counter)
            case other =>
              throw ProtocolError(s"$other isn't an array with four elements")
          }

        case other =>
          throw ProtocolError(s"$other isn't an array")
      }
  }

  case object ResetOutput extends Output[Unit] {
    protected def tryDecode(respValue: RespValue): Unit =
      respValue match {
        case RespValue.SimpleString("RESET") => ()
        case other                           => throw ProtocolError(s"$other isn't unit.")
      }
  }

  case object RespValueOutput extends Output[RespValue] {
    protected def tryDecode(respValue: RespValue): RespValue = respValue
  }

  final case class ScanOutput[+A](output: Output[A]) extends Output[(Long, Chunk[A])] {
    protected def tryDecode(respValue: RespValue): (Long, Chunk[A]) =
      respValue match {
        case RespValue.ArrayValues(cursor @ RespValue.BulkString(_), RespValue.Array(items)) =>
          (cursor.asLong, items.map(output.tryDecode))
        case other                                                                           =>
          throw ProtocolError(s"$other isn't scan output")
      }
  }

  case object SetOutput extends Output[Boolean] {
    protected def tryDecode(respValue: RespValue): Boolean =
      respValue match {
        case RespValue.NullBulkString  => false
        case RespValue.SimpleString(_) => true
        case other                     => throw ProtocolError(s"$other isn't a valid set response")
      }
  }

  case object LcsOutput extends Output[Lcs] {
    protected def tryDecode(respValue: RespValue): Lcs =
      respValue match {
        case result @ RespValue.BulkString(_) => Lcs.PlainLcs(result.asString)
        case RespValue.Integer(length)        => Lcs.Length(length)
        case RespValue.ArrayValues(
              RespValue.BulkString(_),
              RespValue.Array(items),
              RespValue.BulkString(_),
              RespValue.Integer(length)
            ) =>
          val matches = items.map {
            case RespValue.Array(array) =>
              val matchIdxs   = array.collect { case RespValue.Array(values) =>
                val idxs = values.map {
                  case RespValue.Integer(value) => value
                  case other                    => throw ProtocolError(s"$other isn't a valid response")
                }
                if (idxs.size == 2)
                  MatchIdx(idxs.head, idxs(1))
                else throw ProtocolError(s"Response contains illegal number of indices for a match: ${idxs.size}")
              }
              val matchLength = array.collectFirst { case RespValue.Integer(value) => value }
              Match(matchIdxs(0), matchIdxs(1), matchLength)
            case other                  => throw ProtocolError(s"$other isn't a valid response")
          }
          Lcs.Matches(matches.toList, length)
        case other                            => throw ProtocolError(s"$other isn't a valid set response")
      }
  }

  case object StreamConsumersInfoOutput extends Output[Chunk[StreamConsumersInfo]] {
    protected def tryDecode(respValue: RespValue): Chunk[StreamConsumersInfo] =
      respValue match {
        case RespValue.NullArray       => Chunk.empty
        case RespValue.Array(messages) =>
          messages.collect {
            // Note that you should not rely on the fields exact position. see https://redis.io/commands/xinfo
            case RespValue.Array(elements) if elements.length % 2 == 0 =>
              var streamConsumersInfo: StreamConsumersInfo = StreamConsumersInfo.empty
              val len                                      = elements.length
              var pos                                      = 0
              while (pos < len) {
                (elements(pos), elements(pos + 1)) match {
                  case (key @ RespValue.BulkString(_), value @ RespValue.BulkString(_))
                      if key.asString == XInfoFields.Name =>
                    streamConsumersInfo = streamConsumersInfo.copy(name = value.asString)
                  case (key @ RespValue.BulkString(_), value @ RespValue.Integer(_)) =>
                    if (key.asString == XInfoFields.Pending)
                      streamConsumersInfo = streamConsumersInfo.copy(pending = value.value)
                    else if (key.asString == XInfoFields.Idle)
                      streamConsumersInfo = streamConsumersInfo.copy(idle = value.value.millis)
                    else if (key.asString == XInfoFields.Inactive)
                      streamConsumersInfo = streamConsumersInfo.copy(inactive = value.value.millis)
                  case _                                                             =>
                }
                pos += 2
              }
              streamConsumersInfo
            case array @ RespValue.Array(_) =>
              throw ProtocolError(s"$array doesn't have an even number of elements")
            case other =>
              throw ProtocolError(s"$other isn't an array")
          }

        case other =>
          throw ProtocolError(s"$other isn't an array")
      }
  }

  final case class StreamClaimedOutput[SI: BinaryCodec, I: BinaryCodec, K: BinaryCodec, V: BinaryCodec]()(implicit
    streamIdSchema: Schema[SI],
    idSchema: Schema[I],
    keySchema: Schema[K],
    valueSchema: Schema[V]
  ) extends Output[StreamClaimedEntries[SI, I, K, V]] {
    protected def tryDecode(respValue: RespValue): StreamClaimedEntries[SI, I, K, V] = {
      val (streamId, entries, deletedIds) = respValue match {
        case RespValue.ArrayValues(a, b, c) =>
          (
            ArbitraryOutput[SI]().unsafeDecode(a),
            StreamEntriesOutput[I, K, V]().unsafeDecode(b),
            ChunkOutput(ArbitraryOutput[I]()).unsafeDecode(c)
          )
        case RespValue.ArrayValues(a, b)    =>
          (ArbitraryOutput[SI]().unsafeDecode(a), StreamEntriesOutput[I, K, V]().unsafeDecode(b), Chunk.empty[I])
        case other                          => throw ProtocolError(s"$other isn't a valid array")
      }
      StreamClaimedEntries(streamId, entries, deletedIds)
    }
  }

  final case class StreamClaimedIdOutput[SI: BinaryCodec, I: BinaryCodec]()(implicit
    streamIdSchema: Schema[SI],
    idSchema: Schema[I]
  ) extends Output[StreamClaimedIdChunk[SI, I]] {
    protected def tryDecode(respValue: RespValue): StreamClaimedIdChunk[SI, I] = {
      val (streamId, messageIds, deletedIds) = respValue match {
        case RespValue.ArrayValues(a, b, c) =>
          (
            ArbitraryOutput[SI]().unsafeDecode(a),
            ChunkOutput(ArbitraryOutput[I]()).unsafeDecode(b),
            ChunkOutput(ArbitraryOutput[I]()).unsafeDecode(c)
          )
        case RespValue.ArrayValues(a, b)    =>
          (ArbitraryOutput[SI]().unsafeDecode(a), ChunkOutput(ArbitraryOutput[I]()).unsafeDecode(b), Chunk.empty[I])
        case other                          => throw ProtocolError(s"$other isn't a valid array")
      }
      StreamClaimedIdChunk(streamId, messageIds, deletedIds)
    }
  }

  final case class StreamEntriesOutput[I: BinaryCodec, K: BinaryCodec, V: BinaryCodec]()(implicit
    idSchema: Schema[I],
    keySchema: Schema[K],
    valueSchema: Schema[V]
  ) extends Output[Chunk[StreamEntry[I, K, V]]] {
    protected def tryDecode(respValue: RespValue): Chunk[StreamEntry[I, K, V]] =
      ChunkOutput(StreamEntryOutput[I, K, V]()).unsafeDecode(respValue)
  }

  final case class StreamEntryOutput[I: BinaryCodec, K: BinaryCodec, V: BinaryCodec]()(implicit
    idSchema: Schema[I],
    keySchema: Schema[K],
    valueSchema: Schema[V]
  ) extends Output[StreamEntry[I, K, V]] {
    protected def tryDecode(respValue: RespValue): StreamEntry[I, K, V] =
      respValue match {
        case RespValue.Array(Seq(id @ RespValue.BulkString(_), value)) =>
          val entryId = ArbitraryOutput[I]().unsafeDecode(id)
          val entry   = KeyValueOutput(ArbitraryOutput[K](), ArbitraryOutput[V]()).unsafeDecode(value)
          StreamEntry(entryId, entry)
        case other                                                     =>
          throw ProtocolError(s"$other isn't a valid array")
      }
  }

  case object StreamGroupsInfoOutput extends Output[Chunk[StreamGroupsInfo]] {
    protected def tryDecode(respValue: RespValue): Chunk[StreamGroupsInfo] =
      respValue match {
        case RespValue.NullArray       => Chunk.empty
        case RespValue.Array(messages) =>
          messages.collect {
            // Note that you should not rely on the fields exact position. see https://redis.io/commands/xinfo
            case RespValue.Array(elements) if elements.length % 2 == 0 =>
              var streamGroupsInfo: StreamGroupsInfo = StreamGroupsInfo.empty
              val len                                = elements.length
              var pos                                = 0
              while (pos < len) {
                (elements(pos), elements(pos + 1)) match {
                  case (key @ RespValue.BulkString(_), value @ RespValue.BulkString(_)) =>
                    if (key.asString == XInfoFields.Name)
                      streamGroupsInfo = streamGroupsInfo.copy(name = value.asString)
                    else if (key.asString == XInfoFields.LastDeliveredId)
                      streamGroupsInfo = streamGroupsInfo.copy(lastDeliveredId = value.asString)
                  case (key @ RespValue.BulkString(_), value @ RespValue.Integer(_))    =>
                    if (key.asString == XInfoFields.Pending)
                      streamGroupsInfo = streamGroupsInfo.copy(pending = value.value)
                    else if (key.asString == XInfoFields.Consumers)
                      streamGroupsInfo = streamGroupsInfo.copy(consumers = value.value)
                    else if (key.asString == XInfoFields.EntriesRead)
                      streamGroupsInfo = streamGroupsInfo.copy(entriesRead = value.value)
                    else if (key.asString == XInfoFields.Lag)
                      streamGroupsInfo = streamGroupsInfo.copy(lag = value.value)
                  case _                                                                =>
                }
                pos += 2
              }
              streamGroupsInfo
            case array @ RespValue.Array(_) =>
              throw ProtocolError(s"$array doesn't have an even number of elements")
            case other =>
              throw ProtocolError(s"$other isn't an array")
          }

        case other =>
          throw ProtocolError(s"$other isn't an array")
      }
  }

  final case class StreamInfoFullOutput[I: Schema: BinaryCodec, K: Schema: BinaryCodec, V: Schema: BinaryCodec]()
      extends Output[StreamInfoWithFull.FullStreamInfo[I, K, V]] {
    protected def tryDecode(
      respValue: RespValue
    ): StreamInfoWithFull.FullStreamInfo[I, K, V] = {
      var streamInfoFull: StreamInfoWithFull.FullStreamInfo[I, K, V] = StreamInfoWithFull.FullStreamInfo.empty
      respValue match {
        // Note that you should not rely on the fields exact position. see https://redis.io/commands/xinfo
        case RespValue.Array(elements) if elements.length % 2 == 0 =>
          var pos = 0
          while (pos < elements.length) {
            (elements(pos), elements(pos + 1)) match {
              // Get the basic information of the outermost stream.
              case (key @ RespValue.BulkString(_), value @ RespValue.Integer(_))                                  =>
                key.asString match {
                  case XInfoFields.Length         =>
                    streamInfoFull = streamInfoFull.copy(length = value.value)
                  case XInfoFields.RadixTreeNodes =>
                    streamInfoFull = streamInfoFull.copy(radixTreeNodes = value.value)
                  case XInfoFields.RadixTreeKeys  =>
                    streamInfoFull = streamInfoFull.copy(radixTreeKeys = value.value)
                  case XInfoFields.EntriesAdded   =>
                    streamInfoFull = streamInfoFull.copy(entriesAdded = value.value)
                  case _                          =>
                }
              case (key @ RespValue.BulkString(_), value @ RespValue.BulkString(_))                               =>
                key.asString match {
                  case XInfoFields.LastGeneratedId      =>
                    streamInfoFull = streamInfoFull.copy(lastGeneratedId = value.asString)
                  case XInfoFields.MaxDeletedEntryId    =>
                    streamInfoFull = streamInfoFull.copy(maxDeletedEntryId = value.asString)
                  case XInfoFields.RecordedFirstEntryId =>
                    streamInfoFull = streamInfoFull.copy(recordedFirstEntryId = value.asString)
                  case _                                =>
                }
              case (key @ RespValue.BulkString(_), value) if key.asString == XInfoFields.Entries                  =>
                streamInfoFull = streamInfoFull.copy(entries = StreamEntriesOutput[I, K, V]().unsafeDecode(value))
              case (key @ RespValue.BulkString(_), RespValue.Array(values)) if key.asString == XInfoFields.Groups =>
                // Get the group list of the stream.
                streamInfoFull = streamInfoFull.copy(groups = values.map(extractXInfoFullGroup))
              case _                                                                                              =>
            }
            pos += 2
          }
          streamInfoFull
        case array @ RespValue.Array(_) =>
          throw ProtocolError(s"$array doesn't have an even number of elements")
        case other =>
          throw ProtocolError(s"$other isn't an array")
      }
    }
  }

  final case class StreamInfoOutput[I: Schema: BinaryCodec, K: Schema: BinaryCodec, V: Schema: BinaryCodec]()
      extends Output[StreamInfo[I, K, V]] {
    protected def tryDecode(respValue: RespValue): StreamInfo[I, K, V] = {
      var streamInfo: StreamInfo[I, K, V] = StreamInfo.empty
      respValue match {
        // Note that you should not rely on the fields exact position. see https://redis.io/commands/xinfo
        case RespValue.Array(elements) if elements.length % 2 == 0 =>
          val len = elements.length
          var pos = 0
          while (pos < len) {
            (elements(pos), elements(pos + 1)) match {
              case (key @ RespValue.BulkString(_), value @ RespValue.Integer(_))    =>
                if (key.asString == XInfoFields.Length)
                  streamInfo = streamInfo.copy(length = value.value)
                else if (key.asString == XInfoFields.RadixTreeNodes)
                  streamInfo = streamInfo.copy(radixTreeNodes = value.value)
                else if (key.asString == XInfoFields.RadixTreeKeys)
                  streamInfo = streamInfo.copy(radixTreeKeys = value.value)
                else if (key.asString == XInfoFields.Groups)
                  streamInfo = streamInfo.copy(groups = value.value)
                else if (key.asString == XInfoFields.EntriesAdded)
                  streamInfo = streamInfo.copy(entriesAdded = value.value)
              case (key @ RespValue.BulkString(_), value @ RespValue.BulkString(_)) =>
                if (key.asString == XInfoFields.LastGeneratedId)
                  streamInfo = streamInfo.copy(lastGeneratedId = value.asString)
                else if (key.asString == XInfoFields.MaxDeletedEntryId)
                  streamInfo = streamInfo.copy(maxDeletedEntryId = value.asString)
                else if (key.asString == XInfoFields.RecordedFirstEntryId)
                  streamInfo = streamInfo.copy(recordedFirstEntryId = value.asString)
              case (key @ RespValue.BulkString(_), value @ RespValue.Array(_))      =>
                if (key.asString == XInfoFields.FirstEntry)
                  streamInfo = streamInfo.copy(firstEntry = Some(StreamEntryOutput[I, K, V]().unsafeDecode(value)))
                else if (key.asString == XInfoFields.LastEntry)
                  streamInfo = streamInfo.copy(lastEntry = Some(StreamEntryOutput[I, K, V]().unsafeDecode(value)))
              case _                                                                =>
            }
            pos += 2
          }
          streamInfo
        case array @ RespValue.Array(_) =>
          throw ProtocolError(s"$array doesn't have an even number of elements")

        case other =>
          throw ProtocolError(s"$other isn't an array")
      }
    }
  }

  final case class StreamOutput[N: BinaryCodec, I: BinaryCodec, K: BinaryCodec, V: BinaryCodec]()(implicit
    nameSchema: Schema[N],
    idSchema: Schema[I],
    keySchema: Schema[K],
    valueSchema: Schema[V]
  ) extends Output[StreamChunk[N, I, K, V]] {
    protected def tryDecode(respValue: RespValue): StreamChunk[N, I, K, V] = {
      val (name, entries) = Tuple2Output(ArbitraryOutput[N](), StreamEntriesOutput[I, K, V]()).unsafeDecode(respValue)
      StreamChunk(name, entries)
    }
  }

  case object StringOutput extends Output[String] {
    protected def tryDecode(respValue: RespValue): String =
      respValue match {
        case RespValue.SimpleString(s) => s
        case other                     => throw ProtocolError(s"$other isn't a simple string")
      }
  }

  final case class Tuple2Output[+A, +B](_1: Output[A], _2: Output[B]) extends Output[(A, B)] {
    protected def tryDecode(respValue: RespValue): (A, B) =
      respValue match {
        case RespValue.ArrayValues(a: RespValue, b: RespValue) => (_1.tryDecode(a), _2.tryDecode(b))
        case other                                             => throw ProtocolError(s"$other isn't a tuple2")
      }
  }

  final case class Tuple3Output[+A, +B, +C](_1: Output[A], _2: Output[B], _3: Output[C]) extends Output[(A, B, C)] {
    protected def tryDecode(respValue: RespValue): (A, B, C) =
      respValue match {
        case RespValue.ArrayValues(a: RespValue, b: RespValue, c: RespValue) =>
          (_1.tryDecode(a), _2.tryDecode(b), _3.tryDecode(c))
        case other                                                           => throw ProtocolError(s"$other isn't a tuple3")
      }
  }

  case object TypeOutput extends Output[RedisType] {
    protected def tryDecode(respValue: RespValue): RedisType =
      respValue match {
        case RespValue.SimpleString("string") => RedisType.String
        case RespValue.SimpleString("list")   => RedisType.List
        case RespValue.SimpleString("set")    => RedisType.Set
        case RespValue.SimpleString("zset")   => RedisType.SortedSet
        case RespValue.SimpleString("hash")   => RedisType.Hash
        case RespValue.SimpleString("stream") => RedisType.Stream
        case other                            => throw ProtocolError(s"$other isn't redis type.")
      }
  }

  case object UnitOutput extends Output[Unit] {
    protected def tryDecode(respValue: RespValue): Unit =
      respValue match {
        case RespValue.SimpleString("OK") => ()
        case other                        => throw ProtocolError(s"$other isn't unit.")
      }
  }

  final case class ZRandMemberOutput[+A](output: Output[A]) extends Output[Chunk[A]] {
    protected def tryDecode(respValue: RespValue): Chunk[A] =
      respValue match {
        case RespValue.NullBulkString => Chunk.empty
        case RespValue.NullArray      => Chunk.empty
        case RespValue.Array(values)  => values.map(output.tryDecode)
        case other                    => throw ProtocolError(s"$other isn't an array")
      }
  }

  final case class ZRandMemberTuple2Output[+A, +B](_1: Output[A], _2: Output[B]) extends Output[Chunk[(A, B)]] {
    protected def tryDecode(respValue: RespValue): Chunk[(A, B)] =
      respValue match {
        case RespValue.NullBulkString => Chunk.empty
        case RespValue.NullArray      => Chunk.empty
        case RespValue.Array(values) if values.length % 2 == 0 =>
          Chunk.fromIterator(values.grouped(2).map(g => _1.tryDecode(g(0)) -> _2.tryDecode(g(1))))
        case array @ RespValue.Array(_) =>
          throw ProtocolError(s"$array doesn't have an even number of elements")
        case other =>
          throw ProtocolError(s"$other isn't an array")
      }
  }

  case object XPendingOutput extends Output[PendingInfo] {
    protected def tryDecode(respValue: RespValue): PendingInfo =
      respValue match {
        case RespValue.Array(Seq(RespValue.Integer(total), f, l, ps)) =>
          val first = OptionalOutput(MultiStringOutput).unsafeDecode(f)
          val last  = OptionalOutput(MultiStringOutput).unsafeDecode(l)

          val pairs = ps match {
            case RespValue.NullArray    => Chunk.empty
            case RespValue.Array(value) => value
            case other                  => throw ProtocolError(s"$other isn't an array")
          }

          val consumers = collection.mutable.Map.empty[String, Long]

          pairs.foreach {
            case RespValue.Array(Seq(consumer @ RespValue.BulkString(_), total @ RespValue.BulkString(_))) =>
              consumers += (consumer.asString -> total.asLong)
            case _                                                                                         =>
              throw ProtocolError(s"Consumers doesn't have 2 elements")
          }

          PendingInfo(total, first, last, consumers.toMap)

        case array @ RespValue.Array(_) =>
          throw ProtocolError(s"$array doesn't have valid format")

        case other =>
          throw ProtocolError(s"$other isn't an array")
      }
  }

  private[redis] case object PushMessageOutput extends Output[PushMessage] {
    protected def tryDecode(respValue: RespValue): PushMessage =
      respValue match {
        case RespValue.NullArray     => throw ProtocolError(s"Array must not be empty")
        case RespValue.Array(values) =>
          val name = MultiStringOutput.unsafeDecode(values(0))
          val key  = MultiStringOutput.unsafeDecode(values(1))
          name match {
            case "subscribe"    =>
              val num = LongOutput.unsafeDecode(values(2))
              PushMessage.Subscribed(SubscriptionKey.Channel(key), num)
            case "psubscribe"   =>
              val num = LongOutput.unsafeDecode(values(2))
              PushMessage.Subscribed(SubscriptionKey.Pattern(key), num)
            case "unsubscribe"  =>
              val num = LongOutput.unsafeDecode(values(2))
              PushMessage.Unsubscribed(SubscriptionKey.Channel(key), num)
            case "punsubscribe" =>
              val num = LongOutput.unsafeDecode(values(2))
              PushMessage.Unsubscribed(SubscriptionKey.Pattern(key), num)
            case "message"      =>
              val message = values(2)
              PushMessage.Message(SubscriptionKey.Channel(key), key, message)
            case "pmessage"     =>
              val channel = MultiStringOutput.unsafeDecode(values(2))
              val message = values(3)
              PushMessage.Message(SubscriptionKey.Pattern(key), channel, message)
            case other          => throw ProtocolError(s"$other isn't a pushed message")
          }
        case other                   => throw ProtocolError(s"$other isn't an array")
      }
  }

  case object NumSubResponseOutput extends Output[Map[String, Long]] {
    protected def tryDecode(respValue: RespValue): Map[String, Long] =
      respValue match {
        case RespValue.Array(values) =>
          val builder = Map.newBuilder[String, Long]
          values.grouped(2).foreach { chunk =>
            val channel   = MultiStringOutput.unsafeDecode(chunk(0))
            val numOfSubs = LongOutput.unsafeDecode(chunk(1))
            builder += channel -> numOfSubs
          }
          builder.result()
        case other                   => throw ProtocolError(s"$other isn't an array")
      }
  }

  private def decodeDouble(bytes: Chunk[Byte], withInfinity: Boolean = false): Double = {
    val text = new String(bytes.toArray, StandardCharsets.UTF_8)
    text match {
      case "inf" if withInfinity  => Double.PositiveInfinity
      case "-inf" if withInfinity => Double.NegativeInfinity
      case _                      =>
        try text.toDouble
        catch {
          case _: NumberFormatException =>
            throw ProtocolError(
              if (withInfinity) s"'$text' isn't a double or an infinity." else s"'$text' isn't a double."
            )
        }
    }
  }

  private def extractXInfoFullGroup(group: RespValue): StreamInfoWithFull.ConsumerGroups = {
    var readyGroup = StreamInfoWithFull.ConsumerGroups.empty
    group match {
      case RespValue.Array(groupElements) if groupElements.length % 2 == 0 =>
        var groupElementPos = 0
        while (groupElementPos < groupElements.length) {
          (groupElements(groupElementPos), groupElements(groupElementPos + 1)) match {
            // Get the basic information of the current group.
            case (key @ RespValue.BulkString(_), value @ RespValue.BulkString(_)) =>
              key.asString match {
                case XInfoFields.LastDeliveredId => readyGroup = readyGroup.copy(lastDeliveredId = value.asString)
                case XInfoFields.Name            => readyGroup = readyGroup.copy(name = value.asString)
                case _                           =>
              }
            case (key @ RespValue.BulkString(_), value @ RespValue.Integer(_))    =>
              key.asString match {
                case XInfoFields.PelCount    => readyGroup = readyGroup.copy(pelCount = value.value)
                case XInfoFields.Lag         => readyGroup = readyGroup.copy(lag = value.value)
                case XInfoFields.EntriesRead => readyGroup = readyGroup.copy(entriesRead = value.value)
                case _                       =>
              }
            case (key @ RespValue.BulkString(_), RespValue.Array(values))         =>
              // Get the consumer list of the current group.
              key.asString match {
                case XInfoFields.Consumers =>
                  readyGroup = readyGroup.copy(consumers = values.map(extractXInfoFullConsumer))
                case XInfoFields.Pending   =>
                  // Get the pel list of the current group.
                  val groupPelList = values.map {
                    case RespValue.Array(
                          Seq(
                            entryId @ RespValue.BulkString(_),
                            consumerName @ RespValue.BulkString(_),
                            deliveryTime @ RespValue.Integer(_),
                            deliveryCount @ RespValue.Integer(_)
                          )
                        ) =>
                      StreamInfoWithFull.GroupPel(
                        entryId.asString,
                        consumerName.asString,
                        deliveryTime.value.millis,
                        deliveryCount.value
                      )
                    case other => throw ProtocolError(s"$other isn't a valid array")
                  }
                  readyGroup = readyGroup.copy(pending = groupPelList)
                case _                     =>
              }
            case _                                                                =>
          }
          groupElementPos += 2
        }
        readyGroup
      case array @ RespValue.Array(_) =>
        throw ProtocolError(s"$array doesn't have an even number of elements")
      case other =>
        throw ProtocolError(s"$other isn't an array")
    }
  }

  private def extractXInfoFullConsumer(consumer: RespValue): StreamInfoWithFull.Consumers = {
    var readyConsumer = StreamInfoWithFull.Consumers.empty
    consumer match {
      case RespValue.Array(consumerElements) if consumerElements.length % 2 == 0 =>
        var consumerElementPos = 0
        while (consumerElementPos < consumerElements.length) {
          (consumerElements(consumerElementPos), consumerElements(consumerElementPos + 1)) match {
            // Get the basic information of the current consumer.
            case (key @ RespValue.BulkString(_), value @ RespValue.BulkString(_)) if key.asString == XInfoFields.Name =>
              readyConsumer = readyConsumer.copy(name = value.asString)
            case (key @ RespValue.BulkString(_), value @ RespValue.Integer(_))                                        =>
              key.asString match {
                case XInfoFields.PelCount   => readyConsumer = readyConsumer.copy(pelCount = value.value)
                case XInfoFields.SeenTime   => readyConsumer = readyConsumer.copy(seenTime = value.value.millis)
                case XInfoFields.ActiveTime => readyConsumer = readyConsumer.copy(activeTime = value.value.millis)
                case _                      =>
              }
            // Get the pel list of the current consumer.
            case (key @ RespValue.BulkString(_), RespValue.Array(values)) if key.asString == XInfoFields.Pending      =>
              val consumerPelList = values.map {
                case RespValue.Array(
                      Seq(
                        entryId @ RespValue.BulkString(_),
                        deliveryTime @ RespValue.Integer(_),
                        deliveryCount @ RespValue.Integer(_)
                      )
                    ) =>
                  StreamInfoWithFull.ConsumerPel(entryId.asString, deliveryTime.value.millis, deliveryCount.value)
                case other => throw ProtocolError(s"$other isn't a valid array")
              }
              readyConsumer = readyConsumer.copy(pending = consumerPelList)
            case _                                                                                                    =>
          }
          consumerElementPos += 2
        }
        readyConsumer
      case array @ RespValue.Array(_) =>
        throw ProtocolError(s"$array doesn't have an even number of elements")
      case other =>
        throw ProtocolError(s"$other isn't an array")
    }
  }
}
