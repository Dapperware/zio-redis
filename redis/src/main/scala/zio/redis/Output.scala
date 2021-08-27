package zio.redis

import java.net.InetAddress

import zio.Chunk
import zio.duration._
import zio.schema.Schema
import zio.schema.codec.Codec

sealed trait Output[+A] {
  self =>

  private[redis] final def unsafeDecode(respValue: RespValue)(implicit codec: Codec): A =
    respValue match {
      case RespValue.Error(msg) if msg.startsWith("ERR") =>
        throw RedisError.ProtocolError(msg.drop(3).trim)
      case RespValue.Error(msg) if msg.startsWith("WRONGTYPE") =>
        throw RedisError.WrongType(msg.drop(9).trim)
      case RespValue.Error(msg) if msg.startsWith("BUSYGROUP") =>
        throw RedisError.BusyGroup(msg.drop(9).trim)
      case RespValue.Error(msg) if msg.startsWith("NOGROUP") =>
        throw RedisError.NoGroup(msg.drop(7).trim)
      case RespValue.Error(msg) =>
        throw RedisError.ProtocolError(msg.trim)
      case success =>
        tryDecode(success)
    }

  protected def tryDecode(respValue: RespValue)(implicit codec: Codec): A

  final def map[B](f: A => B): Output[B] =
    new Output[B] {
      protected def tryDecode(respValue: RespValue)(implicit codec: Codec): B = f(self.tryDecode(respValue))
    }

}

object Output {

  import RedisError._

  case object BoolOutput extends Output[Boolean] {
    protected def tryDecode(respValue: RespValue)(implicit codec: Codec): Boolean =
      respValue match {
        case RespValue.Integer(0) => false
        case RespValue.Integer(1) => true
        case other                => throw ProtocolError(s"$other isn't a boolean")
      }
  }

  final case class ChunkOutput[+A](output: Output[A]) extends Output[Chunk[A]] {
    protected def tryDecode(respValue: RespValue)(implicit codec: Codec): Chunk[A] =
      respValue match {
        case RespValue.NullArray     => Chunk.empty
        case RespValue.Array(values) => values.map(output.tryDecode)
        case other                   => throw ProtocolError(s"$other isn't an array")
      }
  }

  final case class ZRandMemberOutput[+A](output: Output[A]) extends Output[Chunk[A]] {
    protected def tryDecode(respValue: RespValue)(implicit codec: Codec): Chunk[A] =
      respValue match {
        case RespValue.NullBulkString => Chunk.empty
        case RespValue.NullArray      => Chunk.empty
        case RespValue.Array(values)  => values.map(output.tryDecode)
        case other                    => throw ProtocolError(s"$other isn't an array")
      }
  }

  final case class ChunkTuple2Output[+A, +B](_1: Output[A], _2: Output[B]) extends Output[Chunk[(A, B)]] {
    protected def tryDecode(respValue: RespValue)(implicit codec: Codec): Chunk[(A, B)] =
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

  final case class ZRandMemberTuple2Output[+A, +B](_1: Output[A], _2: Output[B]) extends Output[Chunk[(A, B)]] {
    protected def tryDecode(respValue: RespValue)(implicit codec: Codec): Chunk[(A, B)] =
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

  case object DoubleOutput extends Output[Double] {
    protected def tryDecode(respValue: RespValue)(implicit codec: Codec): Double =
      respValue match {
        case RespValue.BulkString(bytes) => decodeDouble(bytes)
        case other                       => throw ProtocolError(s"$other isn't a double.")
      }
  }

  private object DurationOutput extends Output[Long] {
    protected def tryDecode(respValue: RespValue)(implicit codec: Codec): Long =
      respValue match {
        case RespValue.Integer(-2L) => throw ProtocolError("Key not found.")
        case RespValue.Integer(-1L) => throw ProtocolError("Key has no expire.")
        case RespValue.Integer(n)   => n
        case other                  => throw ProtocolError(s"$other isn't a duration.")
      }
  }

  final val DurationMillisecondsOutput: Output[Duration] = DurationOutput.map(_.milliseconds)

  final val DurationSecondsOutput: Output[Duration] = DurationOutput.map(_.seconds)

  case object LongOutput extends Output[Long] {
    protected def tryDecode(respValue: RespValue)(implicit codec: Codec): Long =
      respValue match {
        case RespValue.Integer(v) => v
        case other                => throw ProtocolError(s"$other isn't an integer")
      }
  }

  final case class OptionalOutput[+A](output: Output[A]) extends Output[Option[A]] {
    protected def tryDecode(respValue: RespValue)(implicit codec: Codec): Option[A] =
      respValue match {
        case RespValue.NullBulkString | RespValue.NullArray | RespValue.BulkString(Chunk.empty) => None
        case other                                                                              => Some(output.tryDecode(other))
      }
  }

  final case class ScanOutput[+A](output: Output[A]) extends Output[(Long, Chunk[A])] {
    protected def tryDecode(respValue: RespValue)(implicit codec: Codec): (Long, Chunk[A]) =
      respValue match {
        case RespValue.ArrayValues(cursor @ RespValue.BulkString(_), RespValue.Array(items)) =>
          (cursor.asLong, items.map(output.tryDecode))
        case other =>
          throw ProtocolError(s"$other isn't scan output")
      }
  }

  case object KeyElemOutput extends Output[Option[(String, String)]] {
    protected def tryDecode(respValue: RespValue)(implicit codec: Codec): Option[(String, String)] =
      respValue match {
        case RespValue.NullArray =>
          None
        case RespValue.ArrayValues(a @ RespValue.BulkString(_), b @ RespValue.BulkString(_)) =>
          Some((a.asString, b.asString))
        case other => throw ProtocolError(s"$other isn't blPop output")
      }
  }

  case object StringOutput extends Output[String] {
    protected def tryDecode(respValue: RespValue)(implicit codec: Codec): String =
      respValue match {
        case RespValue.SimpleString(s) => s
        case other                     => throw ProtocolError(s"$other isn't a simple string")
      }
  }

  case object MultiStringOutput extends Output[String] {
    protected def tryDecode(respValue: RespValue)(implicit codec: Codec): String =
      respValue match {
        case s @ RespValue.BulkString(_) => s.asString
        case other                       => throw ProtocolError(s"$other isn't a bulk string")
      }
  }

  case object BulkStringOutput extends Output[Chunk[Byte]] {
    protected def tryDecode(respValue: RespValue)(implicit codec: Codec): Chunk[Byte] =
      respValue match {
        case RespValue.BulkString(value) => value
        case other                       => throw ProtocolError(s"$other isn't a bulk string")
      }
  }

  final case class ArbitraryOutput[A]()(implicit schema: Schema[A]) extends Output[A] {
    protected def tryDecode(respValue: RespValue)(implicit codec: Codec): A =
      respValue match {
        case RespValue.BulkString(s) => codec.decode(schema)(s).fold(e => throw CodecError(e), identity)
        case other                   => throw ProtocolError(s"$other isn't a bulk string")
      }
  }

  final case class Tuple2Output[+A, +B](_1: Output[A], _2: Output[B]) extends Output[(A, B)] {
    protected def tryDecode(respValue: RespValue)(implicit codec: Codec): (A, B) =
      respValue match {
        case RespValue.ArrayValues(a: RespValue, b: RespValue) => (_1.tryDecode(a), _2.tryDecode(b))
        case other                                             => throw ProtocolError(s"$other isn't a tuple2")
      }
  }

  final case class Tuple3Output[+A, +B, +C](_1: Output[A], _2: Output[B], _3: Output[C]) extends Output[(A, B, C)] {
    protected def tryDecode(respValue: RespValue)(implicit codec: Codec): (A, B, C) =
      respValue match {
        case RespValue.ArrayValues(a: RespValue, b: RespValue, c: RespValue) =>
          (_1.tryDecode(a), _2.tryDecode(b), _3.tryDecode(c))
        case other => throw ProtocolError(s"$other isn't a tuple3")
      }
  }

  case object SingleOrMultiStringOutput extends Output[String] {
    protected def tryDecode(respValue: RespValue)(implicit codec: Codec): String =
      respValue match {
        case RespValue.SimpleString(s)   => s
        case s @ RespValue.BulkString(_) => s.asString
        case other                       => throw ProtocolError(s"$other isn't a bulk string")
      }
  }

  final case class MultiStringChunkOutput[+A](output: Output[A]) extends Output[Chunk[A]] {
    protected def tryDecode(respValue: RespValue)(implicit codec: Codec): Chunk[A] =
      respValue match {
        case RespValue.NullBulkString    => Chunk.empty
        case s @ RespValue.BulkString(_) => Chunk.single(output.tryDecode(s))
        case RespValue.Array(elements)   => elements.map(output.tryDecode)
        case other                       => throw ProtocolError(s"$other isn't a string nor an array")
      }
  }

  case object TypeOutput extends Output[RedisType] {
    protected def tryDecode(respValue: RespValue)(implicit codec: Codec): RedisType =
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
    protected def tryDecode(respValue: RespValue)(implicit codec: Codec): Unit =
      respValue match {
        case RespValue.SimpleString("OK") => ()
        case other                        => throw ProtocolError(s"$other isn't unit.")
      }
  }

  case object ResetOutput extends Output[Unit] {
    protected def tryDecode(respValue: RespValue)(implicit codec: Codec): Unit =
      respValue match {
        case RespValue.SimpleString("RESET") => ()
        case other                           => throw ProtocolError(s"$other isn't unit.")
      }
  }

  case object GeoOutput extends Output[Chunk[Option[LongLat]]] {
    protected def tryDecode(respValue: RespValue)(implicit codec: Codec): Chunk[Option[LongLat]] =
      respValue match {
        case RespValue.NullArray =>
          Chunk.empty
        case RespValue.Array(elements) =>
          elements.map {
            case RespValue.NullArray => None
            case RespValue.ArrayValues(RespValue.BulkString(long), RespValue.BulkString(lat)) =>
              Some(LongLat(decodeDouble(long), decodeDouble(lat)))
            case other =>
              throw ProtocolError(s"$other was not a longitude,latitude pair")
          }
        case other =>
          throw ProtocolError(s"$other isn't geo output")
      }
  }

  case object GeoRadiusOutput extends Output[Chunk[GeoView]] {
    protected def tryDecode(respValue: RespValue)(implicit codec: Codec): Chunk[GeoView] =
      respValue match {
        case RespValue.Array(elements) =>
          elements.map {
            case s @ RespValue.BulkString(_) =>
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

  final case class KeyValueOutput[K, V](outK: Output[K], outV: Output[V]) extends Output[Map[K, V]] {
    protected def tryDecode(respValue: RespValue)(implicit codec: Codec): Map[K, V] =
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

  final case class KeyValueTwoOutput[K, V](outK: Output[K], outV: Output[V]) extends Output[Map[K, V]] {
    protected def tryDecode(respValue: RespValue)(implicit codec: Codec): Map[K, V] =
      respValue match {
        case RespValue.NullArray => Map.empty[K, V]
        case RespValue.Array(entities) =>
          entities.map {
            case RespValue.Array(Seq(key @ RespValue.BulkString(_), value)) =>
              outK.unsafeDecode(key) -> outV.unsafeDecode(value)
            case other =>
              throw ProtocolError(s"$other isn't a valid array")
          }.toMap
        case other => throw ProtocolError(s"$other isn't an array")
      }
  }

  final case class StreamOutput[I, K, V]()(implicit
    idSchema: Schema[I],
    keySchema: Schema[K],
    valueSchema: Schema[V]
  ) extends Output[Map[I, Map[K, V]]] {
    protected def tryDecode(respValue: RespValue)(implicit codec: Codec): Map[I, Map[K, V]] =
      KeyValueTwoOutput(ArbitraryOutput[I](), KeyValueOutput(ArbitraryOutput[K](), ArbitraryOutput[V]()))
        .unsafeDecode(respValue)
  }

  case object StreamGroupsInfoOutput extends Output[Chunk[StreamGroupsInfo]] {
    override protected def tryDecode(respValue: RespValue)(implicit codec: Codec): Chunk[StreamGroupsInfo] =
      respValue match {
        case RespValue.NullArray => Chunk.empty
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
                  case (key @ RespValue.BulkString(_), value @ RespValue.Integer(_)) =>
                    if (key.asString == XInfoFields.Pending)
                      streamGroupsInfo = streamGroupsInfo.copy(pending = value.value)
                    else if (key.asString == XInfoFields.Consumers)
                      streamGroupsInfo = streamGroupsInfo.copy(consumers = value.value)
                  case _ =>
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

  case object StreamConsumersInfoOutput extends Output[Chunk[StreamConsumersInfo]] {
    override protected def tryDecode(respValue: RespValue)(implicit codec: Codec): Chunk[StreamConsumersInfo] =
      respValue match {
        case RespValue.NullArray => Chunk.empty
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
                  case _ =>
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

  final case class StreamInfoFullOutput[I: Schema, K: Schema, V: Schema]()
      extends Output[StreamInfoWithFull.FullStreamInfo[I, K, V]] {
    override protected def tryDecode(
      respValue: RespValue
    )(implicit codec: Codec): StreamInfoWithFull.FullStreamInfo[I, K, V] = {
      var streamInfoFull: StreamInfoWithFull.FullStreamInfo[I, K, V] = StreamInfoWithFull.FullStreamInfo.empty
      respValue match {
        // Note that you should not rely on the fields exact position. see https://redis.io/commands/xinfo
        case RespValue.Array(elements) if elements.length % 2 == 0 =>
          var pos = 0
          while (pos < elements.length) {
            (elements(pos), elements(pos + 1)) match {
              // Get the basic information of the outermost stream.
              case (key @ RespValue.BulkString(_), value @ RespValue.Integer(_)) =>
                key.asString match {
                  case XInfoFields.Length         => streamInfoFull = streamInfoFull.copy(length = value.value)
                  case XInfoFields.RadixTreeNodes => streamInfoFull = streamInfoFull.copy(radixTreeNodes = value.value)
                  case XInfoFields.RadixTreeKeys  => streamInfoFull = streamInfoFull.copy(radixTreeKeys = value.value)
                  case _                          =>
                }
              case (key @ RespValue.BulkString(_), value @ RespValue.BulkString(_))
                  if key.asString == XInfoFields.LastGeneratedId =>
                streamInfoFull = streamInfoFull.copy(lastGeneratedId = value.asString)
              case (key @ RespValue.BulkString(_), RespValue.Array(values)) if key.asString == XInfoFields.Entries =>
                val streamEntryList = values.map(extractStreamEntry[I, K, V])
                streamInfoFull = streamInfoFull.copy(entries = streamEntryList)
              case (key @ RespValue.BulkString(_), RespValue.Array(values)) if key.asString == XInfoFields.Groups =>
                // Get the group list of the stream.
                streamInfoFull = streamInfoFull.copy(groups = values.map(extractXInfoFullGroup))
              case _ =>
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
            case (key @ RespValue.BulkString(_), value @ RespValue.Integer(_))
                if XInfoFields.PelCount == key.asString =>
              readyGroup = readyGroup.copy(pelCount = value.value)
            case (key @ RespValue.BulkString(_), RespValue.Array(values)) =>
              // Get the consumer list of the current group.
              key.asString match {
                case XInfoFields.Consumers =>
                  readyGroup = readyGroup.copy(consumers = values.map(extractXInfoFullConsumer))
                case XInfoFields.Pending =>
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
                case _ =>
              }
            case _ =>
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
            case (key @ RespValue.BulkString(_), value @ RespValue.Integer(_)) =>
              key.asString match {
                case XInfoFields.PelCount => readyConsumer = readyConsumer.copy(pelCount = value.value)
                case XInfoFields.SeenTime => readyConsumer = readyConsumer.copy(seenTime = value.value.millis)
                case _                    =>
              }
            // Get the pel list of the current consumer.
            case (key @ RespValue.BulkString(_), RespValue.Array(values)) if key.asString == XInfoFields.Pending =>
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
            case _ =>
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

  final case class StreamInfoOutput[I: Schema, K: Schema, V: Schema]() extends Output[StreamInfo[I, K, V]] {
    override protected def tryDecode(respValue: RespValue)(implicit codec: Codec): StreamInfo[I, K, V] = {
      var streamInfo: StreamInfo[I, K, V] = StreamInfo.empty
      respValue match {
        // Note that you should not rely on the fields exact position. see https://redis.io/commands/xinfo
        case RespValue.Array(elements) if elements.length % 2 == 0 =>
          val len = elements.length
          var pos = 0
          while (pos < len) {
            (elements(pos), elements(pos + 1)) match {
              case (key @ RespValue.BulkString(_), value @ RespValue.Integer(_)) =>
                if (key.asString == XInfoFields.Length)
                  streamInfo = streamInfo.copy(length = value.value)
                else if (key.asString == XInfoFields.RadixTreeNodes)
                  streamInfo = streamInfo.copy(radixTreeNodes = value.value)
                else if (key.asString == XInfoFields.RadixTreeKeys)
                  streamInfo = streamInfo.copy(radixTreeKeys = value.value)
                else if (key.asString == XInfoFields.Groups)
                  streamInfo = streamInfo.copy(groups = value.value)
              case (key @ RespValue.BulkString(_), value @ RespValue.BulkString(_))
                  if key.asString == XInfoFields.LastGeneratedId =>
                streamInfo = streamInfo.copy(lastGeneratedId = value.asString)
              case (key @ RespValue.BulkString(_), value @ RespValue.Array(_)) =>
                if (key.asString == XInfoFields.FirstEntry)
                  streamInfo = streamInfo.copy(firstEntry = Some(extractStreamEntry(value)))
                else if (key.asString == XInfoFields.LastEntry)
                  streamInfo = streamInfo.copy(lastEntry = Some(extractStreamEntry(value)))
              case _ =>
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

  private def extractStreamEntry[I: Schema, K: Schema, V: Schema](
    es: RespValue
  )(implicit codec: Codec): StreamEntry[I, K, V] =
    es match {
      case RespValue.Array(Seq(id @ RespValue.BulkString(_), value)) =>
        val eId   = ArbitraryOutput[I]().unsafeDecode(id)
        val entry = KeyValueOutput(ArbitraryOutput[K](), ArbitraryOutput[V]()).unsafeDecode(value)
        StreamEntry(id = eId, entry)
      case other =>
        throw ProtocolError(s"$other isn't a valid array")
    }

  case object XPendingOutput extends Output[PendingInfo] {
    protected def tryDecode(respValue: RespValue)(implicit codec: Codec): PendingInfo =
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
            case _ =>
              throw ProtocolError(s"Consumers doesn't have 2 elements")
          }

          PendingInfo(total, first, last, consumers.toMap)

        case array @ RespValue.Array(_) =>
          throw ProtocolError(s"$array doesn't have valid format")

        case other =>
          throw ProtocolError(s"$other isn't an array")
      }
  }

  case object PendingMessagesOutput extends Output[Chunk[PendingMessage]] {
    protected def tryDecode(respValue: RespValue)(implicit codec: Codec): Chunk[PendingMessage] =
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

  case object SetOutput extends Output[Boolean] {
    protected def tryDecode(respValue: RespValue)(implicit codec: Codec): Boolean =
      respValue match {
        case RespValue.NullBulkString  => false
        case RespValue.SimpleString(_) => true
        case other                     => throw ProtocolError(s"$other isn't a valid set response")
      }
  }

  case object StrAlgoLcsOutput extends Output[LcsOutput] {
    protected def tryDecode(respValue: RespValue)(implicit codec: Codec): LcsOutput =
      respValue match {
        case result @ RespValue.BulkString(_) => LcsOutput.Lcs(result.asString)
        case RespValue.Integer(length)        => LcsOutput.Length(length)
        case RespValue.ArrayValues(
              RespValue.BulkString(_),
              RespValue.Array(items),
              RespValue.BulkString(_),
              RespValue.Integer(length)
            ) =>
          val matches = items.map {
            case RespValue.Array(array) =>
              val matchIdxs = array.collect { case RespValue.Array(values) =>
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
            case other => throw ProtocolError(s"$other isn't a valid response")
          }
          LcsOutput.Matches(matches.toList, length)
        case other => throw ProtocolError(s"$other isn't a valid set response")
      }
  }

  private def decodeDouble(bytes: Chunk[Byte]): Double = {
    val text = RespValue.decode(bytes)
    try text.toDouble
    catch {
      case _: NumberFormatException => throw ProtocolError(s"'$text' isn't a double.")
    }
  }

  case object ClientInfoOutput extends Output[Chunk[ClientInfo]] {
    private def parseLong(s: String): Option[Long] =
      try Some(s.toLong)
      catch { case _: NumberFormatException => None }

    private def parseInt(s: String): Option[Int] =
      try Some(s.toInt)
      catch { case _: NumberFormatException => None }

    protected def tryDecode(respValue: RespValue)(implicit codec: Codec): Chunk[ClientInfo] =
      respValue match {
        case bulk @ RespValue.BulkString(_) =>
          val clients: List[Map[String, String]] = bulk.asString.split('\n').toList.map {
            _.split(' ').toList.map {
              _.split('=').toList match {
                case key :: value :: Nil => key -> value
                case other               => throw ProtocolError(s"Invalid text $other in client information")
              }
            }.toMap
          }
          Chunk.fromIterable(clients).map { client =>
            val flags: Set[ClientFlag] = client
              .get("flags")
              .map { s =>
                import ClientFlag._
                s.collect {
                  case 'A' => ToBeClosedAsap
                  case 'b' => Blocked
                  case 'c' => ToBeClosedAfterReply
                  case 'd' => WatchedKeysModified
                  case 'M' => IsMaster
                  case 'O' => MonitorMode
                  case 'P' => PubSub
                  case 'r' => ReadOnlyMode
                  case 'S' => Replica
                  case 'u' => Unblocked
                  case 'U' => UnixDomainSocket
                  case 'x' => MultiExecContext
                  case 't' => KeysTrackingEnabled
                  case 'R' => TrackingTargetClientInvalid
                  case 'B' => BroadcastTrackingMode
                }.toSet[ClientFlag]
              }
              .getOrElse(Set.empty)
            val events =
              client
                .get("events")
                .map(s => ClientEvents(readable = s.contains('r'), writable = s.contains('w')))
                .getOrElse(ClientEvents())
            ClientInfo(
              id = client.get("id").flatMap(parseLong).getOrElse(0L),
              name = client.get("name"),
              address = client.get("addr").map(InetAddress.getByName),
              localAddress = client.get("laddr").map(InetAddress.getByName),
              fileDescriptor = client.get("fd").flatMap(parseLong),
              age = client.get("age").flatMap(parseLong).map(_.seconds),
              idle = client.get("idle").flatMap(parseLong).map(_.seconds),
              flags = flags,
              databaseId = client.get("db").flatMap(parseLong),
              subscriptions = client.get("sub").flatMap(parseInt).getOrElse(0),
              patternSubscriptions = client.get("psub").flatMap(parseInt).getOrElse(0),
              multiCommands = client.get("multi").flatMap(parseInt).getOrElse(0),
              queryBufferLength = client.get("qbuf").flatMap(parseInt),
              queryBufferFree = client.get("qbuf-free").flatMap(parseInt),
              outputBufferLength = client.get("obl").flatMap(parseInt),
              outputListLength = client.get("oll").flatMap(parseInt),
              outputBufferMem = client.get("omem").flatMap(parseLong),
              events = events,
              lastCommand = client.get("cmd"),
              argvMemory = client.get("argv-mem").flatMap(parseLong),
              totalMemory = client.get("tot-mem").flatMap(parseLong),
              redirectionClientId = client.get("redir").flatMap(parseLong),
              user = client.get("user")
            )
          }
        case other => throw ProtocolError(s"$other isn't a bulk string")
      }
  }

  case object ClientTrackingInfoOutput extends Output[ClientTrackingInfo] {
    protected def tryDecode(respValue: RespValue)(implicit codec: Codec): ClientTrackingInfo =
      respValue match {
        case RespValue.NullArray => throw ProtocolError(s"Array must not be empty")
        case RespValue.Array(values) if values.length % 2 == 0 =>
          val fields = values.toList
            .grouped(2)
            .map {
              case (bulk @ RespValue.BulkString(_)) :: value :: Nil => (bulk.asString, value)
              case other                                            => throw ProtocolError(s"$other isn't a valid format")
            }
            .toMap
          ClientTrackingInfo(
            fields
              .get("flags")
              .map {
                case RespValue.Array(value) =>
                  val set = value.map {
                    case bulk @ RespValue.BulkString(_) => bulk.asString
                    case other                          => throw ProtocolError(s"$other isn't a string")
                  }.toSet
                  ClientTrackingFlags(
                    set.contains("on"),
                    set match {
                      case s if s.contains("optin")  => Some(ClientTrackingMode.OptIn)
                      case s if s.contains("optout") => Some(ClientTrackingMode.OptOut)
                      case s if s.contains("bcast")  => Some(ClientTrackingMode.Broadcast)
                      case _                         => None
                    },
                    set.contains("noloop"),
                    set match {
                      case s if s.contains("caching-yes") => Some(true)
                      case s if s.contains("caching-no")  => Some(false)
                      case _                              => None
                    },
                    set.contains("broken_redirect")
                  )
                case other => throw ProtocolError(s"$other isn't an array with elements")
              }
              .getOrElse(throw ProtocolError("Missing flags field")),
            fields
              .get("redirect")
              .map {
                case RespValue.Integer(-1L)         => None
                case RespValue.Integer(v) if v > 0L => Some(v)
                case RespValue.Integer(v)           => throw ProtocolError(s"$v isn't a valid integer")
                case other                          => throw ProtocolError(s"$other isn't an integer")
              }
              .getOrElse(throw ProtocolError("Missing redirect field")),
            fields
              .get("prefixes")
              .map {
                case RespValue.NullArray => Set.empty[String]
                case RespValue.Array(value) =>
                  value.map {
                    case bulk @ RespValue.BulkString(_) => bulk.asString
                    case other                          => throw ProtocolError(s"$other isn't a string")
                  }.toSet[String]
                case other => throw ProtocolError(s"$other isn't an array")
              }
              .getOrElse(throw ProtocolError("Missing prefixes field"))
          )
        case array @ RespValue.Array(_) => throw ProtocolError(s"$array doesn't have an even number of elements")
        case other                      => throw ProtocolError(s"$other isn't an array")
      }
  }

  case object ClientTrackingRedirectOutput extends Output[ClientTrackingRedirect] {
    protected def tryDecode(respValue: RespValue)(implicit codec: Codec): ClientTrackingRedirect =
      respValue match {
        case RespValue.Integer(-1L)         => ClientTrackingRedirect.NotEnabled
        case RespValue.Integer(0L)          => ClientTrackingRedirect.NotRedirected
        case RespValue.Integer(v) if v > 0L => ClientTrackingRedirect.RedirectedTo(v)
        case other                          => throw ProtocolError(s"$other isn't an integer >= -1")
      }
  }
}
