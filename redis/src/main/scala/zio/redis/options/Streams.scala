package zio.redis.options

import zio.Chunk
import zio.duration._

trait Streams {

  case object WithForce {
    private[redis] def stringify = "FORCE"
  }

  type WithForce = WithForce.type

  case object WithJustId {
    private[redis] def stringify = "JUSTID"
  }

  type WithJustId = WithJustId.type

  sealed trait XGroupCommand

  object XGroupCommand {

    case class Create(key: String, group: String, id: String, mkStream: Boolean) extends XGroupCommand

    case class SetId(key: String, group: String, id: String) extends XGroupCommand

    case class Destroy(key: String, group: String) extends XGroupCommand

    case class CreateConsumer(key: String, group: String, consumer: String) extends XGroupCommand

    case class DelConsumer(key: String, group: String, consumer: String) extends XGroupCommand

  }

  sealed trait XInfoCommand

  object XInfoCommand {

    case class Stream(key: String, full: Option[Full] = None) extends XInfoCommand

    case class Full(count: Option[Long])

    case class Groups(key: String) extends XInfoCommand

    case class Consumers(key: String, group: String) extends XInfoCommand

  }

  case object MkStream {
    private[redis] def stringify = "MKSTREAM"
  }

  type MkStream = MkStream.type

  case class PendingInfo(
    total: Long,
    first: Option[String],
    last: Option[String],
    consumers: Map[String, Long]
  )

  case class PendingMessage(
    id: String,
    owner: String,
    lastDelivered: Duration,
    counter: Long
  )

  case class Group(group: String, consumer: String)

  case object NoAck {
    private[redis] def stringify: String = "NOACK"
  }

  type NoAck = NoAck.type

  case class MaxLen(approximate: Boolean, count: Long)

  case class StreamEntry(id: String, fields: Map[String, String])

  case class StreamInfo(
    length: Long,
    radixTreeKeys: Long,
    radixTreeNodes: Long,
    groups: Long,
    lastGeneratedId: String,
    firstEntry: Option[StreamEntry],
    lastEntry: Option[StreamEntry]
  )

  object StreamInfo {
    def empty: StreamInfo = StreamInfo(0, 0, 0, 0, "", None, None)
  }

  case class StreamGroupsInfo(
    name: String,
    consumers: Long,
    pending: Long,
    lastDeliveredId: String
  )

  object StreamGroupsInfo {
    def empty: StreamGroupsInfo = StreamGroupsInfo("", 0, 0, "")
  }

  case class StreamConsumersInfo(
    name: String,
    pending: Long,
    idle: Duration
  )

  object StreamConsumersInfo {
    def empty: StreamConsumersInfo = StreamConsumersInfo("", 0, 0.millis)
  }

  object StreamInfoWithFull {

    case class FullStreamInfo(
      length: Long,
      radixTreeKeys: Long,
      radixTreeNodes: Long,
      lastGeneratedId: String,
      entries: Chunk[StreamEntry],
      groups: Chunk[ConsumerGroups]
    )

    object FullStreamInfo {
      def empty: FullStreamInfo = FullStreamInfo(0, 0, 0, "", Chunk.empty, Chunk.empty)
    }

    case class ConsumerGroups(
      name: String,
      lastDeliveredId: String,
      pelCount: Long,
      pending: Chunk[GroupPel],
      consumers: Chunk[Consumers]
    )

    object ConsumerGroups {
      def empty: ConsumerGroups = ConsumerGroups("", "", 0, Chunk.empty, Chunk.empty)
    }

    case class GroupPel(entryId: String, consumerName: String, deliveryTime: Duration, deliveryCount: Long)

    case class Consumers(name: String, seenTime: Duration, pelCount: Long, pending: Chunk[ConsumerPel])

    object Consumers {
      def empty: Consumers = Consumers("", 0.millis, 0, Chunk.empty)
    }

    case class ConsumerPel(entryId: String, deliveryTime: Duration, deliveryCount: Long)

  }

  private[redis] object XInfoFields {
    val Name: String    = "name"
    val Idle: String    = "idle"
    val Pending: String = "pending"

    val Consumers: String       = "consumers"
    val LastDeliveredId: String = "last-delivered-id"

    val Length: String          = "length"
    val RadixTreeKeys: String   = "radix-tree-keys"
    val RadixTreeNodes: String  = "radix-tree-nodes"
    val Groups: String          = "groups"
    val LastGeneratedId: String = "last-generated-id"
    val FirstEntry: String      = "first-entry"
    val LastEntry: String       = "last-entry"

    val Entries: String  = "entries"
    val PelCount: String = "pel-count"
    val SeenTime: String = "seen-time"
  }

}
