package zio.redis.options

trait SortedSets {
  sealed trait Aggregate extends Product { self =>
    private[redis] final def stringify: String =
      self match {
        case Aggregate.Max => "MAX"
        case Aggregate.Min => "MIN"
        case Aggregate.Sum => "SUM"
      }
  }

  object Aggregate {
    case object Sum extends Aggregate
    case object Min extends Aggregate
    case object Max extends Aggregate
  }

  case object Changed {
    private[redis] def stringify: String = "CH"
  }

  type Changed = Changed.type

  case object Increment {
    private[redis] def stringify: String = "INCR"
  }

  type Increment = Increment.type

  sealed trait LexMaximum { self =>
    private[redis] final def stringify: String =
      self match {
        case LexMaximum.Unbounded     => "+"
        case LexMaximum.Open(value)   => s"($value"
        case LexMaximum.Closed(value) => s"[$value"
      }
  }

  object LexMaximum {
    case object Unbounded                   extends LexMaximum
    sealed case class Open(value: String)   extends LexMaximum
    sealed case class Closed(value: String) extends LexMaximum
  }

  sealed trait LexMinimum { self =>
    private[redis] final def stringify: String =
      self match {
        case LexMinimum.Unbounded     => "-"
        case LexMinimum.Open(value)   => s"($value"
        case LexMinimum.Closed(value) => s"[$value"
      }
  }

  object LexMinimum {
    case object Unbounded                   extends LexMinimum
    sealed case class Open(value: String)   extends LexMinimum
    sealed case class Closed(value: String) extends LexMinimum
  }

  sealed case class LexRange(min: LexMinimum, max: LexMaximum)

  sealed case class MemberScore[+M](score: Double, member: M)

  sealed trait ScoreMaximum { self =>
    private[redis] final def stringify: String =
      self match {
        case ScoreMaximum.Infinity      => "+inf"
        case ScoreMaximum.Open(value)   => s"($value"
        case ScoreMaximum.Closed(value) => s"$value"
      }
  }

  object ScoreMaximum {
    case object Infinity                    extends ScoreMaximum
    sealed case class Open(value: Double)   extends ScoreMaximum
    sealed case class Closed(value: Double) extends ScoreMaximum
  }

  sealed trait ScoreMinimum { self =>
    private[redis] final def stringify: String =
      self match {
        case ScoreMinimum.Infinity      => "-inf"
        case ScoreMinimum.Open(value)   => s"($value"
        case ScoreMinimum.Closed(value) => s"$value"
      }
  }

  object ScoreMinimum {
    case object Infinity                    extends ScoreMinimum
    sealed case class Open(value: Double)   extends ScoreMinimum
    sealed case class Closed(value: Double) extends ScoreMinimum
  }

  sealed case class ScoreRange(min: ScoreMinimum, max: ScoreMaximum)

  case object WithScores {
    private[redis] def stringify: String = "WITHSCORES"
  }

  type WithScores = WithScores.type

}
