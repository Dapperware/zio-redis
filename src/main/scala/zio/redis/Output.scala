package zio.redis

import zio.Chunk
import zio.duration.Duration

sealed trait Output[+A] {
  private[redis] def decode(text: String): Either[RedisError, A] = Left(RedisError.WrongType(text))
}

object Output {
  case object BoolOutput extends Output[Boolean] {
    // def decode(text: String): Either[RedisError, Boolean] = ???
  }

  case object ByteOutput extends Output[Chunk[Byte]] {
    // def decode(text: String): Either[RedisError, Chunk[Byte]] = ???
  }

  case object ChunkOutput extends Output[Chunk[Chunk[Byte]]] {
    // def decode(text: String): Either[RedisError, Chunk[Chunk[Byte]]] = ???
  }

  case object DoubleOutput extends Output[Double] {
    // def decode(text: String): Either[RedisError, Double] = ???
  }

  case object DurationOutput extends Output[Duration] {
    // def decode(text: String): Either[RedisError, Duration] = ???
  }

  case object LongOutput extends Output[Long] {
    // def decode(text: String): Either[RedisError, Long] = ???
  }

  final case class OptionalOutput[+A](output: Output[A]) extends Output[Option[A]] {
    // def decode(text: String): Either[RedisError, Option[A]] = ???
  }

  case object ScanOutput extends Output[(Long, Chunk[Chunk[Byte]])] {
    // def decode(text: String): Either[RedisError, (Long, Chunk[Chunk[Byte]])] = ???
  }

  case object StringOutput extends Output[String] {
    // def decode(text: String): Either[RedisError, String] = ???
  }

  case object UnitOutput extends Output[Unit] {
    // def decode(text: String): Either[RedisError, Unit] = ???
  }
}
