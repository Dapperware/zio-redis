package zio.redis.codec

import java.nio.charset.StandardCharsets

import zio.redis.RedisError.CodecError
import zio.schema.Schema
import zio.schema.StandardType.{ DoubleType, IntType, LongType }
import zio.schema.codec.Codec
import zio.stream.ZTransducer
import zio.{ Chunk, ZIO }

object StringUtf8Codec extends Codec {
  def encoder[A](schema: Schema[A]): ZTransducer[Any, Nothing, A, Byte] =
    ZTransducer.fromPush { (opt: Option[Chunk[A]]) =>
      ZIO.succeed(opt.map(values => values.flatMap(Encoder.encode(schema, _))).getOrElse(Chunk.empty))
    }

  def encode[A](schema: Schema[A]): A => Chunk[Byte] = { a =>
    Encoder.encode(schema, a)
  }

  def decoder[A](schema: Schema[A]): ZTransducer[Any, String, Byte, A] =
    ZTransducer.fromPush { (opt: Option[Chunk[Byte]]) =>
      ZIO.fromEither(opt.map(chunk => Decoder.decode(schema, chunk).map(Chunk(_))).getOrElse(Right(Chunk.empty)))
    }

  def decode[A](schema: Schema[A]): Chunk[Byte] => Either[String, A] = { ch =>
    Decoder.decode(schema, ch)
  }

  object Encoder {
    def encode[A](schema: Schema[A], value: A): Chunk[Byte] =
      schema match {
        case Schema.Primitive(_) => Chunk.fromArray(value.toString.getBytes(StandardCharsets.UTF_8))
        case _                   => throw CodecError("the codec support only primitives")
      }
  }

  object Decoder {
    def decode[A](schema: Schema[A], chunk: Chunk[Byte]): Either[String, A] = {
      def utf8String = new String(chunk.toArray, StandardCharsets.UTF_8)

      schema match {
        case Schema.Primitive(IntType)    => Right(utf8String.toInt.asInstanceOf[A])
        case Schema.Primitive(LongType)   => Right(utf8String.toLong.asInstanceOf[A])
        case Schema.Primitive(DoubleType) => Right(utf8String.toDouble.asInstanceOf[A])
        case Schema.Primitive(_)          => Right(utf8String.asInstanceOf[A])
        case _                            => Left("the codec support only primitives")
      }
    }
  }
}
