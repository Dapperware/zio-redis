package zio.redis

import zio.ZIO
import zio.schema.Schema

trait ResultBuilder[F[_]] {
  import ResultBuilder.NeedsReturnType

  def returning[R: Schema]: ZIO[RedisExecutor, RedisError, F[R]]

  final def map(f: Nothing => Any)(implicit nrt: NeedsReturnType): ZIO[Any, Nothing, Nothing] = ???

  final def flatMap(f: Nothing => Any)(implicit nrt: NeedsReturnType): ZIO[Any, Nothing, Nothing] = ???
}

object ResultBuilder {
  @annotation.implicitNotFound("Use `returning[A]` to specify method's return type")
  final abstract class NeedsReturnType
}
