package zio.redis.api

import zio.redis.Command
import zio.redis.Input._
import zio.redis.Output._

trait Connection {
  final val auth   = Command("AUTH", StringInput, UnitOutput)
  final val echo   = Command("ECHO", StringInput, StringOutput)
  final val ping   = Command("PING", Varargs(StringInput), ByteOutput)
  final val select = Command("SELECT", LongInput, UnitOutput)
}
