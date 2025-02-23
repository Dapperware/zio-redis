package zio.redis

import zio.prelude.Assertion.greaterThanOrEqualTo
import zio.prelude.Newtype

package object options {
  object PositiveLong extends Newtype[Long] {
    override def assertion = assert(greaterThanOrEqualTo(1L)) // scalafix:ok
  }

  type PositiveLong = PositiveLong.Type
}
