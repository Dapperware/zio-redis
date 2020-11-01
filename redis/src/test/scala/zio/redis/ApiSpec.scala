package zio.redis

import zio.clock.Clock
import zio.logging.Logging
import zio.test._

object ApiSpec
    extends KeysSpec
    with ListSpec
    with SetsSpec
    with SortedSetsSpec
    with StringsSpec
    with GeoSpec
    with HyperLogLogSpec
    with HashSpec {

  def spec =
    suite("Redis commands")(
      keysSuite,
      listSuite,
      setsSuite,
      sortedSetsSuite,
      stringsSuite,
      geoSuite,
      hyperLogLogSuite,
      hashSuite
    ).provideCustomLayerShared(Logging.ignore >>> Executor ++ Clock.live)

  private val Executor = RedisExecutor.loopback().orDie
}
