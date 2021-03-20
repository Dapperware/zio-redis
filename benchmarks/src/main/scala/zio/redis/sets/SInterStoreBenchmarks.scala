package zio.redis.sets

import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations._

import zio.ZIO
import zio.redis.{ BenchmarkRuntime, sAdd, sInterStore }

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 15)
@Warmup(iterations = 15)
@Fork(2)
class SInterStoreBenchmarks extends BenchmarkRuntime {

  @Param(Array("500"))
  private var count: Int = _

  private var items: List[String]      = _
  private var otherItems: List[String] = _

  private val key            = "test-set1"
  private val otherKey       = "test-set2"
  private val destinationKey = "test-set3"

  @Setup(Level.Trial)
  def setup(): Unit = {
    items = (0 to count).toList.map(_.toString)
    otherItems = (0 to count).toList.map(_.toString)
    zioUnsafeRun(sAdd(key, items.head, items.tail: _*).unit)
    zioUnsafeRun(sAdd(otherKey, otherItems.head, otherItems.tail: _*).unit)

  }

  @Benchmark
  def laserdisc(): Unit = {
    import _root_.laserdisc.fs2._
    import _root_.laserdisc.{ all => cmd, _ }
    import cats.instances.list._
    import cats.syntax.foldable._

    unsafeRun[LaserDiscClient](c =>
      items.traverse_(_ =>
        c.send(cmd.sinterstore(Key.unsafeFrom(key), Key.unsafeFrom(otherKey), Key.unsafeFrom(destinationKey)))
      )
    )
  }

  @Benchmark
  def rediculous(): Unit = {
    import cats.implicits._
    import io.chrisdavenport.rediculous._
    unsafeRun[RediculousClient](c =>
      items.traverse_(_ => RedisCommands.sinterstore[RedisIO](destinationKey, List(key, otherKey)).run(c))
    )
  }

  @Benchmark
  def redis4cats(): Unit = {
    import cats.instances.list._
    import cats.syntax.foldable._
    unsafeRun[Redis4CatsClient[String]](c => items.traverse_(_ => c.sInterStore(destinationKey, key, otherKey)))
  }

  @Benchmark
  def zio(): Unit = zioUnsafeRun(ZIO.foreach_(items)(_ => sInterStore(destinationKey, key, otherKey)))
}
