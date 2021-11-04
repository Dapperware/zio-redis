package zio.redis.sets

import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations._

import zio.ZIO
import zio.redis.{BenchmarkRuntime, sAdd, sMove}

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 15)
@Warmup(iterations = 15)
@Fork(2)
class SMoveBenchmarks extends BenchmarkRuntime {

  @Param(Array("500"))
  var count: Int = _

  private var items: List[String] = _

  private val key            = "test-set1"
  private val destinationKey = "test-set2"

  @Setup(Level.Trial)
  def setup(): Unit = {
    items = (0 to count).toList.map(_.toString)
    zioUnsafeRun(sAdd(key, items.head, items.tail: _*).unit)
  }

  @Benchmark
  def laserdisc(): Unit = {
    import _root_.laserdisc.fs2._
    import _root_.laserdisc.{all => cmd, _}
    import cats.instances.list._
    import cats.syntax.foldable._

    unsafeRun[LaserDiscClient](c =>
      items.traverse_(i => c.send(cmd.smove(Key.unsafeFrom(key), Key.unsafeFrom(destinationKey), Key.unsafeFrom(i))))
    )
  }

  @Benchmark
  def rediculous(): Unit = {
    import cats.implicits._
    import io.chrisdavenport.rediculous._
    unsafeRun[RediculousClient](c => items.traverse_(i => RedisCommands.smove[RedisIO](key, destinationKey, i).run(c)))
  }

  @Benchmark
  def redis4cats(): Unit = {
    import cats.instances.list._
    import cats.syntax.foldable._
    unsafeRun[Redis4CatsClient[String]](c => items.traverse_(i => c.sMove(key, destinationKey, i)))
  }

  @Benchmark
  def zio(): Unit = zioUnsafeRun(ZIO.foreach_(items)(i => sMove(key, destinationKey, i)))
}
