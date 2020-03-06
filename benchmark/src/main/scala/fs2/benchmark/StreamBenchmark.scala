package fs2
package benchmark

import cats.effect.IO
import org.openjdk.jmh.annotations.{
  Benchmark,
  BenchmarkMode,
  Mode,
  OutputTimeUnit,
  Param,
  Scope,
  State
}
import java.util.concurrent.TimeUnit

@State(Scope.Thread)
class StreamBenchmark {
  @Param(Array("10", "100", "1000", "10000"))
  var n: Int = _

  @Benchmark
  def leftAssocConcat(): Int =
    (0 until n)
      .map(Stream.emit)
      .foldRight(Stream.empty.covaryOutput[Int])(_ ++ _)
      .covary[IO]
      .compile
      .last
      .unsafeRunSync
      .get

  @Benchmark
  def rightAssocConcat(): Int =
    (0 until n)
      .map(Stream.emit)
      .foldRight(Stream.empty.covaryOutput[Int])(_ ++ _)
      .covary[IO]
      .compile
      .last
      .unsafeRunSync
      .get

  @Benchmark
  def leftAssocFlatMap(): Int =
    (0 until n)
      .map(Stream.emit)
      .foldLeft(Stream.emit(0))((acc, a) => acc.flatMap(_ => a))
      .covary[IO]
      .compile
      .last
      .unsafeRunSync
      .get

  @Benchmark
  def rightAssocFlatMap(): Int =
    (0 until n)
      .map(Stream.emit)
      .reverse
      .foldLeft(Stream.emit(0))((acc, a) => a.flatMap(_ => acc))
      .covary[IO]
      .compile
      .last
      .unsafeRunSync
      .get

  @Benchmark
  def eval(): Unit =
    Stream.repeatEval(IO(())).take(n).compile.last.unsafeRunSync.get

  @Benchmark
  def toVector(): Vector[Int] =
    Stream.emits(0 until n).covary[IO].compile.toVector.unsafeRunSync

  @Benchmark @BenchmarkMode(Array(Mode.AverageTime)) @OutputTimeUnit(TimeUnit.NANOSECONDS)
  def emitsThenFlatMap(): Vector[Int] =
    Stream.emits(0 until n).flatMap(Stream(_)).toVector

  @Benchmark
  def sliding() =
    Stream.emits(0 until 16384).sliding(n).covary[IO].compile.drain.unsafeRunSync

  @Benchmark
  def mapAccumulate() =
    Stream
      .emits(0 until n)
      .mapAccumulate(0) {
        case (acc, i) =>
          val added = acc + i
          (added, added)
      }
      .covary[IO]
      .compile
      .drain
      .unsafeRunSync

  @Benchmark
  def evalMap() =
    Stream.emits(0 until n).evalMap(x => IO(x * 5)).compile.drain.unsafeRunSync

  @Benchmark
  def evalMaps() =
    Stream.emits(0 until n).evalMapChunk(x => IO(x * 5)).compile.drain.unsafeRunSync
}
