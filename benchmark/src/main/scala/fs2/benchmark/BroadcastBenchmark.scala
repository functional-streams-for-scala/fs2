/*
 * Copyright (c) 2013 Functional Streams for Scala
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package fs2
package benchmark

import cats.effect.{IO}
import cats.effect.unsafe.implicits.global
import org.openjdk.jmh.annotations.{Benchmark, Param, Scope, State, Setup, TearDown}
import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext

@State(Scope.Thread)
class BroadcastBenchmark {
  val jExecutor = Executors.newSingleThreadExecutor
  val sExecutor = ExecutionContext.fromExecutor(jExecutor)
  //val blocker = Blocker.liftExecutionContext(sExecutor)
  //implicit val cs: ContextShift[IO] = IO.contextShift(sExecutor)
  //implicit val concurrent: Concurrent[IO] = IO.ioConcurrentEffect

  val drainPipe: Pipe[IO, Int, INothing] = _.drain

  @Param(Array("10", "1000", "100000"))
  var streamSize: Int = _

  var fs2Stream: Stream[IO, Int] = _

  @Setup
  def start(): Unit = {
    fs2Stream = Stream.range(1, streamSize).covary[IO] // Every number goes to it own chunk
  }

  @Benchmark
  def noPipe(): Unit = execute(fs2Stream)

  @Benchmark
  def through(): Unit = execute(fs2Stream.through(drainPipe))

  @Benchmark
  def broadcastTo1(): Unit = execute(fs2Stream.broadcastThrough(1)(drainPipe))

  @Benchmark
  def broadcastTo10(): Unit = execute(fs2Stream.broadcastThrough(10)(drainPipe))

  @Benchmark
  def broadcastTo100(): Unit = execute(fs2Stream.broadcastThrough(100)(drainPipe))

  @TearDown
  def shutdown(): Unit = jExecutor.shutdown()

  private def execute[O](s: Stream[IO, O]) = IO.blocking(s.compile.drain).unsafeRunSync()
}
