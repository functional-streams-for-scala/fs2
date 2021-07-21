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

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.openjdk.jmh.annotations.{Benchmark, Param, Scope, State}

@State(Scope.Thread)
class StreamConcurrentBenchmark {

  @Param(Array("10", "1000"))
  var n: Int = _

  @Param(Array("1"))
  var fibN: Int = _

  private def fib(n: Long): Long =
    if (n < 2) 1
    else fib(n - 1) + fib(n - 2)

  private def dummyLoad = IO.delay(fib(fibN))

  @Benchmark
  def evalMap(): Unit =
    execute(getStream.evalMap(_ => dummyLoad))

  @Benchmark
  def prevParEvalMapUnordered1(): Unit =
    execute(getStream.prevParEvalMapUnordered(1)(_ => dummyLoad))

  @Benchmark
  def parEvalMapUnordered1(): Unit =
    execute(getStream.parEvalMapUnordered(1)(_ => dummyLoad))

  @Benchmark
  def prevParEvalMapUnordered10(): Unit =
    execute(getStream.prevParEvalMapUnordered(10)(_ => dummyLoad))

  @Benchmark
  def parEvalMapUnordered10(): Unit =
    execute(getStream.parEvalMapUnordered(10)(_ => dummyLoad))

  private def getStream: Stream[IO, Unit] = Stream.constant(()).take(n).covary[IO]
  private def execute(s: Stream[IO, Long]) = s.compile.drain.unsafeRunSync()
}
