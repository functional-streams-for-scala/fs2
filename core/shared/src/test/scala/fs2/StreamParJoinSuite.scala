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

import scala.concurrent.duration._

import cats.effect.IO
import cats.effect.kernel.{Deferred, Ref}
import cats.syntax.all._
import org.scalacheck.effect.PropF.forAllF

class StreamParJoinSuite extends Fs2Suite {
  test("no concurrency") {
    forAllF { (s: Stream[Pure, Int]) =>
      val expected = s.toList
      s.covary[IO]
        .map(Stream.emit(_).covary[IO])
        .parJoin(1)
        .compile
        .toList
        .assertEquals(expected)
    }
  }

  test("concurrency") {
    forAllF { (s: Stream[Pure, Int], n0: Int) =>
      val n = (n0 % 20).abs + 1
      val expected = s.toList.toSet
      s.covary[IO]
        .map(Stream.emit(_).covary[IO])
        .parJoin(n)
        .compile
        .toList
        .map(_.toSet)
        .assertEquals(expected)
    }
  }

  test("concurrent flattening") {
    forAllF { (s: Stream[Pure, Stream[Pure, Int]], n0: Int) =>
      val n = (n0 % 20).abs + 1
      val expected = s.flatten.toList.toSet
      s.map(_.covary[IO])
        .covary[IO]
        .parJoin(n)
        .compile
        .toList
        .map(_.toSet)
        .assertEquals(expected)
    }
  }

  test("merge consistency") {
    forAllF { (s1: Stream[Pure, Int], s2: Stream[Pure, Int]) =>
      val parJoined = Stream(s1.covary[IO], s2).parJoin(2).compile.toList.map(_.toSet)
      val merged = s1.covary[IO].merge(s2).compile.toList.map(_.toSet)
      (parJoined, merged).tupled.map { case (pj, m) => assertEquals(pj, m) }
    }
  }

  test("resources acquired in outer stream are released after inner streams complete") {
    val bracketed =
      Stream.bracket(IO(new java.util.concurrent.atomic.AtomicBoolean(true)))(b => IO(b.set(false)))
    // Starts an inner stream which fails if the resource b is finalized
    val s: Stream[IO, Stream[IO, Unit]] = bracketed.map { b =>
      Stream
        .eval(IO(b.get))
        .flatMap(b => if (b) Stream(()) else Stream.raiseError[IO](new Err))
        .repeat
        .take(10000)
    }
    s.parJoinUnbounded.compile.drain
  }

  test("run finalizers of inner streams first") {
    forAllF { (s1: Stream[Pure, Int], bias: Boolean) =>
      val err = new Err
      val biasIdx = if (bias) 1 else 0
      Ref
        .of[IO, List[String]](Nil)
        .flatMap { finalizerRef =>
          Ref.of[IO, List[Int]](Nil).flatMap { runEvidenceRef =>
            Deferred[IO, Unit].flatMap { halt =>
              def bracketed =
                Stream.bracket(IO.unit)(_ => finalizerRef.update(_ :+ "Outer"))

              def registerRun(idx: Int): IO[Unit] =
                runEvidenceRef.update(_ :+ idx)

              def finalizer(idx: Int): IO[Unit] =
                // this introduces delay and failure based on bias of the test
                if (idx == biasIdx)
                  IO.sleep(100.millis) >>
                    finalizerRef.update(_ :+ s"Inner $idx") >>
                    IO.raiseError(err)
                else
                  finalizerRef.update(_ :+ s"Inner $idx")

              val prg0 =
                bracketed.flatMap { _ =>
                  Stream(
                    Stream.bracket(registerRun(0))(_ => finalizer(0)) >> s1,
                    Stream.bracket(registerRun(1))(_ => finalizer(1)) >> Stream
                      .exec(halt.complete(()).void)
                  )
                }

              prg0.parJoinUnbounded.compile.drain.attempt.flatMap { r =>
                finalizerRef.get.flatMap { finalizers =>
                  runEvidenceRef.get.flatMap { streamRunned =>
                    IO {
                      val expectedFinalizers = streamRunned.map { idx =>
                        s"Inner $idx"
                      } :+ "Outer"
                      assertEquals(finalizers.toSet, expectedFinalizers.toSet)
                      assertEquals(finalizers.lastOption, Some("Outer"))
                      if (streamRunned.contains(biasIdx)) assertEquals(r, Left(err))
                      else assertEquals(r, Right(()))
                    }
                  }
                }
              }
            }
          }
        }
    }
  }

  group("hangs") {
    val full = Stream.constant(42).chunks.evalTap(_ => IO.cede).flatMap(Stream.chunk)
    val hang = Stream.repeatEval(IO.never[Unit])
    val hang2: Stream[IO, Nothing] = full.drain
    val hang3: Stream[IO, Nothing] =
      Stream
        .repeatEval[IO, Unit](IO.async_[Unit](cb => cb(Right(()))) >> IO.cede)
        .drain

    test("1") {
      Stream(full, hang)
        .parJoin(10)
        .take(1)
        .compile
        .lastOrError
        .assertEquals(42)
    }
    test("2") {
      Stream(full, hang2)
        .parJoin(10)
        .take(1)
        .compile
        .lastOrError
        .assertEquals(42)
    }
    test("3") {
      Stream(full, hang3)
        .parJoin(10)
        .take(1)
        .compile
        .lastOrError
        .assertEquals(42)
    }
    test("4") {
      Stream(hang3, hang2, full)
        .parJoin(10)
        .take(1)
        .compile
        .lastOrError
        .assertEquals(42)
    }
  }

  test("outer failed") {
    Stream(
      Stream.sleep_[IO](1.minute),
      Stream.raiseError[IO](new Err)
    ).parJoinUnbounded.compile.drain
      .intercept[Err]
  }

  test("propagate error from inner stream before ++") {
    val err = new Err

    (Stream
      .emit(Stream.raiseError[IO](err))
      .parJoinUnbounded ++ Stream.emit(1)).compile.drain
      .intercept[Err]
  }

  test("issue-2332") {
    val s = Stream(()).repeatN(4).covary[IO]
    val par4 = s.map(IO.delay(_)).parEvalMap(4)(identity)
    Vector
      .fill(8)(Deferred[IO, Unit])
      .sequence
      .flatMap { defs =>
        val merged = par4.merge(par4)
        merged.zipWithIndex
          .parEvalMap(8) { case (_, i) => defs(i.toInt).complete(()) *> IO.never }
          .compile
          .drain
          .start *> defs.traverse(_.get).as(true)
      }
      .timeout(1.second)
      .assert
  }
}
