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

import cats.effect.IO
import scala.concurrent.duration._
import cats.implicits._
import org.scalacheck.effect.PropF.forAllF
import cats.effect.std.CountDownLatch
import cats.effect.Resource
import cats.effect.Resource.ExitCase
import cats.effect.kernel.Outcome.Canceled
import cats.effect.kernel.Outcome.Errored
import cats.data.NonEmptyList

class StreamParMapEvalSuite extends Fs2Suite {

  val l = CountDownLatch[IO](1)

  val Illegal = new IllegalArgumentException

  def err(action: IO[Unit]) = action *> IO.raiseError(Illegal)

  property("toList values") {
    def sleepLimit10AndEmit(i: Int) = IO.sleep(math.abs(i % 10).millis).as(i)

    test("paralleled .sorted equals") {
      forAllF { (s: Stream[Pure, Int]) =>
        s.covary[IO]
          .parEvalMapUnordered(Int.MaxValue)(sleepLimit10AndEmit)
          .compile
          .toList
          .map(_.sorted)
          .assertEquals(s.toList.sorted)
      }
    }

    test("no parallelism - no shuffle") {
      forAllF { (s: Stream[Pure, Int]) =>
        s.covary[IO]
          .parEvalMapUnordered(1)(sleepLimit10AndEmit)
          .compile
          .toList
          .assertEquals(s.toList)
      }
    }
  }

  test("sorts by execution time") {
    def sleepAndEmit(i: Int) = IO.sleep(i.millis).as(i)
    val s = Stream(100, 50, 0, 0).covary[IO]
    s.parEvalMapUnordered(5)(sleepAndEmit).compile.toList.assertEquals(List(0, 0, 50, 100))
  }

  test("waits for element completion") {
    def stream(executed: CountDownLatch[IO], launched: CountDownLatch[IO]) =
      (Stream(executed.await) ++ Stream.exec(launched.release.void))
        .parEvalMapUnordered(2)(identity)

    val action =
      (l, l).tupled.flatMap { case (executed, launched) =>
        for {
          fib <- stream(executed, launched).compile.drain.start
          _ <- launched.await
          either <- IO.race(IO.sleep(500.millis), fib.join)
          _ <- executed.release
          _ <- fib.joinWithNever
        } yield either.isLeft
      }

    action.assert
  }

  test("should launch exactly maxConcurrent") {
    def stream(defs: List[CountDownLatch[IO]]) =
      Stream.emits(defs).covary[IO].parEvalMapUnordered(100)(_.release *> IO.never)

    val action =
      for {
        defs <- List.fill(101)(l).sequence
        fib <- stream(defs).compile.drain.start
        _ <- defs.take(100).traverse(_.await)
        either <- IO.race(IO.sleep(500.millis), defs.last.await)
        _ <- fib.cancel
      } yield either.isLeft

    action.assert
  }

  test("single error in incoming should cancel executing") {
    def stream(cancelled: CountDownLatch[IO]) = {
      val nev = Stream(IO.never.onCancel(cancelled.release))
      val s = nev ++ Stream.raiseError[IO](Illegal)
      s.parEvalMapUnordered(2)(identity)
    }

    val action =
      for {
        cancelled <- l
        fib <- stream(cancelled).compile.drain.start
        _ <- cancelled.await
        outcome <- fib.join
      } yield outcome match {
        case Errored(Illegal) => true
        case _                => false
      }

    action.assert
  }

  test("single error in executing should cancel other executing") {
    def stream(cancelled: CountDownLatch[IO]) =
      Stream(IO.never[Unit].onCancel(cancelled.release), IO.raiseError[Unit](Illegal))
        .covary[IO]
        .parEvalMapUnordered(2)(identity)

    val action =
      for {
        cancelled <- l
        fib <- stream(cancelled).compile.drain.start
        _ <- cancelled.await
        outcome <- fib.join
      } yield outcome match {
        case Errored(Illegal) => true
        case _                => false
      }

    action.assert
  }

  test("two errors in execution should be combined to CompositeError") {
    def stream(errsThrow: CountDownLatch[IO], errsLaunched: CountDownLatch[IO]) =
      for {
        _ <- Stream(err(errsThrow.await), err(errsThrow.await), IO.unit)
          .covary[IO]
          .parEvalMapUnordered(3)(identity)
        _ <- Stream.eval(errsLaunched.release)
      } yield {}

    val action =
      (l, l).tupled.flatMap { case (errsThrow, errsLaunched) =>
        for {
          fib <- stream(errsThrow, errsLaunched).compile.drain.start
          _ <- errsLaunched.await
          _ <- errsThrow.release
          outcome <- fib.join
        } yield outcome match {
          case Errored(CompositeFailure(Illegal, NonEmptyList(Illegal, List()))) => true
          case _                                                                 => false
        }
      }

    action.assert
  }

  test("resources before and after parEvalMapUnordered should be freed") {
    def res(cleanup: CountDownLatch[IO]) =
      Resource
        .makeCase(IO.unit) {
          case ((), ExitCase.Canceled) => cleanup.release
          case _                       => IO.unit
        }

    def stream(
        resCleanup1: CountDownLatch[IO],
        resCleanup2: CountDownLatch[IO],
        resAckuired: CountDownLatch[IO]
    ) =
      for {
        _ <- Stream.resource(res(resCleanup1))
        _ <- Stream(IO.never[Unit], IO.unit).covary[IO].parEvalMapUnordered(10)(identity)
        _ <- Stream.resource(res(resCleanup2))
        _ <- Stream.eval(resAckuired.release)
        _ <- Stream.eval(IO.never[Unit])
      } yield {}

    val action =
      (l, l, l).tupled.flatMap { case (resCleanup1, resCleanup2, resAckuired) =>
        for {
          fib <- stream(resCleanup1, resCleanup2, resAckuired).compile.drain.start
          _ <- resAckuired.await
          _ <- fib.cancel
          _ <- resCleanup1.await.timeout(1.second)
          _ <- resCleanup2.await.timeout(1.second)
          outcome <- fib.join
        } yield outcome match {
          case Canceled() => true
          case _          => false
        }
      }

    action.assert
  }
}
