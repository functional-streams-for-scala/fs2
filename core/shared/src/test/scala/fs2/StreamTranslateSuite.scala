package fs2

import scala.concurrent.duration._

import cats.~>
import cats.effect.IO

class StreamTranslateSuite extends Fs2Suite {
  test("1 - id") {
    forAllAsync { (s: Stream[Pure, Int]) =>
      val expected = s.toList
      s.covary[IO]
        .flatMap(i => Stream.eval(IO.pure(i)))
        .translate(cats.arrow.FunctionK.id[IO])
        .compile
        .toList
        .map(it => assert(it == expected))
    }
  }

  test("2") {
    forAllAsync { (s: Stream[Pure, Int]) =>
      val expected = s.toList
      s.covary[Function0]
        .flatMap(i => Stream.eval(() => i))
        .flatMap(i => Stream.eval(() => i))
        .translate(new (Function0 ~> IO) {
          def apply[A](thunk: Function0[A]) = IO(thunk())
        })
        .compile
        .toList
        .map(it => assert(it == expected))
    }
  }

  test("3 - ok to have multiple translates") {
    forAllAsync { (s: Stream[Pure, Int]) =>
      val expected = s.toList
      s.covary[Function0]
        .flatMap(i => Stream.eval(() => i))
        .flatMap(i => Stream.eval(() => i))
        .translate(new (Function0 ~> Some) {
          def apply[A](thunk: Function0[A]) = Some(thunk())
        })
        .flatMap(i => Stream.eval(Some(i)))
        .flatMap(i => Stream.eval(Some(i)))
        .translate(new (Some ~> IO) {
          def apply[A](some: Some[A]) = IO(some.get)
        })
        .compile
        .toList
        .map(it => assert(it == expected))
    }
  }

  test("4 - ok to translate after zip with effects") {
    val stream: Stream[Function0, Int] =
      Stream.eval(() => 1)
    stream
      .zip(stream)
      .translate(new (Function0 ~> IO) {
        def apply[A](thunk: Function0[A]) = IO(thunk())
      })
      .compile
      .toList
      .map(it => assert(it == List((1, 1))))
  }

  test("5 - ok to translate a step leg that emits multiple chunks") {
    def goStep(step: Option[Stream.StepLeg[Function0, Int]]): Pull[Function0, Int, Unit] =
      step match {
        case None       => Pull.done
        case Some(step) => Pull.output(step.head) >> step.stepLeg.flatMap(goStep)
      }
    (Stream.eval(() => 1) ++ Stream.eval(() => 2)).pull.stepLeg
      .flatMap(goStep)
      .stream
      .translate(new (Function0 ~> IO) {
        def apply[A](thunk: Function0[A]) = IO(thunk())
      })
      .compile
      .toList
      .map(it => assert(it == List(1, 2)))
  }

  test("6 - ok to translate step leg that has uncons in its structure") {
    def goStep(step: Option[Stream.StepLeg[Function0, Int]]): Pull[Function0, Int, Unit] =
      step match {
        case None       => Pull.done
        case Some(step) => Pull.output(step.head) >> step.stepLeg.flatMap(goStep)
      }
    (Stream.eval(() => 1) ++ Stream.eval(() => 2))
      .flatMap(a => Stream.emit(a))
      .flatMap(a => Stream.eval(() => a + 1) ++ Stream.eval(() => a + 2))
      .pull
      .stepLeg
      .flatMap(goStep)
      .stream
      .translate(new (Function0 ~> IO) {
        def apply[A](thunk: Function0[A]) = IO(thunk())
      })
      .compile
      .toList
      .map(it => assert(it == List(2, 3, 3, 4)))
  }

  test("7 - ok to translate step leg that is forced back in to a stream") {
    def goStep(step: Option[Stream.StepLeg[Function0, Int]]): Pull[Function0, Int, Unit] =
      step match {
        case None => Pull.done
        case Some(step) =>
          Pull.output(step.head) >> step.stream.pull.echo
      }
    (Stream.eval(() => 1) ++ Stream.eval(() => 2)).pull.stepLeg
      .flatMap(goStep)
      .stream
      .translate(new (Function0 ~> IO) {
        def apply[A](thunk: Function0[A]) = IO(thunk())
      })
      .compile
      .toList
      .map(it => assert(it == List(1, 2)))
  }

  test("stack safety") {
    Stream
      .repeatEval(IO(0))
      .translate(new (IO ~> IO) { def apply[X](x: IO[X]) = IO.suspend(x) })
      .take(if (isJVM) 1000000 else 10000)
      .compile
      .drain
  }

  test("translateInterruptible") {
    Stream
      .eval(IO.never)
      .merge(Stream.eval(IO(1)).delayBy(5.millis).repeat)
      .interruptAfter(10.millis)
      .translateInterruptible(cats.arrow.FunctionK.id[IO])
      .compile
      .drain
  }

}
