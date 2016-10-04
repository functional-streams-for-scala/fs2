package fs2

import fs2.util.UF1
import org.scalacheck.Gen

class StreamSpec extends Fs2Spec {

  "Stream" - {

    "chunk" in forAll { (c: Vector[Int]) =>
      runLog(Stream.chunk(Chunk.seq(c))) shouldBe c
    }

    "fail (1)" in forAll { (f: Failure) =>
      an[Err.type] should be thrownBy f.get.run.unsafeRun()
    }

    "fail (2)" in {
      assert(throws (Err) { Stream.fail(Err) })
    }

    "fail (3)" in {
      assert(throws (Err) { Stream.emit(1) ++ Stream.fail(Err) })
    }

    "eval" in {
      runLog(Stream.eval(Task.delay(23))) shouldBe Vector(23)
    }

    "++" in forAll { (s: PureStream[Int], s2: PureStream[Int]) =>
      runLog(s.get ++ s2.get) shouldBe { runLog(s.get) ++ runLog(s2.get) }
    }

    "flatMap" in forAll { (s: PureStream[PureStream[Int]]) =>
      runLog(s.get.flatMap(inner => inner.get)) shouldBe { runLog(s.get).flatMap(inner => runLog(inner.get)) }
    }

    "iterate" in {
      Stream.iterate(0)(_ + 1).take(100).toList shouldBe List.iterate(0, 100)(_ + 1)
    }

    "iterateEval" in {
      Stream.iterateEval(0)(i => Task.delay(i + 1)).take(100).runLog.unsafeRun() shouldBe List.iterate(0, 100)(_ + 1)
    }

    "map" in forAll { (s: PureStream[Int]) =>
      runLog(s.get.map(identity)) shouldBe runLog(s.get)
    }

    "onError (1)" in forAll { (s: PureStream[Int], f: Failure) =>
      val s2 = s.get ++ f.get
      runLog(s2.onError(_ => Stream.empty)) shouldBe runLog(s.get)
    }

    "onError (2)" in {
      runLog(Stream.fail(Err) onError { _ => Stream.emit(1) }) shouldBe Vector(1)
    }

    "onError (3)" in {
      runLog(Stream.emit(1) ++ Stream.fail(Err) onError { _ => Stream.emit(1) }) shouldBe Vector(1,1)
    }

    "onError (4)" in {
      Stream.eval(Task.delay(throw Err)).map(Right(_)).onError(t => Stream.emit(Left(t)))
            .take(1)
            .runLog.unsafeRun() shouldBe Vector(Left(Err))
    }

    "onError (5)" in {
      val r = Stream.fail[Task](Err).onError(e => Stream.emit(e)).flatMap(Stream.emit).runLog.unsafeRun()
      val r2 = Stream.fail[Task](Err).onError(e => Stream.emit(e)).map(identity).runLog.unsafeRun()
      val r3 = concurrent.join[Task, Int](4)(Stream(Stream.emit(1), Stream.fail(Err), Stream.emit(2)))
                         .attempt.runLog.unsafeRun()
      r shouldBe Vector(Err)
      r2 shouldBe Vector(Err)
      r3.contains(Left(Err)) shouldBe true
    }

    "range" in {
      Stream.range(0, 100).toList shouldBe List.range(0, 100)
      Stream.range(0, 1).toList shouldBe List.range(0, 1)
      Stream.range(0, 0).toList shouldBe List.range(0, 0)
      Stream.range(0, 101, 2).toList shouldBe List.range(0, 101, 2)
      Stream.range(5,0, -1).toList shouldBe List.range(5,0,-1)
      Stream.range(5,0, 1).toList shouldBe Nil
      Stream.range(10, 50, 0).toList shouldBe Nil
    }

    "ranges" in forAll(Gen.choose(1, 101)) { size =>
      Stream.ranges[Task](0, 100, size).flatMap { case (i,j) => Stream.emits(i until j) }.runLog.unsafeRun() shouldBe
        IndexedSeq.range(0, 100)
    }

    "translate (1)" in forAll { (s: PureStream[Int]) =>
      runLog(s.get.flatMap(i => Stream.eval(Task.now(i))).translate(UF1.id[Task])) shouldBe
      runLog(s.get)
    }

    "translate (2)" in forAll { (s: PureStream[Int]) =>
      runLog(s.get.translate(UF1.id[Pure]).covary[Task]) shouldBe runLog(s.get)
    }

    "toList" in forAll { (s: PureStream[Int]) =>
      s.get.toList shouldBe runLog(s.get).toList
    }

    "toVector" in forAll { (s: PureStream[Int]) =>
      s.get.toVector shouldBe runLog(s.get)
    }

    "unfold" in {
      Stream.unfold((0, 1)) {
        case (f1, f2) => if (f1 <= 13) Some(((f1, f2), (f2, f1 + f2))) else None
      }.map(_._1).toList shouldBe List(0, 1, 1, 2, 3, 5, 8, 13)
    }

    "unfoldEval" in {
      Stream.unfoldEval(10)(s => Task.now(if (s > 0) Some((s, s - 1)) else None))
        .runLog.unsafeRun().toList shouldBe List.range(10, 0, -1)
    }

    "translate stack safety" in {
      import fs2.util.{~>}
      Stream.repeatEval(Task.delay(0)).translate(new (Task ~> Task) { def apply[X](x: Task[X]) = Task.suspend(x) }).take(1000000).run.unsafeRun()
    }

    "repeaat empty" in {
      Stream.empty.pure.repeat.toList shouldBe List()
    }
  }
}
