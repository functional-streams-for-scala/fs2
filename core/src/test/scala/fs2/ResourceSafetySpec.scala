package fs2

import TestUtil._
import fs2.util.Task
import java.util.concurrent.atomic.AtomicLong
import org.scalacheck.Prop._
import org.scalacheck._

object ResourceSafetySpec extends Properties("ResourceSafety") {

  property("pure fail") = protect {
    try { Stream.emit(0).flatMap(_ => Stream.fail(Err)) === Vector(); false }
    catch { case Err => true } // expected
  }

  def spuriousFail(s: Stream[Task,Int], f: Failure): Stream[Task,Int] =
    s.flatMap { i => if (i % (math.random * 10 + 1).toInt == 0) f.get
                     else Stream.emit(i) }

  property("bracket (normal termination / failing)") = forAll {
  (s0: List[PureStream[Int]], f: Failure, ignoreFailure: Boolean) =>
    val c = new AtomicLong(0)
    val s = s0.map { s => bracket(c)(if (ignoreFailure) s.get else spuriousFail(s.get,f)) }
    val s2 = s.foldLeft(Stream.empty: Stream[Task,Int])(_ ++ _)
    swallow { run(s2) }
    swallow { run(s2.take(1000)) }
    f.tag |: 0L =? c.get
  }

  property("bracket ++ throw Err") = forAll { (s: PureStream[Int]) =>
    val c = new AtomicLong(0)
    val b = bracket(c)(s.get ++ ((throw Err): Stream[Pure,Int]))
    swallow { b }
    c.get ?= 0
  }

  property("1 million brackets in sequence") = protect {
    val c = new AtomicLong(0)
    val b = bracket(c)(Stream.emit(1))
    val bs = Stream.range(0, 1000000).flatMap(_ => b)
    run { bs }
    c.get ?= 0
  }

  property("nested bracket") = forAll { (s0: List[Int], f: Failure, finalizerFail: Boolean) =>
    val c = new AtomicLong(0)
    // construct a deeply nested bracket stream in which the innermost stream fails
    // and check that as we unwind the stack, all resources get released
    // Also test for case where finalizer itself throws an error
    val innermost =
      if (!finalizerFail) f.get
      else Stream.bracket(Task.delay(c.decrementAndGet))(
             _ => f.get,
             _ => Task.delay { c.incrementAndGet; throw Err })
    val nested = Chunk.seq(s0).foldRight(innermost)((i,inner) => bracket(c)(Stream.emit(i) ++ inner))
    try { run { nested }; throw Err } // this test should always fail, so the `run` should throw
    catch { case Err => () }
    f.tag |: 0L =? c.get
  }

  case class Small(get: Long)
  implicit def smallLongArb = Arbitrary(Gen.choose(0L,10L).map(Small(_)))

  property("early termination of bracket") = forAll {
    (s: PureStream[Int], i: Small, j: Small, k: Small) =>
    val c = new AtomicLong(0)
    val bracketed = bracket(c)(s.get)
    run { bracketed.take(i.get) } // early termination
    run { bracketed.take(i.get).take(j.get) } // 2 levels termination
    run { bracketed.take(i.get).take(i.get) } // 2 levels of early termination (2)
    run { bracketed.take(i.get).take(j.get).take(k.get) } // 3 levels of early termination
    // Let's just go crazy
    run { bracketed.take(i.get).take(j.get).take(i.get).take(k.get).take(j.get).take(i.get) }
    s.tag |: 0L =? c.get
  }

  property("asynchronous resource allocation (1)") = forAll {
    (s1: PureStream[Int], f1: Failure) =>
    val c = new AtomicLong(0)
    val b1 = bracket(c)(s1.get)
    val b2 = f1.get
    swallow { run { b1.merge(b2) }}
    swallow { run { b2.merge(b1) }}
    c.get ?= 0L
  }

  property("asynchronous resource allocation (2a)") = forAll { (u: Unit) =>
    val s1 = Stream.fail(Err)
    val s2 = Stream.fail(Err)
    val c = new AtomicLong(0)
    val b1 = bracket(c)(s1)
    val b2 = s2: Stream[Task,Int]
    // subtle test, get different scenarios depending on interleaving:
    // `s2` completes with failure before the resource is acquired by `s2`.
    // `b1` has just caught `s1` error when `s2` fails
    // `b1` fully completes before `s2` fails
    swallow { run { b1 merge b2 }}
    c.get ?= 0L
  }

  property("asynchronous resource allocation (2b)") = forAll {
    (s1: PureStream[Int], s2: PureStream[Int], f1: Failure, f2: Failure) =>
    val c = new AtomicLong(0)
    val b1 = bracket(c)(s1.get)
    val b2 = bracket(c)(s2.get)
    swallow { run { spuriousFail(b1,f1) merge b2 }}
    swallow { run { b1 merge spuriousFail(b2,f2) }}
    swallow { run { spuriousFail(b1,f1) merge spuriousFail(b2,f2) }}
    c.get ?= 0L
  }

  property("asynchronous resource allocation (3)") = forAll {
    (s: PureStream[(PureStream[Int], Option[Failure])]
    , allowFailure: Boolean, f: Failure, n: SmallPositive) =>
    val c = new AtomicLong(0)
    val s2 = bracket(c)(s.get.map { case (s, f) =>
      if (allowFailure) f.map(f => spuriousFail(bracket(c)(s.get), f)).getOrElse(bracket(c)(s.get))
      else bracket(c)(s.get)
    })
    swallow { run { concurrent.join(n.get)(s2) }}
    c.get ?= 0L
  }

  property("asynchronous resource allocation (4)") = forAll {
    (s: PureStream[Int], f: Option[Failure]) =>
    val c = new AtomicLong(0)
    val s2 = bracket(c)(f match {
      case None => s.get
      case Some(f) => spuriousFail(s.get, f)
    })
    swallow { run { s2 through process1.prefetch }}
    swallow { run { s2 through process1.prefetch through process1.prefetch }}
    swallow { run { s2 through process1.prefetch through process1.prefetch through process1.prefetch }}
    c.get ?= 0L
  }

  def swallow(a: => Any): Unit =
    try { a; () }
    catch { case e: Throwable => () }

  def bracket[A](c: AtomicLong)(s: Stream[Task,A]): Stream[Task,A] = Stream.suspend {
    Stream.bracket(Task.delay { c.decrementAndGet })(
      _ => s,
      _ => Task.delay { c.incrementAndGet; () })
  }
}
