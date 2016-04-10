package fs2

import fs2.util.{Free,Monad,RealSupertype,Sub1,~>}

/**
 * Mixin trait for various non-primitive operations exposed on `Stream`
 * for syntactic convenience.
 */
private[fs2]
trait StreamOps[+F[_],+A] extends Process1Ops[F,A] /* with TeeOps[F,A] with WyeOps[F,A] */ {
  self: Stream[F,A] =>

  import Stream.Handle

  // NB: methods in alphabetical order

  def ++[F2[_],B>:A](p2: => Stream[F2,B])(implicit R: RealSupertype[A,B], S: Sub1[F,F2]): Stream[F2,B] =
    Stream.append(Sub1.substStream(self), p2)

  def attempt: Stream[F,Either[Throwable,A]] =
    self.map(Right(_)).onError(e => Stream.emit(Left(e)))

  def append[F2[_],B>:A](p2: => Stream[F2,B])(implicit R: RealSupertype[A,B], S: Sub1[F,F2]): Stream[F2,B] =
    Stream.append(Sub1.substStream(self), p2)

  /** Prepend a single chunk onto the front of this stream. */
  def cons[A2>:A](c: Chunk[A2])(implicit T: RealSupertype[A,A2]): Stream[F,A2] =
    Stream.cons[F,A2](self)(c)

  /** Prepend a single value onto the front of this stream. */
  def cons1[A2>:A](a: A2)(implicit T: RealSupertype[A,A2]): Stream[F,A2] =
    cons(Chunk.singleton(a))

  def covary[F2[_]](implicit S: Sub1[F,F2]): Stream[F2,A] =
    Sub1.substStream(self)

  def drain: Stream[F, Nothing] =
    Stream.drain(self)

  /** Alias for `[[wye.either]](self, s2)`. */
  def either[F2[_]:Async,B](s2: Stream[F2,B])(implicit S: Sub1[F,F2]): Stream[F2,Either[A,B]] =
    fs2.wye.either(Sub1.substStream(self), s2)

  def evalMap[F2[_],B](f: A => F2[B])(implicit S: Sub1[F,F2]): Stream[F2,B] =
    Stream.flatMap(Sub1.substStream(self))(f andThen Stream.eval)

  def flatMap[F2[_],B](f: A => Stream[F2,B])(implicit S: Sub1[F,F2]): Stream[F2,B] =
    Stream.flatMap(Sub1.substStream(self))(f)

  def interruptWhen[F2[_]](haltWhenTrue: Stream[F2,Boolean])(implicit S: Sub1[F,F2], F2: Async[F2]): Stream[F2,A] =
    fs2.wye.interrupt(haltWhenTrue, Sub1.substStream(self))

  def interruptWhen[F2[_]](haltWhenTrue: async.immutable.Signal[F2,Boolean])(implicit S: Sub1[F,F2], F2: Async[F2]): Stream[F2,A] =
    fs2.wye.interrupt(haltWhenTrue.discrete, Sub1.substStream(self))

  /** Alias for `[[tee.interleave]](self, s2)`. */
  def interleave[F2[_], B >: A](s2: Stream[F2,B])(implicit R:RealSupertype[A,B], S:Sub1[F,F2]): Stream[F2,B] =
    fs2.tee.interleave.apply(Sub1.substStream(self), s2)

  /** Alias for `[[tee.interleaveAll]](self, s2)`. */
  def interleaveAll[F2[_], B>:A](s2: Stream[F2,B])(implicit R:RealSupertype[A,B], S:Sub1[F,F2]): Stream[F2,B] =
    fs2.tee.interleaveAll.apply(Sub1.substStream(self), s2)

  def mask: Stream[F,A] =
    Stream.mask(self)

  def map[B](f: A => B): Stream[F,B] =
    Stream.map(self)(f)

  /** Alias for `[[wye.merge]](self, s2)`. */
  def merge[F2[_],B>:A](s2: Stream[F2,B])(implicit R: RealSupertype[A,B], S: Sub1[F,F2], F2: Async[F2]): Stream[F2,B] =
    fs2.wye.merge(Sub1.substStream(self), s2)

  /** Alias for `[[wye.mergeHaltBoth]](self, s2)`. */
  def mergeHaltBoth[F2[_],B>:A](s2: Stream[F2,B])(implicit R: RealSupertype[A,B], S: Sub1[F,F2], F2: Async[F2]): Stream[F2,B] =
    fs2.wye.mergeHaltBoth(Sub1.substStream(self), s2)

  /** Alias for `[[wye.mergeHaltL]](self, s2)`. */
  def mergeHaltL[F2[_],B>:A](s2: Stream[F2,B])(implicit R: RealSupertype[A,B], S: Sub1[F,F2], F2: Async[F2]): Stream[F2,B] =
    fs2.wye.mergeHaltL(Sub1.substStream(self), s2)

  /** Alias for `[[wye.mergeHaltR]](self, s2)`. */
  def mergeHaltR[F2[_],B>:A](s2: Stream[F2,B])(implicit R: RealSupertype[A,B], S: Sub1[F,F2], F2: Async[F2]): Stream[F2,B] =
    fs2.wye.mergeHaltR(Sub1.substStream(self), s2)

  /** Alias for `[[wye.mergeDrainL]](self, s2)`. */
  def mergeDrainL[F2[_],B](s2: Stream[F2,B])(implicit S: Sub1[F,F2], F2: Async[F2]): Stream[F2,B] =
    fs2.wye.mergeDrainL(Sub1.substStream(self)(S), s2)

  /** Alias for `[[wye.mergeDrainR]](self, s2)`. */
  def mergeDrainR[F2[_],B](s2: Stream[F2,B])(implicit S: Sub1[F,F2], F2: Async[F2]): Stream[F2,A] =
    fs2.wye.mergeDrainR(Sub1.substStream(self)(S), s2)

  def onComplete[F2[_],B>:A](regardless: => Stream[F2,B])(implicit R: RealSupertype[A,B], S: Sub1[F,F2]): Stream[F2,B] =
    Stream.onComplete(Sub1.substStream(self): Stream[F2,B], regardless)

  def onError[F2[_],B>:A](f: Throwable => Stream[F2,B])(implicit R: RealSupertype[A,B], S: Sub1[F,F2]): Stream[F2,B] =
    Stream.onError(Sub1.substStream(self): Stream[F2,B])(f)

  def onFinalize[F2[_]](f: F2[Unit])(implicit S: Sub1[F,F2], F2: Monad[F2]): Stream[F2,A] =
    Stream.bracket(F2.pure(()))(_ => Sub1.substStream(self), _ => f)

  def open: Pull[F, Nothing, Handle[F,A]] = Stream.open(self)

  def output: Pull[F,A,Unit] = Pull.outputs(self)

  /** Transform this stream using the given `Process1`. */
  def pipe[B](f: Process1[A,B]): Stream[F,B] = process1.covary(f)(self)

  /** Like `pipe`, but the function may add additional effects. */
  def pipev[F2[_],B](f: Stream[F2,A] => Stream[F2,B])(implicit S: Sub1[F,F2]): Stream[F2,B] =
    f(Sub1.substStream(self))

  /** Like `pipe2`, but the function may add additional effects. */
  def pipe2v[F2[_],B,C](s2: Stream[F2,B])(f: (Stream[F2,A], Stream[F2,B]) => Stream[F2,C])(implicit S: Sub1[F,F2]): Stream[F2,C] =
    f(Sub1.substStream(self), s2)

  /** Like `pull`, but the function may add additional effects. */
  def pullv[F2[_],B](using: Handle[F,A] => Pull[F2,B,Any])(implicit S: Sub1[F,F2]): Stream[F2,B] =
    Stream.pull(self)(using)

  /** Repeat this stream an infinite number of times. `s.repeat == s ++ s ++ s ++ ...` */
  def repeat: Stream[F,A] = {
    self ++ repeat
  }

  def run:Free[F,Unit] =
    Stream.runFold(self,())((_,_) => ())

  def runTrace(t: Trace):Free[F,Unit] =
    Stream.runFoldTrace(t)(self,())((_,_) => ())

  def runFold[B](z: B)(f: (B,A) => B): Free[F,B] =
    Stream.runFold(self, z)(f)

  def runFoldTrace[B](t: Trace)(z: B)(f: (B,A) => B): Free[F,B] =
    Stream.runFoldTrace(t)(self, z)(f)

  def runLog: Free[F,Vector[A]] =
    Stream.runFold(self, Vector.empty[A])(_ :+ _)

  def tee[F2[_],B,C](s2: Stream[F2,B])(f: Tee[A,B,C])(implicit S: Sub1[F,F2]): Stream[F2,C] =
    pipe2v(s2)(fs2.tee.covary(f))

  def translate[G[_]](u: F ~> G): Stream[G,A] = Stream.translate(self)(u)

  def noneTerminate: Stream[F,Option[A]] =
    Stream.noneTerminate(self)

  @deprecated("renamed to noneTerminate", "0.9")
  def terminated: Stream[F,Option[A]] =
    Stream.noneTerminate(self)

  @deprecated("use `pipe2` or `pipe2v`, which now subsumes the functionality of `wye`", "0.9")
  def wye[F2[_],B,C](s2: Stream[F2,B])(f: (Stream[F2,A], Stream[F2,B]) => Stream[F2,C])(implicit S: Sub1[F,F2])
  : Stream[F2,C] = pipe2v(s2)(f)


  /** Alias for `[[tee.zip]](self, s2)`. */
  def zip[F2[_], B](s2: Stream[F2,B])(implicit S:Sub1[F,F2]): Stream[F2, (A, B)] =
    fs2.tee.zip.apply(Sub1.substStream(self), s2)

  /** Alias for `[[tee.zipWith]](f)(self, s2)`. */
  def zipWith[F2[_], B, C](s2: Stream[F2,B])(f: (A, B) => C)(implicit S:Sub1[F,F2]): Stream[F2, C] =
    fs2.tee.zipWith(f).apply(Sub1.substStream(self), s2)
}
