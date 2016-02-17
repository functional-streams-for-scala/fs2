package fs2

import fs2.util.{RealSupertype,Sub1,Task}

/** Various derived operations that are mixed into the `Stream` companion object. */
private[fs2]
trait StreamDerived { self: fs2.Stream.type =>

  def apply[F[_],W](a: W*): Stream[F,W] = self.chunk(Chunk.seq(a))

  def pull[F[_],F2[_],A,B](s: Stream[F,A])(using: Handle[F,A] => Pull[F2,B,Any])(implicit S: Sub1[F,F2])
  : Stream[F2,B] =
    Pull.run { Sub1.substPull(open(s)) flatMap (h => Sub1.substPull(using(h))) }

  def repeatPull[F[_],A,B](s: Stream[F,A])(using: Handle[F,A] => Pull[F,B,Handle[F,A]])
  : Stream[F,B] =
    pull(s)(Pull.loop(using))

  /**
   * Sequence the stream of `Pull` actions produced by mapping `f` over `s`.
   * Unlike `[[Pull.output]](s flatMap (f andThen _.run))`, which runs
   * finalizers after each sequenced `Pull` action, `traversePull(s)(f)`
   * produces a single `Pull` scope as its result. It is useful if the inner
   * pulls each make use of some shared resource that should only be finalized
   * when the stream `s` completes.
   */
  def traversePull[F[_],A,B](s: Stream[F,A])(f: A => Pull[F,B,Any]): Pull[F,B,Unit] = {
    def go(h: Handle[F,A]): Pull[F,B,Unit] =
      h.receive1 { case hd #: tl => f(hd) >> go(tl) }
    s.open.flatMap(go)
  }

  /** Alias for `[[traversePull]](s)(identity)`. */
  def sequencePull[F[_],A,B](s: Stream[F,Pull[F,A,Any]]): Pull[F,A,Unit] =
    traversePull(s)(identity)

  def repeatEval[F[_],A](a: F[A]): Stream[F,A] = Stream.eval(a).repeat

  def repeatPull2[F[_],A,B,C](s: Stream[F,A], s2: Stream[F,B])(
    using: (Handle[F,A], Handle[F,B]) => Pull[F,C,(Handle[F,A],Handle[F,B])])
  : Stream[F,C] =
    s.open.flatMap { s => s2.open.flatMap { s2 => Pull.loop(using.tupled)((s,s2)) }}.run

  def await1Async[F[_],A](h: Handle[F,A])(implicit F: Async[F]): Pull[F, Nothing, AsyncStep1[F,A]] =
    h.awaitAsync map { _ map { _.map {
      case Step(hd, tl) => hd.uncons match {
        case None => Step(None, tl)
        case Some((h,hs)) => Step(Some(h), tl.push(hs))
      }}}
    }

  def terminated[F[_],A](p: Stream[F,A]): Stream[F,Option[A]] =
    p.map(Some(_)) ++ emit(None)

  def drain[F[_],A](p: Stream[F,A]): Stream[F,Nothing] =
    p flatMap { _ => empty }

  def onComplete[F[_],A](p: Stream[F,A], regardless: => Stream[F,A]): Stream[F,A] =
    onError(append(p, mask(regardless))) { err => append(mask(regardless), fail(err)) }

  def mask[F[_],A](a: Stream[F,A]): Stream[F,A] =
    onError(a)(_ => empty)

  def map[F[_],A,B](a: Stream[F,A])(f: A => B): Stream[F,B] =
    Stream.map(a)(f)

  def emit[F[_],A](a: A): Stream[F,A] = chunk(Chunk.singleton(a))

  @deprecated("use Stream.emits", "0.9")
  def emitAll[F[_],A](as: Seq[A]): Stream[F,A] = chunk(Chunk.seq(as))

  def emits[F[_],W](a: Seq[W]): Stream[F,W] = chunk(Chunk.seq(a))

  def force[F[_],A](f: F[Stream[F, A]]): Stream[F,A] =
    flatMap(eval(f))(p => p)

  def eval_[F[_],A](fa: F[A]): Stream[F,Nothing] =
    flatMap(eval(fa)) { _ => empty }

  /**
    * The infinite `Process`, always emits `a`.
    * If for performance reasons it is good to emit `a` in chunks,
    * specify size of chunk by `chunkSize` parameter
    */
  def constant[F[_],W](w: W, chunkSize: Int = 1): Stream[F, W] =
    emits(List.fill(chunkSize)(w)) ++ constant(w, chunkSize)

  def push1[F[_],A](h: Handle[F,A])(a: A): Handle[F,A] =
    push(h)(Chunk.singleton(a))

  def peek[F[_],A](h: Handle[F,A]): Pull[F, Nothing, Step[Chunk[A], Handle[F,A]]] =
    h.await flatMap { case hd #: tl => Pull.pure(hd #: tl.push(hd)) }

  def await1[F[_],A](h: Handle[F,A]): Pull[F, Nothing, Step[A, Handle[F,A]]] =
    h.await flatMap { case Step(hd, tl) => hd.uncons match {
      case None => await1(tl)
      case Some((h,hs)) => Pull.pure(Step(h, tl push hs))
    }}

  def peek1[F[_],A](h: Handle[F,A]): Pull[F, Nothing, Step[A, Handle[F,A]]] =
    h.await1 flatMap { case hd #: tl => Pull.pure(hd #: tl.push1(hd)) }


  /** Lazily produce the range `[start, stopExclusive)`. If you want to produce the sequence in one chunk, instead of lazily, use `emitAll(start until stopExclusive)`.  */
  def range(start: Int, stopExclusive: Int, by: Int = 1): Stream[Pure,Int] =
    unfold(start)(i => if (i < stopExclusive) Some((i + by, Chunk.seq(Seq(i + by)))) else None)

  /** Produce a (potentially infinite) source from an unfold. */
  def unfold[S, A](s0: S)(f: S => Option[(S,Chunk[A])]): Stream[Pure,A] = {
    def go(s: S):  Stream[Pure,A] =
      f(s) match {
        case Some((ns, chunk)) => Stream.chunk(chunk) ++ go(ns)
        case None => Stream.empty
      }
    Stream.suspend(go(s0))
  }


  implicit class HandleOps[+F[_],+A](h: Handle[F,A]) {
    def push[A2>:A](c: Chunk[A2])(implicit A2: RealSupertype[A,A2]): Handle[F,A2] =
      self.push(h: Handle[F,A2])(c)
    def push1[A2>:A](a: A2)(implicit A2: RealSupertype[A,A2]): Handle[F,A2] =
      self.push1(h: Handle[F,A2])(a)
    def #:[H](hd: H): Step[H, Handle[F,A]] = Step(hd, h)
    def await: Pull[F, Nothing, Step[Chunk[A], Handle[F,A]]] = self.await(h)
    def await1: Pull[F, Nothing, Step[A, Handle[F,A]]] = self.await1(h)
    def awaitNonempty: Pull[F, Nothing, Step[Chunk[A], Handle[F,A]]] = Pull.awaitNonempty(h)
    def echo1: Pull[F,A,Handle[F,A]] = Pull.echo1(h)
    def echoChunk: Pull[F,A,Handle[F,A]] = Pull.echoChunk(h)
    def peek: Pull[F, Nothing, Step[Chunk[A], Handle[F,A]]] = self.peek(h)
    def peek1: Pull[F, Nothing, Step[A, Handle[F,A]]] = self.peek1(h)
    def awaitAsync[F2[_],A2>:A](implicit S: Sub1[F,F2], F2: Async[F2], A2: RealSupertype[A,A2]):
      Pull[F2, Nothing, AsyncStep[F2,A2]] = self.awaitAsync(Sub1.substHandle(h))
    def await1Async[F2[_],A2>:A](implicit S: Sub1[F,F2], F2: Async[F2], A2: RealSupertype[A,A2]):
      Pull[F2, Nothing, AsyncStep1[F2,A2]] = self.await1Async(Sub1.substHandle(h))
    def covary[F2[_]](implicit S: Sub1[F,F2]): Handle[F2,A] = Sub1.substHandle(h)
  }

  implicit class HandleInvariantEffectOps[F[_],+A](h: Handle[F,A]) {
    def invAwait1Async[A2>:A](implicit F: Async[F], A2: RealSupertype[A,A2]):
      Pull[F, Nothing, AsyncStep1[F,A2]] = self.await1Async(h)
    def invAwaitAsync[A2>:A](implicit F: Async[F], A2: RealSupertype[A,A2]):
      Pull[F, Nothing, AsyncStep[F,A2]] = self.awaitAsync(h)
    def receive1[O,B](f: Step[A,Handle[F,A]] => Pull[F,O,B]): Pull[F,O,B] = h.await1.flatMap(f)
    def receive[O,B](f: Step[Chunk[A],Handle[F,A]] => Pull[F,O,B]): Pull[F,O,B] = h.await.flatMap(f)
  }

  implicit class StreamInvariantOps[F[_],A](s: Stream[F,A]) {
    def through[B](f: Stream[F,A] => Stream[F,B]): Stream[F,B] = f(s)
    def to[B](f: Stream[F,A] => Stream[F,Unit]): Stream[F,Unit] = f(s)
    def pull[B](using: Handle[F,A] => Pull[F,B,Any]): Stream[F,B] =
      Stream.pull(s)(using)
    def pull2[B,C](s2: Stream[F,B])(using: (Handle[F,A], Handle[F,B]) => Pull[F,C,Any]): Stream[F,C] =
      s.open.flatMap { h1 => s2.open.flatMap { h2 => using(h1,h2) }}.run
    def pipe2[B,C](s2: Stream[F,B])(f: (Stream[F,A], Stream[F,B]) => Stream[F,C]): Stream[F,C] =
      f(s,s2)
    def repeatPull[B](using: Handle[F,A] => Pull[F,B,Handle[F,A]]): Stream[F,B] =
      Stream.repeatPull(s)(using)
    def repeatPull2[B,C](s2: Stream[F,B])(using: (Handle[F,A],Handle[F,B]) => Pull[F,C,(Handle[F,A],Handle[F,B])]): Stream[F,C] =
      Stream.repeatPull2(s,s2)(using)
  }

  implicit class StreamPureOps[+A](s: Stream[Pure,A]) {
    def toList: List[A] =
      s.covary[Task].runFold(List.empty[A])((b, a) => a :: b).run.run.reverse
    def toVector: Vector[A] = s.covary[Task].runLog.run.run
  }

  implicit def covaryPure[F[_],A](s: Stream[Pure,A]): Stream[F,A] = s.covary[F]
}
