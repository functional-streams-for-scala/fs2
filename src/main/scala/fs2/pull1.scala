package fs2

import Step._

private[fs2] trait pull1 {
  import Stream.Handle

  // nb: methods are in alphabetical order

  /** Await the next available `Chunk` from the `Handle`. The `Chunk` may be empty. */
  def await[F[_],I]: Handle[F,I] => Pull[F,Nothing,Step[Chunk[I],Handle[F,I]]] =
    _.await

  /** Await a single element from the `Handle`. */
  def await1[F[_],I]: Handle[F,I] => Pull[F,Nothing,Step[I,Handle[F,I]]] =
    _.await1

  /** Like `await`, but return a `Chunk` of no more than `maxChunkSize` elements. */
  def awaitLimit[F[_],I](maxChunkSize: Int)
    : Handle[F,I] => Pull[F,Nothing,Step[Chunk[I],Handle[F,I]]]
    = _.await.map { s =>
        if (s.head.size <= maxChunkSize) s
        else s.head.take(maxChunkSize) #: s.tail.push(s.head.drop(maxChunkSize))
      }

  /** Return a `List[Chunk[I]]` from the input whose combined size is exactly `n`. */
  def awaitN[F[_],I](n: Int)
    : Handle[F,I] => Pull[F,Nothing,Step[List[Chunk[I]],Handle[F,I]]]
    = h =>
        if (n <= 0) Pull.pure(List() #: h)
        else for {
          hd #: tl <- awaitLimit(n)(h)
          hd2 #: tl <- awaitN(n - hd.size)(tl)
        } yield (hd :: hd2) #: tl

  /** Await the next available chunk from the input, or `None` if the input is exhausted. */
  def awaitOption[F[_],I]: Handle[F,I] => Pull[F,Nothing,Option[Step[Chunk[I],Handle[F,I]]]] =
    h => h.await.map(Some(_)) or Pull.pure(None)

  /** Await the next available element from the input, or `None` if the input is exhausted. */
  def await1Option[F[_],I]: Handle[F,I] => Pull[F,Nothing,Option[Step[I,Handle[F,I]]]] =
    h => h.await1.map(Some(_)) or Pull.pure(None)

  /** Finds the first element from the input for which the given partial 
   function `pf` is defined, and applies the partial function to it. */
  def collectFirst[F[_],I,O](pf: PartialFunction[I,O]): Handle[F,I] => Pull[F,Nothing,Option[O]] =
    _.await.optional flatMap {
      case Some(chunk #: h) => chunk.indexWhere(pf.isDefinedAt) match {
        case Some(i) => Pull.pure(Some(pf(chunk(i))))
        case None    => collectFirst(pf)(h)
      }
      case None             => Pull.pure(None)
    }
    
  /** Copy the next available chunk to the output. */
  def copy[F[_],I]: Handle[F,I] => Pull[F,I,Handle[F,I]] =
    receive { case chunk #: h => Pull.output(chunk) >> Pull.pure(h) }

  /** Copy the next available element to the output. */
  def copy1[F[_],I]: Handle[F,I] => Pull[F,I,Handle[F,I]] =
    receive1 { case hd #: h => Pull.output1(hd) >> Pull.pure(h) }

  /** Write all inputs to the output of the returned `Pull`. */
  def echo[F[_],I]: Handle[F,I] => Pull[F,I,Nothing] =
    receive { case chunk #: h => Pull.output(chunk) >> echo(h) }

  /** Like `[[awaitN]]`, but leaves the buffered input unconsumed. */
  def fetchN[F[_],I](n: Int): Handle[F,I] => Pull[F,Nothing,Handle[F,I]] =
    h => awaitN(n)(h) map { case buf #: h => buf.reverse.foldLeft(h)(_ push _) }

  /** Return the last element of the input `Handle`, if nonempty. */
  def last[F[_],I]: Handle[F,I] => Pull[F,Nothing,Option[I]] = {
    def go(prev: Option[I]): Handle[F,I] => Pull[F,Nothing,Option[I]] =
      h => h.await.optional.flatMap {
        case None => Pull.pure(prev)
        case Some(c #: h) => go(c.foldLeft(prev)((_,i) => Some(i)))(h)
      }
    go(None)
  }

  /**
   * Like `[[await]]`, but runs the `await` asynchronously. A `flatMap` into
   * inner `Pull` logically blocks until this await completes.
   */
  def prefetch[F[_]:Async,I](h: Handle[F,I]): Pull[F,Nothing,Pull[F,Nothing,Handle[F,I]]] =
    h.awaitAsync map { fut =>
      fut.force flatMap { p =>
        p map { case hd #: h => h push hd }
      }
    }

  /**
   * Like `[[prefetch]]`, but continue fetching asynchronously as long as the number of
   * prefetched elements is less than `n`.
   */
  def prefetchN[F[_]:Async,I](n: Int)(h: Handle[F,I]): Pull[F,Nothing,Pull[F,Nothing,Handle[F,I]]] =
    if (n <= 0) Pull.pure(Pull.pure(h))
    else prefetch(h) map { p =>
      for {
        s <- p.flatMap(awaitLimit(n)).optional
        tl <- s match {
          case Some(hd #: h) => prefetchN(n - hd.size)(h) flatMap { tl => tl.map(_ push hd) }
          case None => Pull.pure(Handle.empty)
        }
      } yield tl
    }

  /** Apply `f` to the next available `Chunk`. */
  def receive[F[_],I,O,R](f: Step[Chunk[I],Handle[F,I]] => Pull[F,O,R]): Handle[F,I] => Pull[F,O,R] =
    _.await.flatMap(f)

  /** Apply `f` to the next available element. */
  def receive1[F[_],I,O,R](f: Step[I,Handle[F,I]] => Pull[F,O,R]): Handle[F,I] => Pull[F,O,R] =
    _.await1.flatMap(f)
}
