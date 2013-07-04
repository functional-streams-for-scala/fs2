package scalaz.stream

import scala.collection.immutable.{IndexedSeq,SortedMap,Queue,Vector}

import scalaz.{Catchable,Monad,MonadPlus,Monoid,Nondeterminism,Semigroup}
import scalaz.concurrent.Task
import scalaz.Leibniz.===
import scalaz.{\/,-\/,\/-,~>,Leibniz}
import \/._

/** 
 * A `Process[F,O]` represents a stream of `O` values which can interleave 
 * external requests to evaluate expressions of the form `F[A]`. It takes
 * the form of a state machine with three possible states: `Emit`, which 
 * indicates that `h` should be emitted to the output stream, `Halt`,
 * which indicates that the `Process` is finished making requests and 
 * emitting values to the output stream, and `Await` which asks the driver 
 * to evaluate some `F[A]` and resume processing once the result is available. 
 * See the constructor definitions in the `Process` companion object.
 */
sealed trait Process[+F[_],+O] {
  
  import Process._

  /** Transforms the output values of this `Process` using `f`. */
  final def map[O2](f: O => O2): Process[F,O2] = this match {
    case Await(req,recv,fb,c) => 
      Await[F,Any,O2](req, recv andThen (_ map f), fb map f, c map f) 
    case Emit(h, t) => Emit[F,O2](h map f, t map f)
    case Halt => Halt
  }

  /** 
   * Generate a `Process` dynamically for each output of this `Process`, and
   * sequence these processes using `append`. 
   */
  final def flatMap[F2[x]>:F[x], O2](f: O => Process[F2,O2]): Process[F2,O2] = this match {
    case Halt => Halt
    case Emit(o, t) => 
      if (o.isEmpty) t.flatMap(f)
      else f(o.head) ++ emitSeq(o.tail, t).flatMap(f)
    case Await(req,recv,fb,c) => 
      Await(req, recv andThen (_ flatMap f), fb flatMap f, c flatMap f)
  }
  
  /** 
   * Run this `Process`, then, if it halts without an error, run `p2`. 
   * Note that `p2` is appended to the `fallback` argument of any `Await`
   * produced by this `Process`. If this is not desired, use `then`. 
   */
  final def append[F2[x]>:F[x], O2>:O](p2: => Process[F2,O2]): Process[F2,O2] = this match {
    case Halt => p2
    case Emit(h, t) => emitSeq(h, t append p2)
    case Await(req,recv,fb,c) => 
      Await(req, recv andThen (_ append p2), fb append p2, c)
  }

  /** Operator alias for `append`. */
  final def ++[F2[x]>:F[x], O2>:O](p2: => Process[F2,O2]): Process[F2,O2] = 
    this append p2

  /** 
   * Run this `Process`, then, if it self-terminates, run `p2`. 
   * This differs from `append` in that `p2` is not consulted if this
   * `Process` terminates due to the input being exhausted. That is, 
   * we do not modify the `fallback` arguments to any `Await` produced
   * by this `Process`.
   */
  final def then[F2[x]>:F[x],O2>:O](p2: => Process[F2,O2]): Process[F2,O2] = this match {
    case Halt => p2
    case Emit(h, t) => emitSeq(h, t then p2)
    case Await(req,recv,fb,c) => 
      Await(req, recv andThen (_ then p2), fb, c)
  }

  /** 
   * Removes all emitted elements from the front of this `Process`.
   * The second argument returned by this method is guaranteed to be 
   * an `Await` or a `Halt`--if there are multiple `Emit`s at the
   * front of this process, the sequences are concatenated together.
   * 
   * If this `Process` does not begin with an `Emit`, returns the empty
   * sequence along with `this`. 
   */
  final def unemit: (Seq[O], Process[F,O]) = {
    @annotation.tailrec
    def go(acc: Seq[O], cur: Process[F,O]): (Seq[O], Process[F,O]) = 
      cur match {
        case Emit(h, t) => go(acc ++ h, t) 
        case _ => (acc, cur)
      }
    go(Seq(), this)
  }

  private[stream] final def unconsAll: Process[F, (Seq[O], Process[F,O])] = this match {
    case Halt => Halt
    case Emit(h, t) => if (h.isEmpty) t.unconsAll else emit((h,t))
    case Await(req,recv,fb,c) => await(req)(recv andThen (_.unconsAll), fb.unconsAll, c.unconsAll) 
  }
  private[stream] final def uncons: Process[F, (O, Process[F,O])] = 
    unconsAll map { case (h,t) => (h.head, emitAll(h.tail) ++ t) }
    
  /** 
   * Run this process until it halts, then run it again and again, as
   * long as no errors occur. 
   */
  final def repeat[F2[x]>:F[x],O2>:O]: Process[F2,O2] = {
    def go(cur: Process[F,O]): Process[F,O] = cur match {
      case Halt => go(this)
      case Await(req,recv,fb,c) => Await(req, recv andThen go, fb, c)
      case Emit(h, t) => emitSeq(h, go(t))
    }
    go(this)
  }

  /**
   * Halt this process, but give it an opportunity to run any requests it has 
   * in the `cleanup` argument of its next `Await`.
   */
  @annotation.tailrec
  final def kill: Process[F,Nothing] = this match {
    case Await(req,recv,fb,c) => c.drain 
    case Emit(h, t) => t.kill
    case Halt => Halt
  }

  /** 
   * Switch to the `fallback` case of the _next_ `Await` issued by this `Process`.
   */
  final def fallback: Process[F,O] = this match {
    case Await(req,recv,fb,c) => fb 
    case Emit(h, t) => emitSeq(h, t.fallback)
    case Halt => Halt
  }

  /**
   * Replaces the outermost `fallback` parameter of `Await`.
   */
  final def orElse[F2[x]>:F[x],O2>:O](fallback: Process[F2,O2]): Process[F2,O2] = this match {
    case Await(req,recv,fb,c) => Await(req,recv,fallback,c)
    case Emit(h, t) => Emit(h, t.orElse(fallback))
    case Halt => Halt
  }

  /**
   * Replaces the outermost `fallback` parameter of `Await`.
   */
  final def onFallback[F2[x]>:F[x],O2>:O](fallback: Process[F2,O2]): Process[F2,O2] = 
    orElse(fallback)

  /**
   * Replaces the outermost `cleanup` parameter of `Await`.
   */
  final def onError[F2[x]>:F[x],O2>:O](handler: Process[F2,O2]): Process[F2,O2] = this match {
    case Await(req,recv,fb,c) => Await(req,recv,fb,handler)
    case Emit(h, t) => Emit(h, t.onError(handler))
    case Halt => Halt
  }

  /**
   * Replaces the outermost `fallback` and `cleanup` parameters of `Await`.
   */
  final def onFallbackOrError[F2[x]>:F[x],O2>:O](handler: Process[F2,O2]): Process[F2,O2] = this match {
    case Await(req,recv,fb,c) => Await(req,recv,handler,handler)
    case Emit(h, t) => Emit(h, t.onFallbackOrError(handler))
    case Halt => Halt
  }

  /**
   * Switch to the `fallback` case of _all_ subsequent awaits.
   */
  final def disconnect: Process[Nothing,O] = this match {
    case Await(req,recv,fb,c) => fb.disconnect 
    case Emit(h, t) => emitSeq(h, t.disconnect) 
    case Halt => Halt
  }

  /** 
   * Switch to the `cleanup` case of the next `Await` issued by this `Process`.
   */
  final def cleanup: Process[F,O] = this match {
    case Await(req,recv,fb,c) => fb 
    case Halt => Halt
    case Emit(h, t) => emitSeq(h, t.cleanup)
  }

  /**
   * Switch to the `cleanup` case of _all_ subsequent awaits.
   */
  final def hardDisconnect: Process[Nothing,O] = this match {
    case Await(req,recv,fb,c) => c.hardDisconnect 
    case Halt => Halt
    case Emit(h, t) => emitSeq(h, t.hardDisconnect) 
  }

  /** 
   * Remove any leading emitted values from this `Process`. 
   */
  @annotation.tailrec
  final def trim: Process[F,O] = this match {
    case Emit(h, t) => t.trim
    case _ => this
  }

  /** Correctly typed deconstructor for `Await`. */
  def asAwait: Option[(F[AwaitF.Req], AwaitF.Req => Process[F,O], Process[F,O], Process[F,O])] = 
    this match {
      case Await(req,recv,fb,c) => Some((req.asInstanceOf[F[AwaitF.Req]],recv,fb,c))
      case _ => None
    }

  /**
   * Ignores output of this `Process`. A drained `Process` will never `Emit`.  
   */
  def drain: Process[F,Nothing] = this match {
    case Halt => Halt
    case Emit(h, t) => t.drain
    case Await(req,recv,fb,c) => Await(
      req, recv andThen (_ drain), 
      fb.drain, c.drain)
  }

  /** 
   * Feed the output of this `Process` as input of `p2`. The implementation  
   * will fuse the two processes, so this process will only generate
   * values as they are demanded by `p2`. If `p2` signals termination, `this`
   * is killed using `kill`, giving it the opportunity to clean up. 
   */
  final def pipe[O2](p2: Process1[O,O2]): Process[F,O2] =
    // Since `Process1[O,O2] <: Tee[O,Any,O2]`, but it is a `Tee` that
    // never reads from its right input, we can define this in terms of `tee`!
    (this tee Halt)(p2)

  /** Operator alias for `pipe`. */
  final def |>[O2](p2: Process1[O,O2]): Process[F,O2] = 
    this pipe p2

  /** 
   * Use a `Tee` to interleave or combine the outputs of `this` and
   * `p2`. This can be used for zipping, interleaving, and so forth.
   * Nothing requires that the `Tee` read elements from each 
   * `Process` in lockstep. It could read fifty elements from one 
   * side, then two elements from the other, then combine or
   * interleave these values in some way, etc.
   * 
   * If at any point the `Tee` awaits on a side that has halted, 
   * we gracefully kill off the other side, then halt.
   */ 
  final def tee[F2[x]>:F[x],O2,O3](p2: Process[F2,O2])(t: Tee[O,O2,O3]): Process[F2,O3] = 
    // Somewhat evil: we are passing `null` for the `Nondeterminism` and `Catchable` here, 
    // safe because the types guarantee that a `Tee` cannot issue a `Both` 
    // request that would result in the `Nondeterminism` instance being used.  
    // This lets us reuse a single function, `wye`, as the implementation for
    // both `tee` and `pipe`!
    (this wye p2)(t)(null,null)

  /** 
   * Like `tee`, but we allow the `Wye` to read nondeterministically 
   * from both sides at once, using the supplied `Nondeterminism` 
   * instance.
   * 
   * If `y` is in the state of awaiting `Both`, this implementation 
   * will continue feeding `y` until either it halts or _both_ sides
   * halt. 
   * 
   * If `y` is in the state of awaiting `L`, and the left 
   * input has halted, we halt. Likewise for the right side. 
   * 
   * For as long as `y` permits it, this implementation will _always_ 
   * feed it any leading `Emit` elements from either side before issuing
   * new `F` requests. More sophisticated chunking and fairness 
   * policies do not belong here, but should be built into the `Wye`
   * and/or its inputs. 
   */ 
  final def wye[F2[x]>:F[x],O2,O3](p2: Process[F2,O2])(y: Wye[O,O2,O3])(implicit F2: Nondeterminism[F2], E: Catchable[F2]): Process[F2,O3] = {
    import F2.monadSyntax._
    try y match {
      case Halt => this.kill ++ p2.kill
      case Emit(h,y2) => 
        Emit(h, this.wye(p2)(y2))
      case Await(_,_,_,_) => 
        val u1 = this.unemit; val h1 = u1._1; val t1 = u1._2
        val u2 = p2.unemit; val h2 = u2._1; val t2 = u2._2
        val ready = These.align(h1, h2)
        val (y2, ready2) = if (ready.isEmpty) (y, ready) else { // we have some values queued up, try feeding them to the Wye
          y.feed(ready) { // .feed is a tail recursive function
            case Await(req, recv, fb, c) => (req.tag: @annotation.switch) match {
              case 0 => // Left 
                val recv_ = recv.asInstanceOf[O => Wye[O,O2,O3]]
                (e: These[O,O2]) => e match { 
                  case These.This(o) => (None, Some(recv_(o)))
                  case These.That(_) => (None, None)
                  case These.Both(o,o2) => (Some(These.That(o2)), Some(recv_(o)))
                }
              case 1 => // Right 
                val recv_ = recv.asInstanceOf[O2 => Wye[O,O2,O3]]
                (e: These[O,O2]) => e match { 
                  case These.This(_) => (None, None)
                  case These.That(o2) => (None, Some(recv_(o2)))
                  case These.Both(o,o2) => (Some(These.This(o)), Some(recv_(o2)))
                }
              case 2 => // Both 
                val recv_ = recv.asInstanceOf[These[O,O2] => Wye[O,O2,O3]]
                (e: These[O,O2]) => (None, Some(recv_(e)))
            }
            case _ => _ => (None, None)
          }
        }
        val (h1Next, h2Next) = These.unalign(ready2)
        val (thisNext_, p2Next_) = (emitSeq(h1Next, t1), emitSeq(h2Next, t2))
        val thisNext = thisNext_.asInstanceOf[Process[F2,O]] 
        val p2Next = p2Next_.asInstanceOf[Process[F2,O2]]
        y2 match {
          case Await(req,_,_,_) => (req.tag: @annotation.switch) match {
            case 0 => // Left
              thisNext match {
                case AwaitF(reqL,recvL,fbL,cL) => // unfortunately, casts required here
                  await(reqL)(recvL andThen (_.wye(p2Next)(y2)), fbL.wye(p2Next)(y2), cL.wye(p2Next)(y2))
                case Halt => p2Next.kill ++ y2.disconnect
                case e@Emit(_,_) => thisNext.wye(p2Next)(y2) 
              }
            case 1 => // Right 
              p2Next match {
                case AwaitF(reqR,recvR,fbR,cR) => // unfortunately, casts required here
                  // in the event of a fallback or error, `y` will end up running the right's fallback/cleanup
                  // actions on the next cycle - it still needs that value on the right and will run any awaits
                  // to try to obtain that value!
                  await(reqR)(recvR andThen (p2 => thisNext.wye[F2,O2,O3](p2)(y2)), 
                               thisNext.wye(fbR)(y2), 
                               thisNext.wye(cR)(y2))
                case Halt => thisNext.kill ++ y2.disconnect
                case e@Emit(_,_) => thisNext.wye(p2Next)(y2) 
              }
            case 2 => thisNext match { // Both
              case Halt => p2Next |> y2.detachL
              case AwaitF(reqL, recvL, fbL, cL) => p2Next match {
                case Halt => 
                  Await(reqL, recvL andThen (_.wye(p2Next)(y2)), fbL.wye(p2Next)(y2), cL.wye(p2Next)(y2))
                // If both sides are in the Await state, we use the
                // Nondeterminism instance to request both sides
                // concurrently.
                case AwaitF(reqR, recvR, fbR, cR) =>
                  wrap(
                    F2.choose(
                      E.attempt(reqL.map(recvL)).map {
                        _.fold({ case End => fbL 
                                 case t: Throwable => cL ++ (throw t) 
                               }, 
                               e => e)
                      },
                      E.attempt(reqR.map(recvR)).map {
                        _.fold({ case End => fbR 
                                 case t: Throwable => cR ++ (throw t) 
                               }, 
                               e => e)
                      }
                    )
                  ).flatMap { 
                    _.fold(l => wrap(l._2).flatMap(p2 => l._1.wye(p2)(y2)),
                           r => wrap(r._1).flatMap(t => t.wye(r._2)(y2)))
                  }
              }
            }
          }
          case _ => thisNext.wye(p2Next)(y2)
        }
    }
    catch { case e: Throwable => this.kill ++ p2.kill ++ (throw e) }
  }

  /** Translate the request type from `F` to `G`, using the given polymorphic function. */
  def translate[G[_]](f: F ~> G): Process[G,O] = this match {
    case Emit(h, t) => Emit(h, t.translate(f))
    case Halt => Halt
    case Await(req, recv, fb, c) => 
      Await(f(req), recv andThen (_ translate f), fb translate f, c translate f)
  }

  /** 
   * Catch exceptions produced by this `Process`. The resulting `Process` halts after the
   * the first emitted failure, though this can handled and resumed by a subsequent `Process`. 
   */ 
  def attempt[F2[x]>:F[x]](implicit F: Catchable[F2]): Process[F2, Throwable \/ O] = 
  this match {
    case Emit(h, t) => Emit(h map (\/-(_)), t.attempt[F2])
    case Halt => Halt
    case Await(req, recv, fb, c) => 
      await(F.attempt(req))(
        _.fold(err => emit(-\/(err)), recv andThen (_.attempt[F2])), 
        fb.attempt[F2], c.attempt[F2])
  }

  /** 
   * Catch some of the exceptions generated by this `Process`, rethrowing any
   * not handled by the given `PartialFunction`. 
   */
  def handle[F2[x]>:F[x],O2>:O](f: PartialFunction[Throwable,O2])(implicit F: Catchable[F2]) = 
    attempt(F) |> (process1.lift { 
      case \/-(o) => o
      case -\/(err) => f.lift(err).getOrElse(throw err) })

  /** 
   * Map over this `Process` to produce a stream of `F`-actions, 
   * then evaluate these actions. 
   */
  def evalMap[F2[x]>:F[x],O2](f: O => F2[O2]): Process[F2,O2] = 
    map(f).eval

  /** 
   * Map over this `Process` to produce a stream of `F`-actions, 
   * then evaluate these actions, returning results in whatever
   * order they come back. `bufSize` controls the maximum number
   * of evaluations that can be queued at any one time. 
   */
  def gatherMap[F2[x]>:F[x],O2](bufSize: Int)(f: O => F2[O2])(
                                implicit F: Nondeterminism[F2]): Process[F2,O2] = 
    map(f).gather(bufSize)
 
  /** 
   * Collect the outputs of this `Process[F,O]`, given a `Monad[F]` in
   * which we can catch exceptions. This function is not tail recursive and
   * relies on the `Monad[F]` to ensure stack safety. 
   */
  final def collect[F2[x]>:F[x], O2>:O](implicit F: Monad[F2], C: Catchable[F2]): F2[IndexedSeq[O2]] = {
    def go(cur: Process[F2,O2], acc: IndexedSeq[O2]): F2[IndexedSeq[O2]] =
      cur match {
        case Emit(h,t) => go(t.asInstanceOf[Process[F2,O2]], acc ++ h.asInstanceOf[Seq[O2]]) 
        case Halt => F.point(acc)
        case Await(req,recv,fb,c) => 
           F.bind (C.attempt(req.asInstanceOf[F2[AnyRef]])) {
             _.fold(
               { case End => go(fb.asInstanceOf[Process[F2,O2]], acc)
                 case err => c match {
                   case Halt => C.fail(err)
                   case _ => go(c.asInstanceOf[Process[F2,O2]] ++ wrap(C.fail(err)), IndexedSeq()) 
                 }
               }, o => go(recv.asInstanceOf[AnyRef => Process[F2,O2]](o), acc))
           }
      }
    go(this, IndexedSeq())
  }

  /** Run this `Process` solely for its final emitted value, if one exists. */
  final def runLast[F2[x]>:F[x], O2>:O](implicit F: Monad[F2], C: Catchable[F2]): F2[Option[O2]] = 
    F.map(this.last.collect[F2,O2])(_.lastOption)

  /** Run this `Process` solely for its final emitted value, if one exists, using `o2` otherwise. */
  final def runLastOr[F2[x]>:F[x], O2>:O](o2: => O2)(implicit F: Monad[F2], C: Catchable[F2]): F2[O2] = 
    F.map(this.last.collect[F2,O2])(_.lastOption.getOrElse(o2))

  /** Run this `Process`, purely for its effects. */
  final def run[F2[x]>:F[x]](implicit F: Monad[F2], C: Catchable[F2]): F2[Unit] = 
    F.void(drain.collect(F, C))

  /** Alias for `this |> process1.buffer(n)`. */
  def buffer(n: Int): Process[F,O] = 
    this |> process1.buffer(n)

  /** Alias for `this |> process1.bufferBy(f)`. */
  def bufferBy(f: O => Boolean): Process[F,O] = 
    this |> process1.bufferBy(f)

  /** Alias for `this |> process1.bufferBy(f)`. */
  def bufferAll: Process[F,O] = 
    this |> process1.bufferAll

  /** Ignores the first `n` elements output from this `Process`. */
  def drop(n: Int): Process[F,O] = 
    this |> processes.drop[O](n)

  /** Ignores elements from the output of this `Process` until `f` tests false. */
  def dropWhile(f: O => Boolean): Process[F,O] = 
    this |> processes.dropWhile(f)

  /** Skips any output elements not matching the predicate. */
  def filter(f: O => Boolean): Process[F,O] = 
    this |> processes.filter(f)

  /** Connect this `Process` to `process1.fold(b)(f)`. */
  def fold[B](b: B)(f: (B,O) => B): Process[F,B] = 
    this |> process1.fold(b)(f)
  /** Halts this `Process` after emitting `n` elements. */
  def take(n: Int): Process[F,O] = 
    this |> processes.take[O](n)

  /** Halts this `Process` after emitting 1 element. */
  def once: Process[F,O] = take(1)

  /** Skips all elements emitted by this `Process` except the last. */
  def last: Process[F,O] = this |> process1.last

  /** Halts this `Process` as soon as the predicate tests false. */
  def takeWhile(f: O => Boolean): Process[F,O] = 
    this |> processes.takeWhile(f)

  /** Call `tee` with the `zipWith` `Tee[O,O2,O3]` defined in `tee.scala`. */
  def zipWith[F2[x]>:F[x],O2,O3](p2: Process[F2,O2])(f: (O,O2) => O3): Process[F2,O3] = 
    this.tee(p2)(scalaz.stream.tee.zipWith(f))

  /** Call `tee` with the `zip` `Tee[O,O2,O3]` defined in `tee.scala`. */
  def zip[F2[x]>:F[x],O2](p2: Process[F2,O2]): Process[F2,(O,O2)] =
    this.tee(p2)(scalaz.stream.tee.zip)

  /** Nondeterministic version of `zipWith`. */ 
  def yipWith[F2[x]>:F[x],O2,O3](p2: Process[F2,O2])(f: (O,O2) => O3)(
  implicit F: Nondeterminism[F2], E: Catchable[F2]): Process[F2,O3] = 
    this.wye(p2)(scalaz.stream.wye.yipWith(f))

  /** Nondeterministic version of `zip`. */ 
  def yip[F2[x]>:F[x],O2](p2: Process[F2,O2])(
  implicit F: Nondeterminism[F2], E: Catchable[F2]): Process[F2,(O,O2)] = 
    this.wye(p2)(scalaz.stream.wye.yip)
}

object processes extends process1 with tee with wye with io

object Process {
  case class Await[F[_],A,+O] private[stream](
    req: F[A], recv: A => Process[F,O],
    fallback1: Process[F,O] = Halt,
    cleanup1: Process[F,O] = Halt) extends Process[F,O]

  case class Emit[F[_],O] private[stream](
    head: Seq[O], 
    tail: Process[F,O]) extends Process[F,O]

  case object Halt extends Process[Nothing,Nothing]

  object AwaitF {
    trait Req
    def unapply[F[_],O](self: Process[F,O]): 
        Option[(F[Req], Req => Process[F,O], Process[F,O], Process[F,O])] = 
      self.asAwait
  }
  object Await1 {
    private[stream] def unapply[I,O](self: Process1[I,O]): 
        Option[(I => Process1[I,O], Process1[I,O], Process1[I,O])] = self match {
    
      case Await(_,recv,fb,c) => Some((recv.asInstanceOf[I => Process1[I,O]], fb, c))
      case _ => None
    }
  }
  object AwaitL {
    private[stream] def unapply[I,I2,O](self: Wye[I,I2,O]): 
        Option[(I => Wye[I,I2,O], Wye[I,I2,O], Wye[I,I2,O])] = self match {
      case Await(req,recv,fb,c) if req.tag == 0 => Some((recv.asInstanceOf[I => Wye[I,I2,O]], fb, c))
      case _ => None
    }
  }
  object AwaitR {
    private[stream] def unapply[I,I2,O](self: Wye[I,I2,O]): 
        Option[(I2 => Wye[I,I2,O], Wye[I,I2,O], Wye[I,I2,O])] = self match {
      case Await(req,recv,fb,c) if req.tag == 1 => Some((recv.asInstanceOf[I2 => Wye[I,I2,O]], fb, c))
      case _ => None
    }
  }
  object AwaitBoth {
    private[stream] def unapply[I,I2,O](self: Wye[I,I2,O]): 
        Option[(These[I,I2] => Wye[I,I2,O], Wye[I,I2,O], Wye[I,I2,O])] = self match {
      case Await(req,recv,fb,c) if req.tag == 2 => Some((recv.asInstanceOf[These[I,I2] => Wye[I,I2,O]], fb, c))
      case _ => None
    }
  }

  def emitSeq[F[_],O](
      head: Seq[O], 
      tail: Process[F,O] = Halt): Process[F,O] = 
    if (head.isEmpty) tail
    else tail match {
      case Emit(h2,t) => Emit(head ++ h2.asInstanceOf[Seq[O]], t.asInstanceOf[Process[F,O]])
      case _ => Emit(head, tail)
    }

  def await[F[_],A,O](req: F[A])(
      recv: A => Process[F,O] = (a: A) => Halt, 
      fallback: Process[F,O] = Halt,
      cleanup: Process[F,O] = Halt): Process[F,O] = 
    Await(req, recv, fallback, cleanup)

  def apply[O](o: O*): Process[Nothing,O] = 
    emitSeq[Nothing,O](o, Halt)

  /** `Process.range(0,5) == Process(0,1,2,3,4).` */
  def range(start: Int, stopExclusive: Int): Process[Nothing,Int] = 
    emitSeq(Stream.range(start,stopExclusive))

  /** Emit a single value, then `Halt`. */
  def emit[O](head: O): Process[Nothing,O] = 
    Emit[Nothing,O](Stream(head), Halt)

  /** Emit a sequence of values, then `Halt`. */
  def emitAll[O](seq: Seq[O]): Process[Nothing,O] = 
    emitSeq(seq, Halt)

  implicit def processInstance[F[_]]: MonadPlus[({type f[x] = Process[F,x]})#f] = 
  new MonadPlus[({type f[x] = Process[F,x]})#f] {
    def empty[A] = Halt
    def plus[A](a: Process[F,A], b: => Process[F,A]): Process[F,A] = 
      a ++ b
    def point[A](a: => A): Process[F,A] = emit(a)
    def bind[A,B](a: Process[F,A])(f: A => Process[F,B]): Process[F,B] = 
      a flatMap f
  }

  /** 
   * Special exception indicating normal termination. Throwing this
   * exception results in control switching to the `fallback` case of
   * whatever `Process` is being run.
   */
  case object End extends Exception { //scala.util.control.ControlThrowable {
    //override def fillInStackTrace = this 
  }
 
  case class Env[-I,-I2]() {
    sealed trait Y[-X] { def tag: Int }
    sealed trait T[-X] extends Y[X] 
    sealed trait Is[-X] extends T[X]
    case object Left extends Is[I] { def tag = 0 }
    case object Right extends T[I2] { def tag = 1 }  
    case object Both extends Y[These[I,I2]] { def tag = 2 }
  }

  private val Left_ = Env[Any,Any]().Left
  private val Right_ = Env[Any,Any]().Right
  private val Both_ = Env[Any,Any]().Both

  def Get[I]: Env[I,Any]#Is[I] = Left_ 
  def L[I]: Env[I,Any]#Is[I] = Left_ 
  def R[I2]: Env[Any,I2]#T[I2] = Right_  
  def Both[I,I2]: Env[I,I2]#Y[These[I,I2]] = Both_ 

  def await1[I]: Process1[I,I] = 
    await(Get[I])(emit)

  def awaitL[I]: Tee[I,Any,I] = 
    await(L[I])(emit)

  def awaitR[I2]: Tee[Any,I2,I2] = 
    await(R[I2])(emit)

  def awaitBoth[I,I2]: Wye[I,I2,These[I,I2]] = 
    await(Both[I,I2])(emit) 

  def receive1[I,O](recv: I => Process1[I,O], fallback: Process1[I,O] = Halt): Process1[I,O] = 
    Await(Get[I], recv, fallback, Halt)

  def receiveL[I,I2,O](
      recv: I => Tee[I,I2,O], 
      fallback: Tee[I,I2,O] = Halt,
      cleanup: Tee[I,I2,O] = Halt): Tee[I,I2,O] = 
    await[Env[I,I2]#T,I,O](L)(recv, fallback, cleanup)

  def receiveR[I,I2,O](
      recv: I2 => Tee[I,I2,O], 
      fallback: Tee[I,I2,O] = Halt,
      cleanup: Tee[I,I2,O] = Halt): Tee[I,I2,O] = 
    await[Env[I,I2]#T,I2,O](R)(recv, fallback, cleanup)

  def receiveLOr[I,I2,O](fallback: Tee[I,I2,O])(
                       recvL: I => Tee[I,I2,O]): Tee[I,I2,O] =
    receiveL(recvL, fallback)

  def receiveROr[I,I2,O](fallback: Tee[I,I2,O])(
                       recvR: I2 => Tee[I,I2,O]): Tee[I,I2,O] =
    receiveR(recvR, fallback)

  def receiveBoth[I,I2,O](
      recv: These[I,I2] => Wye[I,I2,O], 
      fallback: Wye[I,I2,O] = Halt,
      cleanup: Wye[I,I2,O] = Halt): Wye[I,I2,O] = 
    await[Env[I,I2]#Y,These[I,I2],O](Both[I,I2])(recv, fallback, cleanup)

  // Subtyping of various Process types:
  // * Process1 is a Tee that only read from the left (Process1[I,O] <: Tee[I,Any,O])
  // * Tee is a Wye that never requests Both (Tee[I,I2,O] <: Wye[I,I2,O])

  type Process0[+O] = Process[Env[Any,Any]#Is,O]
  type Process1[-I,+O] = Process[Env[I,Any]#Is, O]
  type Tee[-I,-I2,+O] = Process[Env[I,I2]#T, O]
  type Wye[-I,-I2,+O] = Process[Env[I,I2]#Y, O]
  type Sink[+F[_],-O] = Process[F, O => F[Unit]]
  type Channel[+F[_],-I,O] = Process[F, I => F[O]]


  object Subtyping {
    def asTee[I,O](p1: Process1[I,O]): Tee[I,Any,O] = p1 
    def asWye[I,I2,O](t: Tee[I,I2,O]): Wye[I,I2,O] = t 
  }

  /** 
   * Various `Process` functions that aren't worth putting on `Process` 
   * due to variance issues. 
   */
  implicit class ProcessSyntax[F[_],O](self: Process[F,O]) { 
    
    /** Feed this `Process` through the given effectful `Channel`. */
    def through[F2[x]>:F[x],O2](f: Channel[F2,O,O2]): Process[F2,O2] = 
      self.zipWith(f)((o,f) => f(o)).eval


     /** 
      * Feed this `Process` through the given effectful `Channel` 
      * that flushes its state before releasing its resource 
      * Shall be used in conjunction with [[scalaz.stream.io.flushChannel]] combinator
      */
    def throughAndFlush[F2[x]>:F[x],O2](fch: Channel[F2,Option[O],Option[O2]]): Process[F2,O2] = 
       ((self.map(Some(_)) ++ emit(None)) through fch).filter(_.isDefined).map(_.get)

    /** Feed this `Process` to a `Sink`. */
    def to[F2[x]>:F[x]](f: Sink[F2,O]): Process[F2,Unit] = 
      through(f)

    def toMonoid[F2[x]>:F[x]](implicit M: Monoid[O]): Process[F2,O] = 
      self |> process1.fromMonoid(M)

    def fold1[F2[x]>:F[x]](f: (O,O) => O): Process[F2,O] = 
      self |> process1.fold1(f)

    def toMonoid1[F2[x]>:F[x]](implicit M: Semigroup[O]): Process[F2,O] = 
      toSemigroup(M)

    def toSemigroup[F2[x]>:F[x]](implicit M: Semigroup[O]): Process[F2,O] = 
      self |> process1.fromSemigroup(M) 

    /** Attach a `Sink` to the output of this `Process` but echo the original signal. */
    def observe[F2[x]>:F[x]](f: Sink[F2,O]): Process[F2,O] = 
      self.zipWith(f)((o,f) => (o,f(o))).flatMap { case (orig,action) => emit(action).eval.drain ++ emit(orig) }

    /** Feed this `Process` through the given `Channel`, using `q` to control the queueing strategy. */
    def through_y[F2[x]>:F[x],O2,O3](chan: Channel[F2,O,O2])(q: Wye[O,O2,O3])(implicit F2: Nondeterminism[F2]): Process[F2,O3] =
      self.zip(chan).enqueue(q)

    final def feed[I](
        input: Seq[I])(
        f: Process[F,O] => (I => (Option[I], Option[Process[F,O]]))): (Process[F,O], Seq[I]) = {

      @annotation.tailrec
      def go(cur: Process[F,O], input: Seq[I]): (Process[F,O], Seq[I]) = {
        if (!input.isEmpty) f(cur)(input.head) match {
          case (_, None) => (cur, input)
          case (Some(revisit), Some(p2)) => go(p2, revisit +: input.tail)
          case (None, Some(p2)) => go(p2, input.tail)
        }
        else (cur, input)
      }
      go(self, input)
    }
  }

  /** 
   * This class provides infix syntax specific to `Process0`. 
   */
  implicit class Process0Syntax[O](self: Process0[O]) {
    def toIndexedSeq: IndexedSeq[O] = self(List())
    def toList: List[O] = toIndexedSeq.toList 
    def toSeq: Seq[O] = toIndexedSeq
    def toMap[K,V](implicit isKV: O <:< (K,V)): Map[K,V] = toIndexedSeq.toMap(isKV)
    def toSortedMap[K,V](implicit isKV: O <:< (K,V), ord: Ordering[K]): SortedMap[K,V] = 
      SortedMap(toIndexedSeq.asInstanceOf[Seq[(K,V)]]: _*)
    def toStream: Stream[O] = toIndexedSeq.toStream
    def toSource: Process[Task,O] = {
      val iter = toIndexedSeq.iterator
      repeatWrap { Task.delay { if (iter.hasNext) iter.next else throw End }}
    }
  }

  /** 
   * This class provides infix syntax specific to `Process1`.
   */
  implicit class Process1Syntax[I,O](self: Process1[I,O]) {

    /** Apply this `Process` to an `Iterable`. */
    def apply(input: Iterable[I]): IndexedSeq[O] =
      Process(input.toSeq: _*).pipe(self.bufferAll).disconnect.unemit._1.toIndexedSeq
  }

  /** 
   * This class provides infix syntax specific to `Wye`. We put these here 
   * rather than trying to cram them into `Process` itself using implicit
   * equality witnesses. This doesn't work out so well due to variance 
   * issues.
   */
  implicit class WyeSyntax[I,I2,O](self: Wye[I,I2,O]) {

    /** 
     * Apply a `Wye` to two `Iterable` inputs. 
     */
    def apply(input: Iterable[I], input2: Iterable[I2]): IndexedSeq[O] = {
      // this is probably rather slow
      val src1 = Process.emitAll(input.toSeq).toSource
      val src2 = Process.emitAll(input2.toSeq).toSource
      src1.wye(src2)(self).collect.run
    }

    /** 
     * Convert a `Wye` to a `Process1`, by converting requests for the
     * left input into normal termination. Note that `Both` requests are rewritten 
     * to fetch from the only input.
     */
    def detachL: Process1[I2,O] = self match {
      case Halt => Halt
      case Emit(h, t) => Emit(h, t.detachL)
      case Await(req, recv, fb, c) => (req.tag: @annotation.switch) match {
        case 0 => fb.detachL
        case 1 => Await(Get[I2], recv andThen (_ detachL), fb.detachL, c.detachL)
        case 2 => Await(Get[I2], (These.That(_:I2)) andThen recv andThen (_ detachL), fb.detachL, c.detachL)
      }
    } 

    /** 
     * Convert a `Wye` to a `Process1`, by converting requests for the
     * right input into normal termination. Note that `Both` requests are rewritten
     * to fetch from the only input. 
     */
    def detachR: Process1[I,O] = self match {
      case Halt => Halt
      case Emit(h, t) => Emit(h, t.detachR)
      case Await(req, recv, fb, c) => (req.tag: @annotation.switch) match {
        case 0 => Await(Get[I], recv andThen (_ detachR), fb.detachR, c.detachR)
        case 1 => fb.detachR
        case 2 => Await(Get[I], (These.This(_:I)) andThen recv andThen (_ detachR), fb.detachR, c.detachR)
      }
    }
  }
  
  implicit class ChanneledProcess[F[_],O,O2](self: Process[F,(O, O => F[O2])]) {
    def enqueue[O3](q: Wye[O,O2,O3])(implicit F: Nondeterminism[F]): Process[F,O3] = go(self, q, Queue(), Queue())

    private def go[F3[_],O,O2,O3](src: Process[F3,(O,O => F3[O2])], q: Wye[O,O2,O3], bufIn: Seq[O], bufOut: Queue[F3[O2]])(
                  implicit F3: Nondeterminism[F3]): Process[F3,O3] = {
      try q match {
        case Emit(out, q2) => emitAll(out) ++ go(src, q2, bufIn, bufOut) 
        case Halt => src.kill
        case Await(_,_,_,_) => 
          val (srcH, srcT0) = src.unemit
          val srcT = srcT0.asInstanceOf[Process[F3, (O,O => F3[O2])]]
          if (!srcH.isEmpty) go(srcT, q, bufIn ++ srcH.map(_._1), bufOut ++ srcH.map(p => p._2(p._1)))
          else {
            val (q1, bufIn1) = q.feed(bufIn) { q => o => q match {
              case AwaitL(recvL,fbL,cL) => (None, Some(recvL(o)))
              case _ => (None, None)
            }}
            val (q2, bufIn2) = q1.feed(bufIn1) { q => o => q match {
              case AwaitBoth(recv,fb,c) => (None, Some(recv(These.This(o))))
              case _ => (None, None) 
            }}
            q2 match {
              case AwaitL(_,_,_) => srcT.unconsAll.flatMap { case (pairs,tl) => 
                val tail = tl.asInstanceOf[Process[F3,(O,O => F3[O2])]]
                go(tail, q2, bufIn2 ++ pairs.view.map(_._1), bufOut ++ pairs.view.map(p => p._2(p._1))) 
              } 
              case AwaitR(recv,fb,c) => 
                if (bufOut.isEmpty) 
                  srcT.unconsAll.flatMap { case (pairs,tl) => 
                    val tail = tl.asInstanceOf[Process[F3,(O,O => F3[O2])]]
                    go(tail, q2, bufIn2 ++ pairs.view.map(_._1), bufOut ++ pairs.view.map(p => p._2(p._1))) 
                  } 
                else {
                  val (outH, outT) = bufOut.dequeue
                  await(outH)(recv andThen (go(srcT, _, bufIn2, outT)), go(srcT, fb, bufIn2, outT), go(srcT, c, bufIn2, outT))
                }
              case AwaitBoth(recv,fb,c) => 
                if (bufOut.isEmpty) 
                  srcT.unconsAll.flatMap { case (pairs,tl) => 
                    val tail = tl.asInstanceOf[Process[F3,(O,O => F3[O2])]]
                    go(tail, q2, bufIn2 ++ pairs.view.map(_._1), bufOut ++ pairs.view.map(p => p._2(p._1))) 
                  }
                else {
                  srcT match {
                    case Halt => emitAll(bufOut).eval |> q2.detachL  
                    case AwaitF(reqsrc,recvsrc,fbsrc,csrc) => 
                      val (outH, outT) = bufOut.dequeue
                      await(F3.choose(reqsrc, outH))(
                        _.fold(
                          { case (p,ro) => go(recvsrc(p), q2, bufIn2, ro +: outT) },
                          { case (rp,o2) => go(await(rp)(recvsrc, fbsrc, csrc), recv(These.That(o2)), bufIn2, outT) }
                        ), 
                        go(fbsrc, q2, bufIn2, bufOut),
                        go(csrc, q2, bufIn2, bufOut)
                      )
                    case _ => sys.error("unpossible!")
                  }
                } 
              case Halt => srcT.kill
              case Emit(h, t) => emitAll(h) ++ go(srcT, t, bufIn2, bufOut)
            }
          } 
      }
      catch { case e: Throwable => src.kill ++ (throw e) }
    }
  }

  /** 
   * Provides infix syntax for `eval: Process[F,F[O]] => Process[F,O]`
   */
  implicit class EvalProcess[F[_],O](self: Process[F,F[O]]) {

    /** 
     * Evaluate the stream of `F` actions produced by this `Process`.
     * This sequences `F` actions strictly--the first `F` action will
     * be evaluated before work begins on producing the next `F` 
     * action. To allow for concurrent evaluation, use `sequence`
     * or `gather`. 
     */
    def eval: Process[F,O] = self match {
      case Halt => Halt
      case Emit(h, t) => 
        if (h.isEmpty) t.eval
        else await[F,O,O](h.head)(o => emit(o) ++ emitSeq(h.tail, t).eval)
      case Await(req,recv,fb,c) => 
        await(req)(recv andThen (_ eval), fb.eval, c.eval) 
    }

    /** 
     * Evaluate the stream of `F` actions produced by this `Process`, 
     * returning the results in order while allowing up to `bufSize`
     * concurrent outstanding requests. If preserving output order is 
     * not important, `gather` can be used instead. 
     */
    def sequence(bufSize: Int)(implicit F: Nondeterminism[F]): Process[F,O] = 
      self.map(fo => (fo, identity[F[O]] _)).enqueue(wye.boundedQueue(bufSize))

    /** 
     * Like `sequence`, but allows output to be reordered. That is,
     * the `n`th output may correspond to any of the `F` actions whose
     * index in the original `Process` is within `bufSize` of `n`. 
     * To preserve the output order while allowing for concurrency in
     * evaluation, use `sequence`. 
     */
    def gather(bufSize: Int)(implicit F: Nondeterminism[F]): Process[F,O] = 
      self.pipe(process1.chunk(bufSize)).map(F.gatherUnordered).eval.flatMap(emitAll) 
  }
  
  /** Prefix syntax for `p.repeat`. */
  def repeat[F[_],O](p: Process[F,O]): Process[F,O] = p.repeat

  /** Wrap an arbitrary effect in a `Process`. The resulting `Process` emits a single value. */
  def wrap[F[_],O](t: F[O]): Process[F,O] = 
    emit(t).eval

  /** 
   * Wrap an arbitrary effect in a `Process`. The resulting `Process` will emit values
   * until evaluation of `t` signals termination with `End` or an error occurs. 
   */
  def repeatWrap[F[_],O](t: F[O]): Process[F,O] =
    wrap(t).repeat
  
  // a failed attempt to work around Scala's broken type refinement in 
  // pattern matching by supplying the equality witnesses manually

  /** Obtain an equality witness from an `Is` request. */
  def witnessIs[I,J](req: Env[I,Nothing]#Is[J]): I === J = 
    Leibniz.refl[I].asInstanceOf[I === J]

  /** Obtain an equality witness from a `T` request. */
  def witnessT[I,I2,J](t: Env[I,I2]#T[J]): 
  (I === J) \/ (I2 === J) = 
    if (t.tag == 0) left(Leibniz.refl[I].asInstanceOf[I === J])
    else right(Leibniz.refl[I2].asInstanceOf[I2 === J])

  /** Obtain an equality witness from a `Y` request. */
  def witnessY[I,I2,J](t: Env[I,I2]#Y[J]): 
  (I === J) \/ (I2 === J) \/ (These[I,I2] === J) = 
    if (t.tag == 2) right(Leibniz.refl[I].asInstanceOf[These[I,I2] === J])
    else left(witnessT(t.asInstanceOf[Env[I,I2]#T[J]]))

  /** Evidence that `F[x] <: G[x]` for all `x`. */
  trait Subprotocol[F[_], G[_]] {
    def subst[C[_[_],_],A](f: C[F,A]): C[G,A]

    implicit def substProcess[A](p: Process[F,A]): Process[G,A] = 
      subst[({type c[f[_],x] = Process[f,x]})#c, A](p)
  }

  // boilerplate to enable monadic infix syntax without explicit imports 

  import scalaz.syntax.{ApplyOps, ApplicativeOps, FunctorOps, MonadOps}
  
  trait ProcessTC[F[_]] { type f[y] = Process[F,y] }

  implicit def toMonadOps[F[_],A](f: Process[F,A]): MonadOps[ProcessTC[F]#f,A] = 
    processInstance.monadSyntax.ToMonadOps(f)
  implicit def toApplicativeOps[F[_],A](f: Process[F,A]): ApplicativeOps[ProcessTC[F]#f,A] = 
    processInstance.applicativeSyntax.ToApplicativeOps(f)
  implicit def toApplyOps[F[_],A](f: Process[F,A]): ApplyOps[ProcessTC[F]#f,A] = 
    processInstance.applySyntax.ToApplyOps(f)
  implicit def toFunctorOps[F[_],A](f: Process[F,A]): FunctorOps[ProcessTC[F]#f,A] =
    processInstance.functorSyntax.ToFunctorOps(f)
}

