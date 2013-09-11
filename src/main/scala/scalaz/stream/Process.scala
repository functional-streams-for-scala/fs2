package scalaz.stream

import scala.collection.immutable.{IndexedSeq,SortedMap,Queue,Vector}
import scala.concurrent.duration._

import scalaz.{Catchable,Functor,Monad,MonadPlus,Monoid,Nondeterminism,Semigroup}
import scalaz.concurrent.{Strategy, Task}
import scalaz.Leibniz.===
import scalaz.{\/,-\/,\/-,~>,Leibniz,Equal}
import \/._
import These.{This,That}

import java.util.concurrent._

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
sealed abstract class Process[+F[_],+O] {

  import Process._

  /** Transforms the output values of this `Process` using `f`. */
  final def map[O2](f: O => O2): Process[F,O2] = {
    // a bit of trickness here - in the event that `f` itself throws an
    // exception, we use the most recent fallback/cleanup from the prior `Await`
    def go(cur: Process[F,O], fallback: Process[F,O], cleanup: Process[F,O]): Process[F,O2] = 
      cur match {
        case h@Halt(_) => h 
        case Await(req,recv,fb,c) =>
          Await[F,Any,O2](req, recv andThen (go(_, fb, c)), fb map f, c map f)
        case Emit(h, t) => 
          try Emit[F,O2](h map f, go(t, fallback, cleanup))
          catch { 
            case End => fallback.map(f)
            case e: Throwable => cleanup.map(f).causedBy(e)
          }
      }
    go(this, halt, halt)
  }

  /**
   * Generate a `Process` dynamically for each output of this `Process`, and
   * sequence these processes using `append`.
   */
  final def flatMap[F2[x]>:F[x], O2](f: O => Process[F2,O2]): Process[F2,O2] = {
    // a bit of trickness here - in the event that `f` itself throws an
    // exception, we use the most recent fallback/cleanup from the prior `Await`
    def go(cur: Process[F,O], fallback: Process[F,O], cleanup: Process[F,O]): Process[F2,O2] = 
      cur match {
        case h@Halt(_) => h
        case Emit(Seq(o), Halt(End)) => 
          try f(o)
          catch {
            case End => fallback.flatMap(f) 
            case e: Throwable => cleanup.flatMap(f).causedBy(e)
          }
        case Emit(o, t) =>
          if (o.isEmpty) go(t, fallback, cleanup)
          else 
            try { f(o.head) ++ go(emitSeq(o.tail, t), fallback, cleanup) }
            catch {
              case End => fallback.flatMap(f) 
              case e: Throwable => cleanup.flatMap(f).causedBy(e)
            }
        case Await(req,recv,fb,c) =>
          Await(req, recv andThen (go(_, fb, c)), fb flatMap f, c flatMap f)
      }
    go(this, halt, halt)
  }

  /**
   * Run this `Process`, then, if it halts without an error, run `p2`.
   * Note that `p2` is appended to the `fallback` argument of any `Await`
   * produced by this `Process`. If this is not desired, use `then`.
   */
  final def append[F2[x]>:F[x], O2>:O](p2: => Process[F2,O2]): Process[F2,O2] = this match {
    case h@Halt(e) => e match { 
      case End => 
        try p2
        catch { case End => h
                case e2: Throwable => Halt(e2) 
              }
      case _ => h
    }
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
    case h@Halt(e) => e match { 
      case End => p2
      case _ => h
    }
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
    case h@Halt(_) => h 
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
      case h@Halt(e) => e match { 
        case End => go(this)
        case _ => h
      }
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
    case h@Halt(_) => h 
  }

  /** Halt this process, allowing for any cleanup actions, and `e` as a cause. */
  final def killBy(e: Throwable): Process[F,Nothing] =
    this.kill.causedBy(e)

  /** 
   * Add `e` as a cause when this `Process` halts. 
   * This is a noop and returns immediately if `e` is `Process.End`. 
   */
  final def causedBy[F2[x]>:F[x],O2>:O](e: Throwable): Process[F2,O2] = {
    e match {
      case End => this
      case _ => this.causedBy_(e)
    }
  }
  private final def causedBy_[F2[x]>:F[x],O2>:O](e: Throwable): Process[F2, O2] = this match {
    case Await(req,recv,fb,c) => 
      Await(req, recv andThen (_.causedBy_(e)), fb.causedBy_(e), c.causedBy_(e))
    case Emit(h, t) => Emit(h, t.causedBy_(e))
    case h@Halt(e0) => e0 match {
      case End => Halt(e)
      case _ => Halt(CausedBy(e0, e))
    }
  }

  /**
   * Switch to the `fallback` case of the _next_ `Await` issued by this `Process`.
   */
  final def fallback: Process[F,O] = this match {
    case Await(req,recv,fb,c) => fb
    case Emit(h, t) => emitSeq(h, t.fallback)
    case h@Halt(_) => h 
  }

  /**
   * Append to the `fallback` and `cleanup` arguments of the _next_ `Await`.
   */
  final def orElse[F2[x]>:F[x],O2>:O](fallback: => Process[F2,O2], cleanup: => Process[F2,O2] = halt): Process[F2,O2] = this match {
    case Await(req,recv,fb,c) => Await(req, recv, fb ++ fallback, c ++ cleanup)
    case Emit(h, t) => Emit(h, t.orElse(fallback, cleanup))
    case h@Halt(_) => h 
  }

  /**
   * Run `p2` after this `Process` if this `Process` completes with an an error. 
   */
  final def onFailure[F2[x]>:F[x],O2>:O](p2: => Process[F2,O2]): Process[F2,O2] = this match {
    case Await(req,recv,fb,c) => Await(req, recv andThen (_.onFailure(p2)), fb, c ++ onFailure(p2))
    case Emit(h, t) => Emit(h, t.onFailure(p2))
    case h@Halt(e) => 
      try p2.causedBy(e)
      catch { case End => h
              case e2: Throwable => Halt(CausedBy(e2, e)) 
            }
  }

  /**
   * Run `p2` after this `Process` completes normally, or in the event of an error. 
   * This behaves almost identically to `append`, except that `p1 append p2` will 
   * not run `p2` if `p1` halts with an error. 
   */
  final def onComplete[F2[x]>:F[x],O2>:O](p2: => Process[F2,O2]): Process[F2,O2] = this match {
    case Await(req,recv,fb,c) => Await(req, recv andThen (_.onComplete(p2)), fb.onComplete(p2), c.onComplete(p2))
    case Emit(h, t) => Emit(h, t.onComplete(p2))
    case h@Halt(e) => 
      try p2.causedBy(e)
      catch { case End => h
              case e2: Throwable => Halt(CausedBy(e2, e)) 
            }
  }

  /**
   * Switch to the `fallback` case of _all_ subsequent awaits.
   */
  final def disconnect: Process[Nothing,O] = this match {
    case Await(req,recv,fb,c) => fb.disconnect
    case Emit(h, t) => emitSeq(h, t.disconnect)
    case h@Halt(_) => h 
  }

  /**
   * Switch to the `cleanup` case of the next `Await` issued by this `Process`.
   */
  final def cleanup: Process[F,O] = this match {
    case Await(req,recv,fb,c) => fb
    case h@Halt(_) => h 
    case Emit(h, t) => emitSeq(h, t.cleanup)
  }

  /**
   * Switch to the `cleanup` case of _all_ subsequent awaits.
   */
  final def hardDisconnect: Process[Nothing,O] = this match {
    case Await(req,recv,fb,c) => c.hardDisconnect
    case h@Halt(_) => h 
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
  private[stream] def asAwait: Option[(F[Any], Any => Process[F,O], Process[F,O], Process[F,O])] =
    this match {
      case Await(req,recv,fb,c) => Some((req,recv,fb,c))
      case _ => None
    }

  /**
   * Ignores output of this `Process`. A drained `Process` will never `Emit`.
   */
  def drain: Process[F,Nothing] = this match {
    case h@Halt(_) => h 
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
    (this tee halt)(p2)

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
    // Implementation is a horrifying mess, due mainly to Scala's broken pattern matching
    import F2.monadSyntax._
    try y match {
      case h@Halt(_) => this.kill onComplete p2.kill onComplete h
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
                  case This(o) => (None, Some(recv_(o)))
                  case That(_) => (None, None)
                  case These(o,o2) => (Some(That(o2)), Some(recv_(o)))
                }
              case 1 => // Right
                val recv_ = recv.asInstanceOf[O2 => Wye[O,O2,O3]]
                (e: These[O,O2]) => e match {
                  case This(_) => (None, None)
                  case That(o2) => (None, Some(recv_(o2)))
                  case These(o,o2) => (Some(This(o)), Some(recv_(o2)))
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
                case Halt(e) => p2Next.killBy(e) onComplete y2.disconnect
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
                case Halt(e) => thisNext.killBy(e) onComplete y2.disconnect
                case e@Emit(_,_) => thisNext.wye(p2Next)(y2)
              }
            case 2 => thisNext match { // Both
              case Halt(e) => p2Next.causedBy(e) |> y2.detachL
              case AwaitF(reqL, recvL, fbL, cL) => p2Next match {
                case Halt(e) => e match {
                  case End => Await(reqL, recvL andThen (_.wye(p2Next)(y2)), fbL.wye(p2Next)(y2), cL.wye(p2Next)(y2))
                  case _ => thisNext.causedBy(e).wye(halt)(y2)
                }
                // If both sides are in the Await state, we use the
                // Nondeterminism instance to request both sides
                // concurrently.
                case AwaitF(reqR, recvR, fbR, cR) =>
                  wrap(F2.choose(E.attempt(reqL), E.attempt(reqR))).flatMap {
                    _.fold(
                      { case (winningReqL, losingReqR) => 
                          winningReqL.fold(
                            { case End => fbL.wye(Await(rethrow(losingReqR), recvR, fbR, cR))(y2)
                              case t: Throwable => cL.wye(Await(rethrow(losingReqR), recvR, fbR, cR))(y2) onComplete (fail(t))
                            },
                            res => {
                              val nextL = recvL(res)
                              nextL.wye(Await(rethrow(losingReqR), recvR, fbR, cR))(y2) 
                            }
                          )
                      },
                      { case (losingReqL, winningReqR) => 
                          winningReqR.fold(
                            { case End => Await(rethrow(losingReqL), recvL, fbL, cL).wye(fbR)(y2)
                              case t: Throwable => Await(rethrow(losingReqL), recvL, fbL, cL).wye(cR)(y2) onComplete (fail(t))
                            },
                            res => {
                              val nextR = recvR(res)
                              Await(rethrow(losingReqL), recvL, fbL, cL).wye(nextR)(y2)
                            }
                          )
                      }
                    ) 
                  }
              }
            }
          }
          case _ => thisNext.wye(p2Next)(y2)
        }
    }
    catch { case e: Throwable => 
      this.kill onComplete p2.kill onComplete (Halt(e))
    }
  }

  /** Translate the request type from `F` to `G`, using the given polymorphic function. */
  def translate[G[_]](f: F ~> G): Process[G,O] = this match {
    case Emit(h, t) => Emit(h, t.translate(f))
    case h@Halt(_) => h 
    case Await(req, recv, fb, c) =>
      Await(f(req), recv andThen (_ translate f), fb translate f, c translate f)
  }

  /**
   * Catch exceptions produced by this `Process`, not including normal termination,
   * and uses `f` to decide whether to resume a second process. 
   */
  def attempt[F2[x]>:F[x],O2](f: Throwable => Process[F2,O2] = (t:Throwable) => emit(t))(
                              implicit F: Catchable[F2]): Process[F2, O2 \/ O] =
  this match {
    case Emit(h, t) => Emit(h map (right), t.attempt[F2,O2](f))
    case Halt(e) => e match {
      case End => halt
      case _ => try f(e).map(left) 
                catch { case End => halt
                        case e2: Throwable => Halt(CausedBy(e2, e))
                      }
    }
    case Await(req, recv, fb, c) =>
      await(F.attempt(req))(
        _.fold(
          { case End => fb.attempt[F2,O2](f)
            case err => c.drain onComplete f(err).map(left) 
          }, 
          recv andThen (_.attempt[F2,O2](f))
        ),
        fb.attempt[F2,O2](f), c.attempt[F2,O2](f))
  }

  /**
   * Catch some of the exceptions generated by this `Process`, rethrowing any
   * not handled by the given `PartialFunction` and stripping out any values
   * emitted before the error.
   */
  def handle[F2[x]>:F[x],O2](f: PartialFunction[Throwable, Process[F2,O2]])(implicit F: Catchable[F2]): Process[F2, O2] =
    attempt(err => f.lift(err).getOrElse(fail(err))).
    dropWhile(_.isRight).
    map(_.fold(identity, _ => sys.error("unpossible")))

  /**
   * Like `attempt`, but accepts a partial function. Unhandled errors are rethrown.
   */
  def partialAttempt[F2[x]>:F[x],O2](f: PartialFunction[Throwable, Process[F2,O2]])(implicit F: Catchable[F2]): Process[F2, O2 \/ O] =
    attempt(err => f.lift(err).getOrElse(fail(err)))

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
    def go(cur: Process[F2,O2], acc: Vector[O2]): F2[IndexedSeq[O2]] =
      cur match {
        case Emit(h,t) =>
          go(t.asInstanceOf[Process[F2,O2]], h.asInstanceOf[Seq[O2]].foldLeft(acc)(_ :+ _))
        case Halt(e) => e match {
          case End => F.point(acc)
          case _ => C.fail(e)
        }
        case Await(req,recv,fb,c) =>
           F.bind (C.attempt(req.asInstanceOf[F2[AnyRef]])) {
             _.fold(
               { case End => go(fb.asInstanceOf[Process[F2,O2]], acc)
                 case e => go(c.asInstanceOf[Process[F2,O2]].causedBy(e), acc)
               },
               o => go(recv.asInstanceOf[AnyRef => Process[F2,O2]](o), acc)
             )
           }
      }
    go(this, Vector[O2]())
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

  /** Alias for `this |> process1.bufferAll`. */
  def bufferAll: Process[F,O] =
    this |> process1.bufferAll

  /** Alias for `this |> process1.chunk(n)`. */
  def chunk(n: Int): Process[F,Vector[O]] =
    this |> process1.chunk(n)

  /** Alias for `this |> process1.chunkBy(f)`. */
  def chunkBy(f: O => Boolean): Process[F,Vector[O]] =
    this |> process1.chunkBy(f)

  /** Alias for `this |> process1.split(f)` */
  def split(f: O => Boolean): Process[F,Vector[O]] =
    this |> process1.split(f)

  /** Alias for `this |> process1.splitOn(f)` */
  def splitOn[P >: O](p: P)(implicit P: Equal[P]): Process[F,Vector[P]] =
    this |> process1.splitOn(p)

  /** Alias for `this |> process1.chunkAll`. */
  def chunkAll: Process[F,Vector[O]] =
    this |> process1.chunkAll

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

  /** Insert `sep` between elements emitted by this `Process`. */
  def intersperse[O2>:O](sep: O2): Process[F,O2] =
    this |> process1.intersperse(sep)

  /** Halts this `Process` after emitting 1 element. */
  def once: Process[F,O] = take(1)

  /** Skips all elements emitted by this `Process` except the last. */
  def last: Process[F,O] = this |> process1.last

  /** Halts this `Process` after emitting `n` elements. */
  def take(n: Int): Process[F,O] =
    this |> processes.take[O](n)

  /** Halts this `Process` as soon as the predicate tests false. */
  def takeWhile(f: O => Boolean): Process[F,O] =
    this |> processes.takeWhile(f)

  /** Wraps all outputs in `Some`, then outputs a single `None` before halting. */
  def terminated: Process[F,Option[O]] =
    this |> processes.terminated

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

  /** Nondeterministic interleave of both streams. Emits values whenever either is defined. */ 
  def merge[F2[x]>:F[x],O2>:O](p2: Process[F2,O2])(
  implicit F: Nondeterminism[F2], E: Catchable[F2]): Process[F2,O2] = 
    this.wye(p2)(scalaz.stream.wye.merge)

  /** Nondeterministic interleave of both streams. Emits values whenever either is defined. */
  def either[F2[x]>:F[x],O2>:O,O3](p2: Process[F2,O3])(
  implicit F: Nondeterminism[F2], E: Catchable[F2]): Process[F2,O2 \/ O3] = 
    this.wye(p2)(scalaz.stream.wye.either)

  /** 
   * When `condition` is `true`, lets through any values in `this` process, otherwise blocks
   * until `condition` becomes true again. Note that the `condition` is checked before 
   * each and every read from `this`, so `condition` should return very quickly or be 
   * continuous to avoid holding up the output `Process`. Use `condition.forwardFill` to 
   * convert an infrequent discrete `Process` to a continuous one for use with this
   * function. 
   */
  def when[F2[x]>:F[x],O2>:O](condition: Process[F2,Boolean]): Process[F2,O2] =
    condition.tee(this)(scalaz.stream.tee.when)

  /**
   * Halts this `Process` as soon as `condition` becomes `true`. Note that `condition`
   * is checked before each and every read from `this`, so `condition` should return
   * very quickly or be continuous to avoid holding up the output `Process`. Use
   * `condition.forwardFill` to convert an infrequent discrete `Process` to a 
   * continuous one for use with this function.
   */
  def until[F2[x]>:F[x],O2>:O](condition: Process[F2,Boolean]): Process[F2,O2] = 
    condition.tee(this)(scalaz.stream.tee.until)
}

object processes extends process1 with tee with wye with io

object Process {
  case class Await[F[_],A,+O] private[stream](
    req: F[A], recv: A => Process[F,O],
    fallback1: Process[F,O] = halt,
    cleanup1: Process[F,O] = halt) extends Process[F,O]

  case class Emit[F[_],O](
    head: Seq[O],
    tail: Process[F,O]) extends Process[F,O]

  case class Halt(cause: Throwable) extends Process[Nothing,Nothing]
  

  object AwaitF {
    trait Req
    private[stream] def unapply[F[_],O](self: Process[F,O]):
        Option[(F[Any], Any => Process[F,O], Process[F,O], Process[F,O])] =
      self.asAwait
  }
  object Await1 {
    def unapply[I,O](self: Process1[I,O]):
        Option[(I => Process1[I,O], Process1[I,O], Process1[I,O])] = self match {

      case Await(_,recv,fb,c) => Some((recv.asInstanceOf[I => Process1[I,O]], fb, c))
      case _ => None
    }
  }
  object AwaitL {
    def unapply[I,I2,O](self: Wye[I,I2,O]):
        Option[(I => Wye[I,I2,O], Wye[I,I2,O], Wye[I,I2,O])] = self match {
      case Await(req,recv,fb,c) if req.tag == 0 => Some((recv.asInstanceOf[I => Wye[I,I2,O]], fb, c))
      case _ => None
    }
  }
  object AwaitR {
    def unapply[I,I2,O](self: Wye[I,I2,O]):
        Option[(I2 => Wye[I,I2,O], Wye[I,I2,O], Wye[I,I2,O])] = self match {
      case Await(req,recv,fb,c) if req.tag == 1 => Some((recv.asInstanceOf[I2 => Wye[I,I2,O]], fb, c))
      case _ => None
    }
  }
  object AwaitBoth {
    def unapply[I,I2,O](self: Wye[I,I2,O]):
        Option[(These[I,I2] => Wye[I,I2,O], Wye[I,I2,O], Wye[I,I2,O])] = self match {
      case Await(req,recv,fb,c) if req.tag == 2 => Some((recv.asInstanceOf[These[I,I2] => Wye[I,I2,O]], fb, c))
      case _ => None
    }
  }

  def emitSeq[F[_],O](
      head: Seq[O],
      tail: Process[F,O] = halt): Process[F,O] =
    if (head.isEmpty) tail
    else tail match {
      case Emit(h2,t) => Emit(head ++ h2.asInstanceOf[Seq[O]], t.asInstanceOf[Process[F,O]])
      case _ => Emit(head, tail)
    }

  def await[F[_],A,O](req: F[A])(
      recv: A => Process[F,O] = (a: A) => halt,
      fallback: Process[F,O] = halt,
      cleanup: Process[F,O] = halt): Process[F,O] =
    Await(req, recv, fallback, cleanup)

  def apply[O](o: O*): Process[Nothing,O] =
    emitSeq[Nothing,O](o, halt)

  /** The infinite `Process`, always emits `a`. */
  def constant[A](a: A, chunkSize: Int = 1): Process[Task,A] = {
    lazy val go: Process[Task,A] =
      if (chunkSize.max(1) == 1)
        await(Task.now(a))(a => Emit(List(a), go))
      else
        await(Task.now(List.fill(chunkSize)(a)))(emitSeq(_, go))
    go
  }

  /** A `Process` which emits `n` repetitions of `a`. */
  def fill[A](n: Int)(a: A, chunkSize: Int = 1): Process[Task,A] = {
    val chunkN = chunkSize max 1
    val chunkTask = Task.now(List.fill(chunkN)(a)) // we can reuse this for each step
    def go(m: Int): Process[Task,A] =
      if (m >= chunkN) await(chunkTask)(emitSeq(_, go(m - chunkN)))
      else if (m <= 0) halt
      else await(Task.now(List.fill(m)(a)))(emitSeq(_, halt))
    go(n max 0)
  }

  /** Produce a (potentially infinite) source from an unfold. */
  def unfold[S,A](s0: S)(f: S => Option[(A,S)]): Process[Task,A] =
    await(Task.delay(f(s0)))(o =>
      o.map(ht => Emit(List(ht._1), unfold(ht._2)(f))).
        getOrElse(halt)
    )

  /** 
   * Produce a stream encapsulating some state, `S`. At each step, 
   * produces the current state, and an effectful function to set the
   * state that will be produced next step.
   */
  def state[S](s0: S): Process[Task, (S, S => Task[Unit])] = suspend {
    val v = async.localRef[S]
    v.signal.continuous.take(1).map(s => (s, (s: S) => Task.delay(v.set(s)))).repeat 
  }
  
  /** 
   * A continuous stream of the elapsed time, computed using `System.nanoTime`. 
   * Note that the actual granularity of these elapsed times depends on the OS, for instance
   * the OS may only update the current time every ten milliseconds or so.
   */
  def duration: Process[Task, Duration] = suspend {
    val t0 = System.currentTimeMillis
    repeatWrap { Task.delay { Duration(System.nanoTime - t0, NANOSECONDS) }}
  }

  /**
   * A continuous stream which is true after `d, 2d, 3d...` elapsed duration.  
   * If you'd like a discrete stream that will actually block until `d` has elapsed, 
   * use `awakeEvery` instead.
   */
  def every(d: Duration): Process[Task, Boolean] = 
    duration zip (emit(0 milliseconds) ++ duration) map {
      case (cur, prev) => (cur - prev) > d  
    }

  // pool for event scheduling
  // threads are just used for timing, no logic is run on this Thread
  private[stream] val _scheduler = {
    Executors.newScheduledThreadPool(4, new ThreadFactory {
      def newThread(r: Runnable) = {
        val t = Executors.defaultThreadFactory.newThread(r)
        t.setDaemon(true)
        t.setName("scheduled-task-thread")
        t
      }
    })
  }
  
  /** 
   * A discrete tasks which emits elapsed durations at the given 
   * regular duration. For example: `awakeEvery(5 seconds)` will 
   * return (approximately) `5s, 10s, 20s`, and will lie dormant 
   * between emitted values. By default, this uses a shared
   * `ScheduledExecutorService` for the timed events, and runs the
   * actual callbacks on `pool`, which avoids blocking a useful 
   * thread simply to interpret the delays between events.
   */
  def awakeEvery(d: Duration)(
      implicit pool: ExecutorService = Strategy.DefaultExecutorService,
               schedulerPool: ScheduledExecutorService = _scheduler): Process[Task, Duration] = suspend {
    val (q, p) = async.queue[Boolean](Strategy.Executor(pool))
    val latest = async.ref[Duration](Strategy.Sequential)
    val t0 = Duration(System.nanoTime, NANOSECONDS)
    val metronome = _scheduler.scheduleAtFixedRate(
       new Runnable { def run = {
         val d = Duration(System.nanoTime, NANOSECONDS) - t0
         latest.set(d)
         if (q.size == 0)
           q.enqueue(true) // only produce one event at a time
       }},
       d.toNanos,
       d.toNanos,
       NANOSECONDS
    )
    repeatWrap {
      Task.async[Duration] { cb => 
        q.dequeue { r => 
          r.fold(
            e => cb(left(e)), 
            _ => latest.get {
              d => pool.submit(new Callable[Unit] { def call = cb(d) }) 
            }
          )
        }
      }
    } onComplete { suspend { metronome.cancel(false); halt } }
  }

  /** `Process.emitRange(0,5) == Process(0,1,2,3,4).` */
  def emitRange(start: Int, stopExclusive: Int): Process[Nothing,Int] =
    emitSeq(start until stopExclusive)

  /** Lazily produce the range `[start, stopExclusive)`. */
  def range(start: Int, stopExclusive: Int, by: Int = 1): Process[Task, Int] =
    unfold(start)(i => if (i < stopExclusive) Some((i,i+by)) else None)

  /**
   * Lazily produce a sequence of nonoverlapping ranges, where each range
   * contains `size` integers, assuming the upper bound is exclusive.
   * Example: `ranges(0, 1000, 10)` results in the pairs
   * `(0, 10), (10, 20), (20, 30) ... (990, 1000)`
   *
   * Note: The last emitted range may be truncated at `stopExclusive`. For
   * instance, `ranges(0,5,4)` results in `(0,4), (4,5)`.
   */
  def ranges(start: Int, stopExclusive: Int, size: Int): Process[Task, (Int, Int)] =
    if (size <= 0) sys.error("size must be > 0, was: " + size)
    else unfold(start)(lower =>
      if (lower < stopExclusive)
        Some((lower -> ((lower+size) min stopExclusive), lower+size))
      else
        None)
  
  /** 
   * Convert a `Process` to a `Task` which can be run repeatedly to generate
   * the elements of the `Process`.
   */
  def toTask[A](p: Process[Task,A]): Task[A] = {
    var cur = p
    def go: Task[A] = cur match {
      case Halt(e) => Task.fail(e) 
      case Emit(h, t) => 
        if (h.isEmpty) { cur = t; go }
        else { val ret = Task.now(h.head); cur = Emit(h.tail, t); ret } 
      case Await(req, recv, fb, c) => 
        req.attempt.flatMap {
          case -\/(End) => cur = fb; go 
          case -\/(t: Throwable) => cur = c; go.flatMap(_ => Task.fail(t))
          case \/-(resp) => cur = recv(resp); go
        } 
    }
    Task.delay(go).flatMap(a => a)
  }
    
  /** 
   * Produce a continuous stream from a discrete stream by using the
   * most recent value.  
   */
  def forwardFill[A](p: Process[Task,A])(implicit S: Strategy = Strategy.DefaultStrategy): Process[Task,A] = {
    suspend {
      val v = async.signal[A](S)
      val t = toTask(p).map(a => v.value.set(a))
      val setvar = repeatWrap(t).drain
      v.continuous.merge(setvar)
    }
  }
     
  /** Emit a single value, then `Halt`. */
  def emit[O](head: O): Process[Nothing,O] =
    Emit[Nothing,O](List(head), halt)

  /** Emit a sequence of values, then `Halt`. */
  def emitAll[O](seq: Seq[O]): Process[Nothing,O] =
    emitSeq(seq, halt)

  implicit def processInstance[F[_]]: MonadPlus[({type f[x] = Process[F,x]})#f] =
  new MonadPlus[({type f[x] = Process[F,x]})#f] {
    def empty[A] = halt
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
  case object End extends Exception {
    override def fillInStackTrace = this
  }

  class CausedBy(e: Throwable, cause: Throwable) extends Exception {
    override def toString = s"$e\n\ncaused by:\n\n$cause"
  }
  
  object CausedBy {
    def apply(e: Throwable, cause: Throwable): Throwable = 
      cause match {
        case End => e
        case _ => new CausedBy(e, cause) 
      }
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

  /** A `Process` that halts due to normal termination. */
  val halt: Process[Nothing,Nothing] = Halt(End)

  /** A `Process` that halts due to the given exception. */
  def fail(e: Throwable): Process[Nothing,Nothing] = Halt(e)

  def await1[I]: Process1[I,I] =
    await(Get[I])(emit)

  def awaitL[I]: Tee[I,Any,I] =
    await(L[I])(emit)

  def awaitR[I2]: Tee[Any,I2,I2] =
    await(R[I2])(emit)

  def awaitBoth[I,I2]: Wye[I,I2,These[I,I2]] =
    await(Both[I,I2])(emit)

  def receive1[I,O](recv: I => Process1[I,O], fallback: Process1[I,O] = halt): Process1[I,O] =
    Await(Get[I], recv, fallback, halt)

  def receiveL[I,I2,O](
      recv: I => Tee[I,I2,O],
      fallback: Tee[I,I2,O] = halt,
      cleanup: Tee[I,I2,O] = halt): Tee[I,I2,O] =
    await[Env[I,I2]#T,I,O](L)(recv, fallback, cleanup)

  def receiveR[I,I2,O](
      recv: I2 => Tee[I,I2,O],
      fallback: Tee[I,I2,O] = halt,
      cleanup: Tee[I,I2,O] = halt): Tee[I,I2,O] =
    await[Env[I,I2]#T,I2,O](R)(recv, fallback, cleanup)

  def receiveLOr[I,I2,O](fallback: Tee[I,I2,O])(
                       recvL: I => Tee[I,I2,O]): Tee[I,I2,O] =
    receiveL(recvL, fallback)

  def receiveROr[I,I2,O](fallback: Tee[I,I2,O])(
                       recvR: I2 => Tee[I,I2,O]): Tee[I,I2,O] =
    receiveR(recvR, fallback)

  def receiveBoth[I,I2,O](
      recv: These[I,I2] => Wye[I,I2,O],
      fallback: Wye[I,I2,O] = halt,
      cleanup: Wye[I,I2,O] = halt): Wye[I,I2,O] =
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
     * Feed this `Process` through the given effectful `Channel`, signaling
     * termination to `f` via `None`. Useful to allow `f` to flush any
     * buffered values to the output when it detects termination, see
     * [[scalaz.stream.io.bufferedChannel]] combinator.
     */
    def throughOption[F2[x]>:F[x],O2](f: Channel[F2,Option[O],O2]): Process[F2,O2] =
      self.terminated.through(f)

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

    /** Feed this `Process` through the given `Channel`, using `q` to control the queueing strategy. Alias for `connect`. */
    def through_y[F2[x]>:F[x],O2,O3](chan: Channel[F2,O,O2])(q: Wye[O,O2,O3])(implicit F2: Nondeterminism[F2]): Process[F2,O3] =
      connect(chan)(q) 

    /** Feed this `Process` through the given `Channel`, using `q` to control the queueing strategy. */
    def connect[F2[x]>:F[x],O2,O3](chan: Channel[F2,O,O2])(q: Wye[O,O2,O3])(implicit F2: Nondeterminism[F2]): Process[F2,O3] =
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

  implicit class SplitProcessSyntax[F[_],A,B](self: Process[F, A \/ B]) {

    /** Feed the left side of this `Process` through the given `Channel`, using `q` to control the queueing strategy. */
    def connectL[F2[x]>:F[x],A2,O](chan: Channel[F2,A,A2])(q: Wye[B,A2,O])(implicit F2: Nondeterminism[F2]): Process[F2,O] = {
      val p: Process[F2, (Option[B], F2[Option[A2]])] = 
        self.zipWith(chan) { (ab,f) => 
          ab.fold(a => (None, F2.map(f(a))(Some(_))), 
                  b => (Some(b), F2.pure(None)))
        }
      p map { (p: (Option[B], F2[Option[A2]])) => (p._1, (o2: Option[B]) => p._2) } enqueue (
        q.attachL(process1.stripNone[B]).attachR(process1.stripNone[A2])
      )
    }

    /** Feed the right side of this `Process` through the given `Channel`, using `q` to control the queueing strategy. */
    def connectR[F2[x]>:F[x],B2,O](chan: Channel[F2,B,B2])(q: Wye[A,B2,O])(implicit F2: Nondeterminism[F2]): Process[F2,O] =
      self.map(_.swap).connectL(chan)(q)

    /** 
     * Feed the left side of this `Process` to a `Sink`, allowing up to `maxUnacknowledged`
     * elements to enqueue at the `Sink` before blocking on the `Sink`. 
     */
    def drainL[F2[x]>:F[x]](maxUnacknowledged: Int)(s: Sink[F2,A])(implicit F2: Nondeterminism[F2]): Process[F2,B] =
      self.connectL(s)(wye.drainR(maxUnacknowledged)) 

    /** 
     * Feed the right side of this `Process` to a `Sink`, allowing up to `maxUnacknowledged`
     * elements to enqueue at the `Sink` before blocking on the `Sink`. 
     */
    def drainR[F2[x]>:F[x]](maxUnacknowledged: Int)(s: Sink[F2,B])(implicit F2: Nondeterminism[F2]): Process[F2,A] =
      self.connectR(s)(wye.drainR(maxUnacknowledged)) 
  }

  /**
   * This class provides infix syntax specific to `Process[Task, _]`.
   */
  implicit class SourceSyntax[O](self: Process[Task, O]) {

    /** 
     * Feed this `Process` through the given `Channel`, using `q` to 
     * control the queueing strategy. The `q` receives the duration
     * since since the start of `self`, which may be used to decide 
     * whether to block on the channel or allow more inputs to enqueue.
     */
    def connectTimed[O2,O3](chan: Channel[Task,O,O2])(q: Wye[(O,Duration),O2,O3]): Process[Task,O3] =
      self.zip(duration).connect(chan.contramap(_._1))(q)

    /** 
     * Feed this `Process` through the given `Channel`, blocking on any
     * queued values sent to the channel if either a response has not been 
     * received within `maxAge` or if `maxSize` elements have enqueued.
     */
    def connectTimed[O2](maxAge: Duration, maxSize: Int = Int.MaxValue)(chan: Channel[Task,O,O2]): Process[Task,O2] = 
      self.connectTimed(chan)(wye.timedQueue(maxAge, maxSize).contramapL(_._2))

    /** Infix syntax for `Process.forwardFill`. */
    def forwardFill: Process[Task,O] = Process.forwardFill(self) 

    /** Infix syntax for `Process.toTask`. */
    def toTask: Task[O] = Process.toTask(self) 
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
    def toSource: Process[Task,O] = 
      emitSeq(toIndexedSeq.map(o => Task.delay(o))).eval
  }

  /**
   * This class provides infix syntax specific to `Process1`.
   */
  implicit class Process1Syntax[I,O](self: Process1[I,O]) {

    /** Apply this `Process` to an `Iterable`. */
    def apply(input: Iterable[I]): IndexedSeq[O] =
      Process(input.toSeq: _*).pipe(self.bufferAll).disconnect.unemit._1.toIndexedSeq

    /** 
     * Transform `self` to operate on the left hand side of an `\/`, passing
     * through any values it receives on the right. Note that this halts
     * whenever `self` halts. 
     */
    def liftL[I2]: Process1[I \/ I2, O \/ I2] = 
      process1.liftL(self)

    /** 
     * Transform `self` to operate on the right hand side of an `\/`, passing
     * through any values it receives on the left. Note that this halts
     * whenever `self` halts.
     */
    def liftR[I0]: Process1[I0 \/ I, I0 \/ O] = 
      process1.liftR(self)

    /** 
     * Feed a single input to this `Process1`.
     */
    def feed1(i: I): Process1[I,O] = 
      process1.feed1(i)(self)
  }

  /**
   * This class provides infix syntax specific to `Tee`. We put these here
   * rather than trying to cram them into `Process` itself using implicit
   * equality witnesses. This doesn't work out so well due to variance
   * issues.
   */
  implicit class TeeSyntax[I,I2,O](self: Tee[I,I2,O]) {

    /** Transform the left input to a `Tee`. */
    def contramapL[I0](f: I0 => I): Tee[I,I2,O] = 
      self.contramapL_(f).asInstanceOf[Tee[I,I2,O]]

    /** Transform the right input to a `Tee`. */
    def contramapR[I3](f: I3 => I2): Tee[I,I3,O] = 
      self.contramapR_(f).asInstanceOf[Tee[I,I3,O]]
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
      case h@Halt(_) => h
      case Emit(h, t) => Emit(h, t.detachL)
      case Await(req, recv, fb, c) => (req.tag: @annotation.switch) match {
        case 0 => fb.detachL
        case 1 => Await(Get[I2], recv andThen (_ detachL), fb.detachL, c.detachL)
        case 2 => Await(Get[I2], (That(_:I2)) andThen recv andThen (_ detachL), fb.detachL, c.detachL)
      }
    }

    /**
     * Convert a `Wye` to a `Process1`, by converting requests for the
     * right input into normal termination. Note that `Both` requests are rewritten
     * to fetch from the only input.
     */
    def detachR: Process1[I,O] = self match {
      case h@Halt(_) => h
      case Emit(h, t) => Emit(h, t.detachR)
      case Await(req, recv, fb, c) => (req.tag: @annotation.switch) match {
        case 0 => Await(Get[I], recv andThen (_ detachR), fb.detachR, c.detachR)
        case 1 => fb.detachR
        case 2 => Await(Get[I], (This(_:I)) andThen recv andThen (_ detachR), fb.detachR, c.detachR)
      }
    }

    /** 
     * Transform the left input of the given `Wye` using a `Process1`. 
     */
    def attachL[I0](f: Process1[I0,I]): Wye[I0, I2, O] = 
      wye.attachL(f)(self)

    /** 
     * Transform the right input of the given `Wye` using a `Process1`. 
     */
    def attachR[I1](f: Process1[I1,I2]): Wye[I, I1, O] = 
      wye.attachR(f)(self)

    /** Transform the left input to a `Wye`. */
    def contramapL[I0](f: I0 => I): Wye[I0, I2, O] =
      contramapL_(f)

    /** Transform the right input to a `Wye`. */
    def contramapR[I3](f: I3 => I2): Wye[I, I3, O] =
      contramapR_(f)

    private[stream] def contramapL_[I0](f: I0 => I): Wye[I0, I2, O] = 
      self.attachL(process1.lift(f))

    private[stream] def contramapR_[I3](f: I3 => I2): Wye[I, I3, O] = 
      self.attachR(process1.lift(f))
  }

  implicit class ChannelSyntax[F[_],I,O](self: Channel[F,I,O]) {
    /** Transform the input of this `Channel`. */
    def contramap[I0](f: I0 => I): Channel[F,I0,O] = 
      self.map(f andThen _)

    /** Transform the output of this `Channel` */
    def mapOut[O2](f: O => O2)(implicit F: Functor[F]): Channel[F,I,O2] = 
      self.map(_ andThen F.lift(f))
  }

  implicit class ChanneledProcess[F[_],O,O2](self: Process[F,(O, O => F[O2])]) {
    def enqueue[O3](q: Wye[O,O2,O3])(implicit F: Nondeterminism[F]): Process[F,O3] = go(self, q, Queue(), Queue())

    private def go[F3[_],O,O2,O3](src: Process[F3,(O,O => F3[O2])], q: Wye[O,O2,O3], bufIn: Seq[O], bufOut: Queue[F3[O2]])(
                  implicit F3: Nondeterminism[F3]): Process[F3,O3] = {
      try q match {
        case Emit(out, q2) => emitAll(out) ++ go(src, q2, bufIn, bufOut)
        case Halt(e) => src.killBy(e)
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
              case AwaitBoth(recv,fb,c) => (None, Some(recv(This(o))))
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
                    case Halt(e) => (emitAll(bufOut).eval |> q2.detachL).causedBy(e)
                    case AwaitF(reqsrc,recvsrc,fbsrc,csrc) =>
                      val (outH, outT) = bufOut.dequeue
                      await(F3.choose(reqsrc, outH))(
                        _.fold(
                          { case (p,ro) => go(recvsrc(p), q2, bufIn2, ro +: outT) },
                          { case (rp,o2) => go(await(rp)(recvsrc, fbsrc, csrc), recv(That(o2)), bufIn2, outT) }
                        ),
                        go(fbsrc, q2, bufIn2, bufOut),
                        go(csrc, q2, bufIn2, bufOut)
                      )
                    case _ => sys.error("unpossible!")
                  }
                }
              case Halt(e) => srcT.killBy(e)
              case Emit(h, t) => emitAll(h) ++ go(srcT, t, bufIn2, bufOut)
            }
          }
      }
      catch { case e: Throwable => src.killBy(e) }
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
    def eval: Process[F,O] = {
      def go(cur: Process[F,F[O]], fallback: Process[F,O], cleanup: Process[F,O]): Process[F,O] =
        cur match {
          case Halt(e) => e match {
            case End => fallback
            case _ => cleanup.causedBy(e)
          }
          case Emit(h, t) => 
            if (h.isEmpty) go(t, fallback, cleanup)
            else await[F,O,O](h.head)(o => Emit(List(o), go(Emit(h.tail, t), fallback, cleanup)), fallback, cleanup)
          case Await(req,recv,fb,c) => 
            await(req)(recv andThen (go(_, fb.eval, c.eval)), fb.eval, c.eval)
        }
      go(self, halt, halt)
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

  /** 
   * Produce `p` lazily, guarded by a single `Await`. Useful if 
   * producing the process involves allocation of some mutable
   * resource we want to ensure is accessed in a single-threaded way.
   */
  def suspend[A](p: => Process[Task, A]): Process[Task, A] =
    await(Task.now {})(_ => p) 

  /** 
   * Feed the output of `f` back in as its input. Note that deadlock 
   * can occur if `f` tries to read from the looped back input
   * before any output has been produced.
   */
  def fix[A](f: Process[Task,A] => Process[Task,A]): Process[Task,A] = Process.suspend {
    val (snk, q) = actor.localQueue[A]
    f(q).map { a => snk ! message.queue.enqueue(a); a }
  }

  /** 
   * When `condition` is `true`, lets through any values in this process, otherwise blocks
   * until `condition` becomes true again. While condition is `true`, the returned `Process`
   * will listen asynchronously for the condition to become false again. There is infix
   * syntax for this as well, `p.when(condition)` has the same effect. 
   */
  def when[F[_],O](condition: Process[F,Boolean])(p: Process[F,O]): Process[F,O] = 
    p.when(condition)

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

  private[stream] def rethrow[F[_],A](f: F[Throwable \/ A])(implicit F: Nondeterminism[F], E: Catchable[F]): F[A] =
    F.bind(f)(_.fold(E.fail, F.pure(_))) 

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

