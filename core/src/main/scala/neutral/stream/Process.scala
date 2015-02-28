package neutral.stream

import Cause._
import scala.annotation.tailrec
import scala.collection.SortedMap
import scalaz.concurrent.{Actor, Strategy, Task}
import neutral.stream.process1.Await1
import Util._

/**
 * An effectful stream of `O` values. In between emitting values
 * a `Process` may request evaluation of `F` effects.
 * A `Process[Nothing,A]` is a pure `Process` with no effects.
 * A `Process[Task,A]` may have `Task` effects. A `Process`
 * halts due to some `Cause`, generally `End` (indicating normal
 * termination) or `Error(t)` for some `t: Throwable` indicating
 * abnormal termination due to some uncaught error.
 */
sealed trait Process[+F[_], +O]
  extends Process1Ops[F,O]
          with TeeOps[F,O] {

 import neutral.stream.Process._
 import neutral.stream.Util._

  /**
   * Generate a `Process` dynamically for each output of this `Process`, and
   * sequence these processes using `append`.
   */
  final def flatMap[F2[x] >: F[x], O2](f: O => Process[F2, O2]): Process[F2, O2] = {
    // Util.debug(s"FMAP $this")
    this match {
      case Halt(_) => this.asInstanceOf[Process[F2, O2]]
      case Emit(os) if os.isEmpty => this.asInstanceOf[Process[F2, O2]]
      case Emit(os) => os.tail.foldLeft(Try(f(os.head)))((p, n) => p ++ Try(f(n)))
      case aw@Await(_, _) => aw.extend(_ flatMap f)
      case ap@Append(p, n) => ap.extend(_ flatMap f)
    }
  }
  /** Transforms the output values of this `Process` using `f`. */
  final def map[O2](f: O => O2): Process[F, O2] =
    flatMap { o => emit(f(o))}

  /**
   * If this process halts due to `Cause.End`, runs `p2` after `this`.
   * Otherwise halts with whatever caused `this` to `Halt`.
   */
  final def append[F2[x] >: F[x], O2 >: O](p2: => Process[F2, O2]): Process[F2, O2] = {
    onHalt {
      case End => p2
      case cause => Halt(cause)
    }
  }

  /** Alias for `append` */
  final def ++[F2[x] >: F[x], O2 >: O](p2: => Process[F2, O2]): Process[F2, O2] = append(p2)

  /** Alias for `append` */
  final def fby[F2[x] >: F[x], O2 >: O](p2: => Process[F2, O2]): Process[F2, O2] = append(p2)

  /**
   * Run one step of an incremental traversal of this `Process`.
   * This function is mostly intended for internal use. As it allows
   * a `Process` to be observed and captured during its execution,
   * users are responsible for ensuring resource safety.
   */
  final def step: HaltOrStep[F, O] = {
    def go(cur: Process[F,O], stack: Vector[Cause => Trampoline[Process[F,O]]]) : HaltOrStep[F,O] = {
      if (stack.nonEmpty) cur match {
        case Halt(cause) => go(Try(stack.head(cause).run), stack.tail)
        case Emit(os) if os.isEmpty => go(Try(stack.head(End).run), stack.tail)
        case emt@(Emit(os)) => Step(emt,Cont(stack))
        case awt@Await(_,_) => Step(awt,Cont(stack))
        case Append(h,st) => go(h, st fast_++ stack)
      } else cur match {
        case hlt@Halt(cause) => hlt
        case emt@Emit(os) if (os.isEmpty) => halt0
        case emt@Emit(os) => Step(emt,Cont(Vector.empty))
        case awt@Await(_,_) => Step(awt,Cont(Vector.empty))
        case Append(h,st) => go(h,st)
      }
    }
    go(this,Vector.empty)

  }

  /**
   * `p.suspendStep` propagates exceptions to `p`.
   */
  final def suspendStep: Process0[HaltOrStep[F, O]] =
    halt onHalt {
      case End => emit(step)
      case early: EarlyCause => emit(injectCause(early).step)
    }

  /**
   * When this `Process` halts, call `f` to produce the next state.
   * Note that this function may be used to swallow or handle errors.
   */
  final def onHalt[F2[x] >: F[x], O2 >: O](f: Cause => Process[F2, O2]): Process[F2, O2] = {
     val next = (t: Cause) => Trampoline.delay(Try(f(t)))
     this match {
       case Append(h, stack) => Append(h, stack :+ next)
       case emt@Emit(_)      => Append(emt, Vector(next))
       case awt@Await(_, _)  => Append(awt, Vector(next))
       case hlt@Halt(rsn)    => Append(hlt, Vector(next))
     }
  }


  //////////////////////////////////////////////////////////////////////////////////////
  //
  // Pipe and Tee
  //
  /////////////////////////////////////////////////////////////////////////////////////


  /**
   * Feed the output of this `Process` as input of `p1`. The implementation
   * will fuse the two processes, so this process will only generate
   * values as they are demanded by `p1`. If `p1` signals termination, `this`
   * is killed with same reason giving it an opportunity to cleanup.
   */
  final def pipe[O2](p1: Process1[O, O2]): Process[F, O2] =
    p1.suspendStep.flatMap({ s1 =>
      s1 match {
        case s@Step(awt1@Await1(rcv1), cont1) =>
          val nextP1 = s.toProcess
          this.step match {
            case Step(awt@Await(_, _), cont) => awt.extend(p => (p +: cont) pipe nextP1)
            case Step(Emit(os), cont)        => cont.continue pipe process1.feed(os)(nextP1)
            case hlt@Halt(End)               => hlt pipe nextP1.disconnect(Kill).swallowKill
            case hlt@Halt(rsn: EarlyCause)   => hlt pipe nextP1.disconnect(rsn)
          }

        case Step(emt@Emit(os), cont)      =>
          // When the pipe is killed from the outside it is killed at the beginning or after emit.
          // This ensures that Kill from the outside is not swallowed.
          emt onHalt {
            case End => this.pipe(cont.continue)
            case early => this.pipe(Halt(early) +: cont).causedBy(early)
          }

        case Halt(rsn)           => this.kill onHalt { _ => Halt(rsn) }
      }
    })

  /** Operator alias for `pipe`. */
  final def |>[O2](p2: Process1[O, O2]): Process[F, O2] = pipe(p2)

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
   *
   * If at any point `t` terminates with cause `c`, both sides are killed, and
   * the resulting `Process` terminates with `c`.
   */
  final def tee[F2[x] >: F[x], O2, O3](p2: Process[F2, O2])(t: Tee[O, O2, O3]): Process[F2, O3] = {
    import neutral.stream.tee.{AwaitL, AwaitR, disconnectL, disconnectR, feedL, feedR}
    t.suspendStep flatMap { ts =>
      ts match {
        case s@Step(AwaitL(_), contT) => this.step match {
          case Step(awt@Await(rq, rcv), contL) => awt.extend { p => (p  +: contL).tee(p2)(s.toProcess) }
          case Step(Emit(os), contL)           => contL.continue.tee(p2)(feedL[O, O2, O3](os)(s.toProcess))
          case hlt@Halt(End)              => hlt.tee(p2)(disconnectL(Kill)(s.toProcess).swallowKill)
          case hlt@Halt(rsn: EarlyCause)  => hlt.tee(p2)(disconnectL(rsn)(s.toProcess))
        }

        case s@Step(AwaitR(_), contT) => p2.step match {
          case s2: Step[F2, O2]@unchecked =>
            (s2.head, s2.next) match {
              case (awt: Await[F2, Any, O2]@unchecked, contR) =>
                awt.extend { (p: Process[F2, O2]) => this.tee(p +: contR)(s.toProcess) }
              case (Emit(o2s), contR) =>
                this.tee(contR.continue.asInstanceOf[Process[F2,O2]])(feedR[O, O2, O3](o2s)(s.toProcess))
            }
          case hlt@Halt(End)              => this.tee(hlt)(disconnectR(Kill)(s.toProcess).swallowKill)
          case hlt@Halt(rsn : EarlyCause) => this.tee(hlt)(disconnectR(rsn)(s.toProcess))
        }

        case Step(emt@Emit(o3s), contT) =>
          // When the process is killed from the outside it is killed at the beginning or after emit.
          // This ensures that Kill from the outside isn't swallowed.
          emt onHalt {
            case End => this.tee(p2)(contT.continue)
            case early => this.tee(p2)(Halt(early) +: contT).causedBy(early)
          }

        case Halt(rsn)             => this.kill onHalt { _ => p2.kill onHalt { _ => Halt(rsn) } }
      }
    }
  }


  //////////////////////////////////////////////////////////////////////////////////////
  //
  // Alphabetically, Other combinators
  //
  /////////////////////////////////////////////////////////////////////////////////////

  /**
   * Catch exceptions produced by this `Process`, not including termination by `Continue`, `End`, `Kill`
   * and uses `f` to decide whether to resume a second process.
   */
  final def attempt[F2[x] >: F[x], O2](
    f: Throwable => Process[F2, O2] = (t: Throwable) => emit(t)
    ): Process[F2, O2 \/ O] =
    this.map(right) onHalt {
      case Error(t) => Try(f(t)).map(left)
      case rsn      => Halt(rsn)
    }

  /**
   * Attached `cause` when this Process terminates.  See `Cause.causedBy` for semantics.
   */
  final def causedBy(cause: Cause): Process[F, O] =
    cause.fold(this)(ec => this.onHalt(c => Halt(c.causedBy(ec))))

  /**
   * Used when a `Process1`, `Tee`, or `Wye` is terminated by awaiting
   * on a branch that is in the halted state or was killed. Such a process
   * is given the opportunity to emit any final values. All Awaits are
   * converted to terminate with `cause`
   */
  final def disconnect(cause: EarlyCause): Process0[O] =
    this.step match {
      case Step(emt@Emit(_), cont)     => emt +: cont.extend(_.disconnect(cause))
      case Step(awt@Await(_, rcv), cont) => suspend((Try(rcv(left(cause)).run) +: cont).disconnect(cause))
      case hlt@Halt(rsn)           => Halt(rsn)
    }

  /** Ignore all outputs of this `Process`. */
  final def drain: Process[F, Nothing] = flatMap(_ => halt)

  /**
   * Map over this `Process` to produce a stream of `F`-actions,
   * then evaluate these actions.
   */
  def evalMap[F2[x]>:F[x],O2](f: O => F2[O2]): Process[F2,O2] =
    map(f).eval

  /** Prepend a sequence of elements to the output of this `Process`. */
  def prepend[O2>:O](os:Seq[O2]) : Process[F,O2] = {
    if (os.nonEmpty) {
      emitAll(os) onHalt {
        case End               => this
        case cause: EarlyCause => this.step match {
          case Step(Await(_, rcv), cont) => Try(rcv(left(cause)).run) +: cont
          case Step(Emit(_), cont)       => Halt(cause) +: cont
          case Halt(rsn)             => Halt(rsn.causedBy(cause))
        }
      }
    } else this
  }



  /** Returns true, if this process is halted */
  final def isHalt: Boolean = this match {
    case Halt(_) => true
    case _ => false
  }

  /**
   * Skip the first part of the process and pretend that it ended with `early`.
   * The first part is the first `Halt` or the first `Emit` or request from the first `Await`.
   */
  private[stream] final def injectCause(early: EarlyCause): Process[F, O] = (this match {
    // Note: We cannot use `step` in the implementation since we want to inject `early` as soon as possible.
    // Eg. Let `q` be `halt ++ halt ++ ... ++ p`. `step` reduces `q` to `p` so if `injectCause` was implemented
    // by `step` then `q.injectCause` would be same as `p.injectCause`. But in our current implementation
    // `q.injectCause` behaves as `Halt(early) ++ halt ++ ... ++ p` which behaves as `Halt(early)`
    // (by the definition of `++` and the fact `early != End`).
    case Halt(rsn) => Halt(rsn.causedBy(early))
    case Emit(_) => Halt(early)
    case Await(_, rcv) => Try(rcv(left(early)).run)
    case Append(Halt(rsn), stack) => Append(Halt(rsn.causedBy(early)), stack)
    case Append(Emit(_), stack) => Append(Halt(early), stack)
    case Append(Await(_, rcv), stack) => Try(rcv(left(early)).run) +: Cont(stack)
  })

  /**
   * Causes this process to be terminated immediately with `Kill` cause,
   * giving chance for any cleanup actions to be run
   */
  final def kill: Process[F, Nothing] = injectCause(Kill).drain.causedBy(Kill)

  /**
   * Run `p2` after this `Process` completes normally, or in the event of an error.
   * This behaves almost identically to `append`, except that `p1 append p2` will
   * not run `p2` if `p1` halts with an `Error` or is killed. Any errors raised by
   * `this` are reraised after `p2` completes.
   *
   * Note that `p2` is made into a finalizer using `asFinalizer`, so we
   * can be assured it is run even when this `Process` is being killed
   * by a downstream consumer.
   */
  final def onComplete[F2[x] >: F[x], O2 >: O](p2: => Process[F2, O2]): Process[F2, O2] =
    this.onHalt { cause => p2.asFinalizer.causedBy(cause) }

  /**
   * Mostly internal use function. Ensures this `Process` is run even
   * when being `kill`-ed. Used to ensure resource safety in various
   * combinators.
   */
  final def asFinalizer: Process[F, O] = {
    def mkAwait[F[_], A, O](req: F[A])(rcv: EarlyCause \/ A => Trampoline[Process[F, O]]) = Await(req, rcv)
    step match {
      case Step(e@Emit(_), cont) => e onHalt {
        case Kill => (halt +: cont).asFinalizer.causedBy(Kill)
        case cause => (Halt(cause) +: cont).asFinalizer
      }
      case Step(Await(req, rcv), cont) => mkAwait(req) {
        case -\/(Kill) => Trampoline.delay(Await(req, rcv).asFinalizer.causedBy(Kill))
        case x => rcv(x).map(p => (p +: cont).asFinalizer)
      }
      case hlt@Halt(_) => hlt
    }
  }

  /**
   * If this `Process` completes with an error, call `f` to produce
   * the next state. `f` is responsible for reraising the error if that
   * is the desired behavior. Since this function is often used for attaching
   * resource deallocation logic, the result of `f` is made into a finalizer
   * using `asFinalizer`, so we can be assured it is run even when this `Process`
   * is being killed by a downstream consumer.
   */
  final def onFailure[F2[x] >: F[x], O2 >: O](f: Throwable => Process[F2, O2]): Process[F2, O2] =
    this.onHalt {
      case err@Error(rsn) => f(rsn).asFinalizer
      case other => Halt(other)
    }

  /**
   * Attach supplied process only if process has been killed.
   * Since this function is often used for attaching resource
   * deallocation logic, the result of `f` is made into a finalizer
   * using `asFinalizer`, so we can be assured it is run even when
   * this `Process` is being killed by a downstream consumer.
   */
  final def onKill[F2[x] >: F[x], O2 >: O](p: => Process[F2, O2]): Process[F2, O2] =
    this.onHalt {
      case Kill => p.asFinalizer
      case other => Halt(other)
    }


  /**
   * Run this process until it halts, then run it again and again, as
   * long as no errors or `Kill` occur.
   */
  final def repeat: Process[F, O] = this.append(this.repeat)

  /**
   * For anly process terminating with `Kill`, this swallows the `Kill` and replaces it with `End` termination
   */
  final def swallowKill: Process[F,O] =
    this.onHalt {
      case Kill | End => halt
      case cause => Halt(cause)
    }

  /**
   * Remove any leading emitted values from this `Process`.
   */
  @tailrec
  final def trim: Process[F,O] =
    this.step match {
      case Step(Emit(_), cont) => cont.continue.trim
      case _ => this
    }


  /**
   * Removes all emitted elements from the front of this `Process`.
   * The second argument returned by this method is guaranteed to be
   * an `Await`, `Halt` or an `Append`-- if there are multiple `Emit'`s at the
   * front of this process, the sequences are concatenated together.
   *
   * If this `Process` does not begin with an `Emit`, returns the empty
   * sequence along with `this`.
   */
  final def unemit:(Seq[O],Process[F,O]) = {
    @tailrec
    def go(cur: Process[F, O], acc: Vector[O]): (Seq[O], Process[F, O]) = {
      cur.step match {
        case Step(Emit(os),cont) => go(cont.continue, acc fast_++ os)
        case Step(awt, cont) => (acc,awt +: cont)
        case Halt(rsn) => (acc,Halt(rsn))
      }
    }
    go(this, Vector())

  }






}


object Process {


  import neutral.stream.Util._

  //////////////////////////////////////////////////////////////////////////////////////
  //
  // Algebra
  //
  /////////////////////////////////////////////////////////////////////////////////////

  type Trampoline[+A] = trampoline.Trampoline[A]
  val Trampoline = trampoline.Trampoline

  /**
   * Tags a state of process that has no appended tail, tha means can be Halt, Emit or Await
   */
  sealed trait HaltEmitOrAwait[+F[_], +O] extends Process[F, O]

  object HaltEmitOrAwait {

    def unapply[F[_], O](p: Process[F, O]): Option[HaltEmitOrAwait[F, O]] = p match {
      case emit: Emit[O@unchecked] => Some(emit)
      case halt: Halt => Some(halt)
      case aw: Await[F@unchecked, _, O@unchecked] => Some(aw)
      case _ => None
    }

  }

  /**
   * Marker trait representing process in Emit or Await state.
   * Is useful for more type safety.
   */
  sealed trait EmitOrAwait[+F[_], +O] extends Process[F, O]


  /**
   * The `Halt` constructor instructs the driver
   * that the last evaluation of Process completed with
   * supplied cause.
   */
  case class Halt(cause: Cause) extends HaltEmitOrAwait[Nothing, Nothing] with HaltOrStep[Nothing, Nothing]


  /**
   * The `Emit` constructor instructs the driver to emit
   * the given sequence of values to the output
   * and then halt execution with supplied reason.
   *
   * Instead calling this constructor directly, please use one
   * of the following helpers:
   *
   * Process.emit
   * Process.emitAll
   */
  case class Emit[+O](seq: Seq[O]) extends HaltEmitOrAwait[Nothing, O] with EmitOrAwait[Nothing, O]

  /**
   * The `Await` constructor instructs the driver to evaluate
   * `req`. If it returns successfully, `recv` is called with result on right side
   * to transition to the next state.
   *
   * In case the req terminates with failure the `Error(failure)` is passed on left side
   * giving chance for any fallback action.
   *
   * In case the process was killed before the request is evaluated `Kill` is passed on left side.
   * `Kill` is passed on left side as well as when the request is already in progress, but process was killed.
   *
   * Note that
   *
   * Instead of this constructor directly, please use:
   *
   * Process.await
   *
   */
  case class Await[+F[_], A, +O](
    req: F[A]
    , rcv: (EarlyCause \/ A) => Trampoline[Process[F, O]]
    ) extends HaltEmitOrAwait[F, O] with EmitOrAwait[F, O] {
    /**
     * Helper to modify the result of `rcv` parameter of await stack-safely on trampoline.
     */
    def extend[F2[x] >: F[x], O2](f: Process[F, O] => Process[F2, O2]): Await[F2, A, O2] =
      Await[F2, A, O2](req, r => Trampoline.suspend(rcv(r)).map(f))
  }


  /**
   * The `Append` constructor instructs the driver to continue with
   * evaluation of first step found in tail Vector.
   *
   * Instead of this constructor please use:
   *
   * Process.append
   */
  case class Append[+F[_], +O](
    head: HaltEmitOrAwait[F, O]
    , stack: Vector[Cause => Trampoline[Process[F, O]]]
    ) extends Process[F, O] {

    /**
     * Helper to modify the head and appended processes
     */
    def extend[F2[x] >: F[x], O2](f: Process[F, O] => Process[F2, O2]): Process[F2, O2] = {
      val ms = stack.map(n => (cause: Cause) => Trampoline.suspend(n(cause)).map(f))

      f(head) match {
        case HaltEmitOrAwait(p) => Append(p, ms)
        case app: Append[F2@unchecked, O2@unchecked] => Append(app.head, app.stack fast_++ ms)
      }

    }

  }

  /**
   * Marker trait representing next step of process or terminated process in `Halt`
   */
  sealed trait HaltOrStep[+F[_], +O]

  /**
   * Intermediate step of process.
   * Used to step within the process to define complex combinators.
   */
  case class Step[+F[_], +O](head: EmitOrAwait[F, O], next: Cont[F, O]) extends HaltOrStep[F, O] {
    def toProcess : Process[F,O] = Append(head.asInstanceOf[HaltEmitOrAwait[F,O]],next.stack)
  }

  /**
   * Continuation of the process. Represents process _stack_. Used in conjunction with `Step`.
   */
  case class Cont[+F[_], +O](stack: Vector[Cause => Trampoline[Process[F, O]]]) {

    /**
     * Prepends supplied process to this stack
     */
    def +:[F2[x] >: F[x], O2 >: O](p: Process[F2, O2]): Process[F2, O2] = prepend(p)

    /** alias for +: */
    def prepend[F2[x] >: F[x], O2 >: O](p: Process[F2, O2]): Process[F2, O2] = {
      if (stack.isEmpty) p
      else p match {
        case app: Append[F2@unchecked, O2@unchecked] => Append[F2, O2](app.head, app.stack fast_++ stack)
        case emt: Emit[O2@unchecked] => Append(emt, stack)
        case awt: Await[F2@unchecked, _, O2@unchecked] => Append(awt, stack)
        case hlt@Halt(_) => Append(hlt, stack)
      }
    }

    /**
     * Converts this stack to process, that is used
     * when following process with normal termination.
     */
    def continue: Process[F, O] = prepend(halt)

    /**
     * Applies transformation function `f` to all frames of this stack.
     */
    def extend[F2[_], O2](f: Process[F, O] => Process[F2, O2]): Cont[F2, O2] =
      Cont(stack.map(tf => (cause: Cause) => Trampoline.suspend(tf(cause).map(f))))


    /**
     * Returns true, when this continuation is empty, i.e. no more appends to process
     */
    def isEmpty : Boolean = stack.isEmpty

  }


  ///////////////////////////////////////////////////////////////////////////////////////
  //
  // CONSTRUCTORS
  //
  //////////////////////////////////////////////////////////////////////////////////////

  /** Alias for emitAll */
  def apply[O](o: O*): Process0[O] = emitAll(o)

  /**
   * Await the given `F` request and use its result.
   * If you need to specify fallback, use `awaitOr`
   */
  def await[F[_], A, O](req: F[A])(rcv: A => Process[F, O]): Process[F, O] =
    awaitOr(req)(Halt.apply)(rcv)

  /**
   * Await a request, and if it fails, use `fb` to determine the next state.
   * Otherwise, use `rcv` to determine the next state.
   */
  def awaitOr[F[_], A, O](req: F[A])(
    fb: EarlyCause => Process[F, O]
    )(rcv: A => Process[F, O]): Process[F, O] = {
    Await(req, (r: EarlyCause \/ A) => Trampoline.delay(Try(r.fold(ec => fb(ec), a => rcv(a)))))
  }

  /** The `Process1` which awaits a single input, emits it, then halts normally. */
  def await1[I]: Process1[I, I] =
    receive1(emit)

  /** Like `await1`, but consults `fb` when await fails to receive an `I` */
  def await1Or[I](fb: => Process1[I, I]): Process1[I, I] =
    receive1Or(fb)(emit)

  /** The `Wye` which request from both branches concurrently. */
  def awaitBoth[I, I2]: Wye[I, I2, ReceiveY[I, I2]] =
    await(Both[I, I2])(emit)

  /** The `Tee` which requests from the left branch, emits this value, then halts. */
  def awaitL[I]: Tee[I, Any, I] =
    await(L[I])(emit)

  /** The `Tee` which requests from the right branch, emits this value, then halts. */
  def awaitR[I2]: Tee[Any, I2, I2] =
    await(R[I2])(emit)

  /** The `Process` which emits the single value given, then halts. */
  def emit[O](o: O): Process0[O] = Emit(Vector(o))

  /** The `Process` which emits the given sequence of values, then halts. */
  def emitAll[O](os: Seq[O]): Process0[O] = Emit(os)

  /** The `Process` which emits no values and halts immediately with the given exception. */
  def fail(rsn: Throwable): Process0[Nothing] = Halt(Error(rsn))

  /** `halt` but with precise type. */
  private[stream] val halt0: Halt = Halt(End)

  /** The `Process` which emits no values and signals normal termination. */
  val halt: Process0[Nothing] = halt0

  /** Alias for `halt`. */
  def empty[F[_],O]: Process[F, O] = halt

  /**
   * The `Process1` which awaits a single input and passes it to `rcv` to
   * determine the next state.
   */
  def receive1[I, O](rcv: I => Process1[I, O]): Process1[I, O] =
    await(Get[I])(rcv)

  /** Like `receive1`, but consults `fb` when it fails to receive an input. */
  def receive1Or[I, O](fb: => Process1[I, O])(rcv: I => Process1[I, O]): Process1[I, O] =
    awaitOr(Get[I])((rsn: EarlyCause) => fb.causedBy(rsn))(rcv)

  ///////////////////////////////////////////////////////////////////////////////////////
  //
  // CONSTRUCTORS -> Helpers
  //
  //////////////////////////////////////////////////////////////////////////////////////

  /** `Writer` based version of `await1`. */
  def await1W[A]: Writer1[Nothing, A, A] =
    liftW(Process.await1[A])

  /** `Writer` based version of `awaitL`. */
  def awaitLW[I]: TeeW[Nothing, I, Any, I] =
    liftW(Process.awaitL[I])

  /** `Writer` based version of `awaitR`. */
  def awaitRW[I2]: TeeW[Nothing, Any, I2, I2] =
    liftW(Process.awaitR[I2])

  /** `Writer` based version of `awaitBoth`. */
  def awaitBothW[I, I2]: WyeW[Nothing, I, I2, ReceiveY[I, I2]] =
    liftW(Process.awaitBoth[I, I2])

  /**
   * The infinite `Process`, always emits `a`.
   * If for performance reasons it is good to emit `a` in chunks,
   * specify size of chunk by `chunkSize` parameter
   */
  def constant[A](a: A, chunkSize: Int = 1): Process0[A] = {
    lazy val go: Process0[A] =
      if (chunkSize.max(1) == 1) emit(a) ++ go
      else emitAll(List.fill(chunkSize)(a)) ++ go
    go
  }

  /** A `Writer` which emits one value to the output. */
  def emitO[O](o: O): Process0[Nothing \/ O] =
    Process.emit(right(o))

  /** A `Writer` which writes the given value. */
  def emitW[W](s: W): Process0[W \/ Nothing] =
    Process.emit(left(s))

  /** A `Process` which emits `n` repetitions of `a`. */
  def fill[A](n: Int)(a: A, chunkSize: Int = 1): Process0[A] = {
    val chunkN = chunkSize max 1
    val chunk = emitAll(List.fill(chunkN)(a)) // we can reuse this for each step
    def go(m: Int): Process0[A] =
      if (m >= chunkN) chunk ++ go(m - chunkN)
      else if (m <= 0) halt
      else emitAll(List.fill(m)(a))
    go(n max 0)
  }

  /**
   * Produce a continuous stream from a discrete stream by using the
   * most recent value.
   */
  def forwardFill[A](p: Process[Task, A])(implicit S: Strategy): Process[Task, A] =
    async.toSignal(p).continuous

  /**
   * An infinite `Process` that repeatedly applies a given function
   * to a start value. `start` is the first value emitted, followed
   * by `f(start)`, then `f(f(start))`, and so on.
   */
  def iterate[A](start: A)(f: A => A): Process0[A] =
    emit(start) ++ iterate(f(start))(f)

  /**
   * Like [[iterate]], but takes an effectful function for producing
   * the next state. `start` is the first value emitted.
   */
  def iterateEval[F[_], A](start: A)(f: A => F[A]): Process[F, A] =
    emit(start) ++ await(f(start))(iterateEval(_)(f))

  /** Promote a `Process` to a `Writer` that writes nothing. */
  def liftW[F[_], A](p: Process[F, A]): Writer[F, Nothing, A] =
    p.map(right)

  /**
   * Promote a `Process` to a `Writer` that writes and outputs
   * all values of `p`.
   */
  def logged[F[_], A](p: Process[F, A]): Writer[F, A, A] =
    p.flatMap(a => emitAll(Vector(left(a), right(a))))

  /** Lazily produce the range `[start, stopExclusive)`. If you want to produce the sequence in one chunk, instead of lazily, use `emitAll(start until stopExclusive)`.  */
  def range(start: Int, stopExclusive: Int, by: Int = 1): Process0[Int] =
    unfold(start)(i => if (i < stopExclusive) Some((i, i + by)) else None)

  /**
   * Lazily produce a sequence of nonoverlapping ranges, where each range
   * contains `size` integers, assuming the upper bound is exclusive.
   * Example: `ranges(0, 1000, 10)` results in the pairs
   * `(0, 10), (10, 20), (20, 30) ... (990, 1000)`
   *
   * Note: The last emitted range may be truncated at `stopExclusive`. For
   * instance, `ranges(0,5,4)` results in `(0,4), (4,5)`.
   *
   * @throws IllegalArgumentException if `size` <= 0
   */
  def ranges(start: Int, stopExclusive: Int, size: Int): Process0[(Int, Int)] = {
    require(size > 0, "size must be > 0, was: " + size)
    unfold(start){
      lower =>
        if (lower < stopExclusive)
          Some((lower -> ((lower+size) min stopExclusive), lower+size))
        else
          None
    }
  }

  /**
   * Delay running `p` until `awaken` becomes true for the first time.
   * The `awaken` process may be discrete.
   */
  def sleepUntil[F[_], A](awaken: Process[F, Boolean])(p: Process[F, A]): Process[F, A] =
    awaken.dropWhile(!_).once.flatMap(_ => p)

  /**
   * A supply of `Long` values, starting with `initial`.
   * Each read is guaranteed to return a value which is unique
   * across all threads reading from this `supply`.
   */
  def supply(initial: Long): Process[Task, Long] = {
    import java.util.concurrent.atomic.AtomicLong
    val l = new AtomicLong(initial)
    repeatEval { Task.delay { l.getAndIncrement }}
  }

  /** A `Writer` which writes the given value; alias for `emitW`. */
  def tell[S](s: S): Process0[S \/ Nothing] =
    emitW(s)

  /** Produce a (potentially infinite) source from an unfold. */
  def unfold[S, A](s0: S)(f: S => Option[(A, S)]): Process0[A] = {
    def go(s: S): Process0[A] =
      f(s) match {
        case Some((a, sn)) => emit(a) ++ go(sn)
        case None => halt
      }
    suspend(go(s0))
  }

  /** Like [[unfold]], but takes an effectful function. */
  def unfoldEval[F[_], S, A](s0: S)(f: S => F[Option[(A, S)]]): Process[F, A] = {
    def go(s: S): Process[F, A] =
      await(f(s)) {
        case Some((a, sn)) => emit(a) ++ go(sn)
        case None => halt
      }
    suspend(go(s0))
  }

  //////////////////////////////////////////////////////////////////////////////////////
  //
  // ENV, Tee, Wye et All
  //
  /////////////////////////////////////////////////////////////////////////////////////


  case class Env[-I, -I2]() {
    sealed trait Y[-X] {
      def tag: Int
      def fold[R](l: => R, r: => R, both: => R): R
    }
    sealed trait T[-X] extends Y[X]
    sealed trait Is[-X] extends T[X]
    case object Left extends Is[I] {
      def tag = 0
      def fold[R](l: => R, r: => R, both: => R): R = l
    }
    case object Right extends T[I2] {
      def tag = 1
      def fold[R](l: => R, r: => R, both: => R): R = r
    }
    case object Both extends Y[ReceiveY[I, I2]] {
      def tag = 2
      def fold[R](l: => R, r: => R, both: => R): R = both
    }
  }


  private val Left_  = Env[Any, Any]().Left
  private val Right_ = Env[Any, Any]().Right
  private val Both_  = Env[Any, Any]().Both

  def Get[I]: Env[I, Any]#Is[I] = Left_
  def L[I]: Env[I, Any]#Is[I] = Left_
  def R[I2]: Env[Any, I2]#T[I2] = Right_
  def Both[I, I2]: Env[I, I2]#Y[ReceiveY[I, I2]] = Both_


  //////////////////////////////////////////////////////////////////////////////////////
  //
  // SYNTAX
  //
  /////////////////////////////////////////////////////////////////////////////////////

  /** Adds syntax for `Channel`. */
  implicit class ChannelSyntax[F[_],I,O](val self: Channel[F,I,O]) extends AnyVal {
    /** Transform the input of this `Channel`. */
    def contramap[I0](f: I0 => I): Channel[F,I0,O] =
      self.map(f andThen _)
  }

  implicit class ProcessSyntax[F[_],O](val self: Process[F,O]) extends AnyVal {
    /** Feed this `Process` through the given effectful `Channel`. */
    def through[F2[x]>:F[x],O2](f: Channel[F2,O,O2]): Process[F2,O2] =
        self.zipWith(f)((o,f) => f(o)).eval

    /**
     * Feed this `Process` through the given effectful `Channel`, signaling
     * termination to `f` via `None`. Useful to allow `f` to flush any
     * buffered values to the output when it detects termination, see
     * [[neutral.stream.io.bufferedChannel]] combinator.
     */
    def throughOption[F2[x]>:F[x],O2](f: Channel[F2,Option[O],O2]): Process[F2,O2] =
      self.terminated.through(f)

    /** Attaches `Sink` to this  `Process`  */
    def to[F2[x]>:F[x]](f: Sink[F2,O]): Process[F2,Unit] =
      through(f)

    /** Attach a `Sink` to the output of this `Process` but echo the original. */
    def observe[F2[x]>:F[x]](f: Sink[F2,O]): Process[F2,O] =
      self.zipWith(f)((o,f) => (o,f(o))).flatMap { case (orig,action) => emit(action).eval.drain ++ emit(orig) }

  }

  /**
   * Provides infix syntax for `eval: Process[F,F[O]] => Process[F,O]`
   */
  implicit class EvalProcess[F[_], O](val self: Process[F, F[O]]) extends AnyVal {

    /**
     * Evaluate the stream of `F` actions produced by this `Process`.
     * This sequences `F` actions strictly--the first `F` action will
     * be evaluated before work begins on producing the next `F`
     * action. To allow for concurrent evaluation, use `sequence`
     * or `gather`.
     *
     * If evaluation of `F` results to `Terminated(cause)`
     * the evaluation of the stream is terminated with `cause`
     */
    def eval: Process[F, O] = {
      self.flatMap(f=> await(f)(emit)).onHalt {
        case Error(Terminated(cause)) => Halt(cause)
        case cause => Halt(cause)
      }
    }
  }

  /**
   * This class provides infix syntax specific to `Process0`.
   */
  implicit class Process0Syntax[O](val self: Process0[O]) extends AnyVal {

    /** Converts this `Process0` to a `Vector`. */
    def toVector: Vector[O] =
      self.unemit match {
        case (_, Halt(Error(rsn))) => throw rsn
        case (os, _) => os.toVector
      }

    /** Converts this `Process0` to an `IndexedSeq`. */
    def toIndexedSeq: IndexedSeq[O] = toVector

    /** Converts this `Process0` to a `List`. */
    def toList: List[O] = toVector.toList

    /** Converts this `Process0` to a `Seq`. */
    def toSeq: Seq[O] = toVector

    /** Converts this `Process0` to a `Stream`. */
    def toStream: Stream[O] = {
      def go(p: Process0[O]): Stream[O] =
        p.step match {
          case s: Step[Nothing, O] =>
            s.head match {
              case Emit(os) => os.toStream #::: go(s.next.continue)
              case _ => sys.error("impossible")
            }
          case Halt(Error(rsn)) => throw rsn
          case Halt(_) => Stream.empty
        }
      go(self)
    }

    /** Converts this `Process0` to a `Map`. */
    def toMap[K, V](implicit isKV: O <:< (K, V)): Map[K, V] = toVector.toMap(isKV)

    /** Converts this `Process0` to a `SortedMap`. */
    def toSortedMap[K, V](implicit isKV: O <:< (K, V), ord: Ordering[K]): SortedMap[K, V] =
      SortedMap(toVector.asInstanceOf[Seq[(K, V)]]: _*)

    def toSource: Process[Task, O] = self

    @deprecated("liftIO is deprecated in favor of toSource. It will be removed in a future release.", "0.7")
    def liftIO: Process[Task, O] = self
  }

  /** Syntax for Sink, that is specialized for Task */
  implicit class SinkTaskSyntax[I](val self: Sink[Task,I]) extends AnyVal {
    /** converts sink to sink that first pipes received `I0` to supplied p1 */
    def pipeIn[I0](p1: Process1[I0, I]): Sink[Task, I0] = Process.suspend {
      // Note: Function `f` from sink `self` may be used for more than 1 element emitted by `p1`.
      @volatile var cur = p1.step
      @volatile var lastF: Option[I => Task[Unit]] = None
      self.takeWhile { _ =>
        cur match {
          case Halt(Cause.End) => false
          case Halt(cause)     => throw new Cause.Terminated(cause)
          case _               => true
        }
      } map { (f: I => Task[Unit]) =>
        lastF = Some(f)
        (i0: I0) => Task.suspend {
          cur match {
            case Halt(_) => sys.error("Impossible")
            case Step(Emit(piped), cont) =>
              cur = process1.feed1(i0) { cont.continue }.step
              piped.toList.traverseTask_(f)
            case Step(hd, cont) =>
              val (piped, tl) = process1.feed1(i0)(hd +: cont).unemit
              cur = tl.step
              piped.toList.traverseTask_(f)
          }
        }
      } onHalt {
        case Cause.Kill =>
          lastF map { f =>
            cur match {
              case Halt(_) => sys.error("Impossible (2)")
              case s@Step(_, _) =>
                s.toProcess.disconnect(Cause.Kill).evalMap(f).drain
            }
          } getOrElse Halt(Cause.Kill)
        case Cause.End  => halt
        case c@Cause.Error(_) => halt.causedBy(c)
      }
    }
  }


  /**
   * This class provides infix syntax specific to `Process1`.
   */
  implicit class Process1Syntax[I,O](val self: Process1[I,O]) extends AnyVal {

    /** Apply this `Process` to an `Iterable`. */
    def apply(input: Iterable[I]): IndexedSeq[O] =
      Process(input.toSeq: _*).pipe(self).toIndexedSeq

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

    /** Transform the input of this `Process1`. */
    def contramap[I2](f: I2 => I): Process1[I2,O] =
      process1.lift(f).pipe(self)
  }


  /**
   * Syntax for processes that have its effects wrapped in Task
   */
  implicit class SourceSyntax[O](val self: Process[Task, O])   extends WyeOps[O] {

    /**
     * Produce a continuous stream from a discrete stream by using the
     * most recent value.
     */
    def forwardFill(implicit S: Strategy): Process[Task, O] =
      async.toSignal(self).continuous

    /**
     * Asynchronous execution of this Process. Note that this method is not resource safe unless
     * callback is called with _left_ side completed. In that case it is guaranteed that all cleanups
     * has been successfully completed.
     * User of this method is responsible for any cleanup actions to be performed by running the
     * next Process obtained on right side of callback.
     *
     * This method returns a function, that when applied, causes the running computation to be interrupted.
     * That is useful of process contains any asynchronous code, that may be left with incomplete callbacks.
     * If the evaluation of the process is interrupted, then the interruption is only active if the callback
     * was not completed before, otherwise interruption is no-op.
     *
     * There is chance, that cleanup code of intermediate `Await` will get called twice on interrupt, but
     * always at least once. The second cleanup invocation in that case may run on different thread, asynchronously.
     *
     *
     * @param cb  result of the asynchronous evaluation of the process. Note that, the callback is never called
     *            on the right side, if the sequence is empty.
     * @param S  Strategy to use when evaluating the process. Note that `Strategy.Sequential` may cause SOE.
     * @return   Function to interrupt the evaluation
     */
    protected[stream] final def runAsync(
      cb: Cause \/ (Seq[O], Cont[Task,O]) => Unit
      )(implicit S: Strategy): (EarlyCause) => Unit = {

          sealed trait M
          case class AwaitDone(res: Throwable \/ Any, awt: Await[Task, Any, O], cont: Cont[Task,O]) extends M
          case class Interrupt(cause: EarlyCause) extends M

          //forward referenced actor here
          var a: Actor[M] = null

          // Set when the executin has been terminated with reason for termination
          var completed: Option[Cause] = None

          // contains reference that eventually builds
          // a cleanup when the last await was interrupted
          // this is consulted only, if await was interrupted
          // volatile marked because of the first usage outside of actor
          @volatile var cleanup: (EarlyCause => Process[Task,O]) = (c:EarlyCause) => Halt(c)

          // runs single step of process.
          // completes with callback if process is `Emit` or `Halt`.
          // or asynchronously executes the Await and send result to actor `a`
          // It returns on left side reason with which this process terminated,
          // or on right side the cleanup code to be run when interrupted.
          @tailrec
          def runStep(p: Process[Task, O]): Cause \/ (EarlyCause => Process[Task,O]) = {
            val step = p.step
            step match {
              case Step(Emit(Seq()), cont)         => runStep(cont.continue)
              case Step(Emit(h), cont)             => S(cb(right((h, cont)))); left(End)
              case Step(awt@Await(req, rcv), cont) =>
                req.runAsync(r => a ! AwaitDone(r, awt, cont))
                right((c:EarlyCause) => rcv(left(c)).run +: cont)
              case Halt(cause)                 => S(cb(left(cause))); left(cause)
            }
          }


          a = new Actor[M]({ m =>
            m match {
              case AwaitDone(r, awt, cont) if completed.isEmpty =>
                val step = Try(awt.rcv(EarlyCause(r)).run) +: cont


                runStep(step).fold(
                  rsn => completed = Some(rsn)
                  , cln => cleanup = cln
                )

              // on interrupt we just run any cleanup code we have memo-ed
              // from last `Await`
              case Interrupt(cause) if completed.isEmpty =>
                completed = Some(cause)
                Try(cleanup(cause)).run.runAsync(_.fold(
                  rsn0 =>  cb(left(Error(rsn0).causedBy(cause)))
                  , _ => cb(left(cause))
                ))

              // this indicates last await was interrupted.
              // In case the request was successful and only then
              // we have to get next state of the process and assure
              // any cleanup will be run.
              // note this won't consult any cleanup contained
              // in `next` or `rcv` on left side
              // as this was already run on `Interrupt`
              case AwaitDone(r, awt, _) =>
                Try(awt.rcv(EarlyCause(r)).run)
                .kill
                .run.runAsync(_ => ())


              // Interrupt after we have been completed this is no-op
              case Interrupt(_) => ()

            }
          })(S)

          runStep(self).fold(
            rsn => (_: Cause) => ()
            , cln => {
              cleanup = cln
              (cause: EarlyCause) => a ! Interrupt(cause)
            }
          )
        }

  }

  /**
   * This class provides infix syntax specific to `Tee`. We put these here
   * rather than trying to cram them into `Process` itself using implicit
   * equality witnesses. This doesn't work out so well due to variance
   * issues.
   */
  implicit class TeeSyntax[I,I2,O](val self: Tee[I,I2,O]) extends AnyVal {

    /** Transform the left input to a `Tee`. */
    def contramapL[I0](f: I0 => I): Tee[I0,I2,O] =
      self.contramapL_(f).asInstanceOf[Tee[I0,I2,O]]

    /** Transform the right input to a `Tee`. */
    def contramapR[I3](f: I3 => I2): Tee[I,I3,O] =
      self.contramapR_(f).asInstanceOf[Tee[I,I3,O]]
  }


  /**
   * Infix syntax for working with `Writer[F,W,O]`. We call
   * the `W` parameter the 'write' side of the `Writer` and
   * `O` the 'output' side. Many method in this class end
   * with either `W` or `O`, depending on what side they
   * operate on.
   */
  implicit class WriterSyntax[F[_],W,O](val self: Writer[F,W,O]) extends AnyVal {

    /** Transform the write side of this `Writer`. */
    def flatMapW[F2[x]>:F[x],W2,O2>:O](f: W => Writer[F2,W2,O2]): Writer[F2,W2,O2] =
      self.flatMap(_.fold(f, emitO))

    /** Remove the write side of this `Writer`. */
    def stripW: Process[F,O] =
      self.flatMap(_.fold(_ => halt, emit))

    /** Map over the write side of this `Writer`. */
    def mapW[W2](f: W => W2): Writer[F,W2,O] =
      self.map(_.leftMap(f))

    /** pipe Write side of this `Writer`  */
    def pipeW[B](f: Process1[W,B]): Writer[F,B,O] =
      self.pipe(process1.liftL(f))

    /**
     * Observe the write side of this `Writer` using the
     * given `Sink`, keeping it available for subsequent
     * processing. Also see `drainW`.
     */
    def observeW(snk: Sink[F,W]): Writer[F,W,O] =
      self.zipWith(snk)((a,f) =>
        a.fold(
          (s: W) => eval_ { f(s) } ++ Process.emitW(s),
          (a: O) => Process.emitO(a)
        )
      ).flatMap(identity)

    /**
     * Observe the write side of this `Writer` using the
     * given `Sink`, then discard it. Also see `observeW`.
     */
    def drainW(snk: Sink[F,W]): Process[F,O] =
      observeW(snk).stripW


    /**
     * Observe the output side of this `Writer` using the
     * given `Sink`, keeping it available for subsequent
     * processing. Also see `drainO`.
     */
    def observeO(snk: Sink[F,O]): Writer[F,W,O] =
      self.map(_.swap).observeW(snk).map(_.swap)

    /**
     * Observe the output side of this Writer` using the
     * given `Sink`, then discard it. Also see `observeW`.
     */
    def drainO(snk: Sink[F,O]): Process[F,W] =
      observeO(snk).stripO

    /** Map over the output side of this `Writer`. */
    def mapO[B](f: O => B): Writer[F,W,B] =
      self.map(_.map(f))

    def flatMapO[F2[x]>:F[x],W2>:W,B](f: O => Writer[F2,W2,B]): Writer[F2,W2,B] =
      self.flatMap(_.fold(emitW, f))

    def stripO: Process[F,W] =
      self.flatMap(_.fold(emit, _ => halt))

    def pipeO[B](f: Process1[O,B]): Writer[F,W,B] =
      self.pipe(process1.liftR(f))
  }


  /**
   * This class provides infix syntax specific to `Wye`. We put these here
   * rather than trying to cram them into `Process` itself using implicit
   * equality witnesses. This doesn't work out so well due to variance
   * issues.
   */
  implicit class WyeSyntax[I,I2,O](val self: Wye[I,I2,O]) extends AnyVal {

    /**
     * Apply a `Wye` to two `Iterable` inputs.
     */
    def apply(input: Iterable[I], input2: Iterable[I2]): IndexedSeq[O] = {
      // this is probably rather slow
      val src1 = Process.emitAll(input.toSeq).toSource
      val src2 = Process.emitAll(input2.toSeq).toSource
      src1.wye(src2)(self).runLog.run
    }

    /**
     * Transform the left input of the given `Wye` using a `Process1`.
     */
    def attachL[I0](f: Process1[I0,I]): Wye[I0, I2, O] =
      neutral.stream.wye.attachL(f)(self)

    /**
     * Transform the right input of the given `Wye` using a `Process1`.
     */
    def attachR[I1](f: Process1[I1,I2]): Wye[I, I1, O] =
     neutral.stream.wye.attachR(f)(self)

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

    /**
     * Converting requests for the left input into normal termination.
     * Note that `Both` requests are rewritten to fetch from the only input.
     */
    def detach1L: Wye[I,I2,O] =   neutral.stream.wye.detach1L(self)

    /**
     * Converting requests for the left input into normal termination.
     * Note that `Both` requests are rewritten to fetch from the only input.
     */
    def detach1R: Wye[I,I2,O] = neutral.stream.wye.detach1R(self)
  }

  //////////////////////////////////////////////////////////////////////////////////////
  //
  // SYNTAX Functions
  //
  /////////////////////////////////////////////////////////////////////////////////////

  /**
   * Evaluate an arbitrary effect in a `Process`. The resulting
   * `Process` emits a single value. To evaluate repeatedly, use
   * `repeatEval(t)`.
   * Do not use `eval.repeat` or  `repeat(eval)` as that may cause infinite loop in certain situations.
   */
  def eval[F[_], O](f: F[O]): Process[F, O] =
    awaitOr(f)(_.asHalt)(emit)

  /**
   * Evaluate an arbitrary effect once, purely for its effects,
   * ignoring its return value. This `Process` emits no values.
   */
  def eval_[F[_], O](f: F[O]): Process[F, Nothing] =
    eval(f).drain

  /** Prefix syntax for `p.repeat`. */
  def repeat[F[_], O](p: Process[F, O]): Process[F, O] = p.repeat


  /**
   * Evaluate an arbitrary effect in a `Process`. The resulting `Process` will emit values
   * until evaluation of `f` signals termination with `End` or an error occurs.
   *
   * Note that if `f` results to failure of type `Terminated` the repeatEval will convert cause
   * to respective process cause termination, and will halt with that cause.
   *
   */
  def repeatEval[F[_], O](f: F[O]): Process[F, O] =
    awaitOr(f)(_.asHalt)(o => emit(o) ++ repeatEval(f))

  /**
   * Produce `p` lazily. Useful if producing the process involves allocation of
   * some local mutable resource we want to ensure is freshly allocated
   * for each consumer of `p`.
   *
   * Note that this implementation assures that:
   * {{{
   *    suspend(p).kill === suspend(p.kill)
   *    suspend(p).kill === p.kill
   *
   *    suspend(p).repeat === suspend(p.repeat)
   *    suspend(p).repeat ===  p.repeat
   *
   *    suspend(p).eval === suspend(p.eval)
   *    suspend(p).eval === p.eval
   *
   *    Halt(cause) ++ suspend(p) === Halt(cause) ++ p
   * }}}
   *
   */
  def suspend[F[_], O](p: => Process[F, O]): Process[F, O] =
    Append(halt0,Vector({
      case End => Trampoline.done(p)
      case early: EarlyCause => Trampoline.done(p.injectCause(early))
    }))
}

