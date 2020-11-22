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

package fs2.internal

import cats.{MonadError, ~>}
import cats.effect.{Concurrent, ExitCase, Fiber}
import cats.syntax.all._
import fs2.{Chunk, CompositeFailure, INothing, Pure => PureK, Stream}
import fs2.internal.FreeC.{Result, ViewL}

import scala.annotation.tailrec
import scala.util.control.NonFatal
import FreeC._

/** Free Monad with Catch (and Interruption).
  *
  * [[FreeC]] provides mechanism for ensuring stack safety and capturing any exceptions that may arise during computation.
  *
  * Furthermore, it may capture Interruption of the evaluation, although [[FreeC]] itself does not have any
  * interruptible behaviour per se.
  *
  * Interruption cause may be captured in [[FreeC.Result.Interrupted]] and allows user to pass along any information relevant
  * to interpreter.
  *
  * Typically the [[FreeC]] user provides interpretation of FreeC in form of [[ViewL]] structure, that allows to step
  * FreeC via series of Results ([[Result.Pure]], [[Result.Fail]] and [[Result.Interrupted]]) and FreeC step ([[ViewL.View]])
  */
private[fs2] sealed abstract class FreeC[+F[_], +O, +R] {
  def flatMap[F2[x] >: F[x], O2 >: O, R2](f: R => FreeC[F2, O2, R2]): FreeC[F2, O2, R2] =
    new Bind[F2, O2, R, R2](this) {
      def cont(e: Result[R]): FreeC[F2, O2, R2] =
        e match {
          case Result.Pure(r) =>
            try f(r)
            catch { case NonFatal(e) => FreeC.Result.Fail(e) }
          case res @ Result.Interrupted(_, _) => res
          case res @ Result.Fail(_)           => res
        }
    }

  def append[F2[x] >: F[x], O2 >: O, R2](post: => FreeC[F2, O2, R2]): FreeC[F2, O2, R2] =
    new Bind[F2, O2, R, R2](this) {
      def cont(r: Result[R]): FreeC[F2, O2, R2] =
        r match {
          case _: Result.Pure[_]     => post
          case r: Result.Interrupted => r
          case r: Result.Fail        => r
        }
    }

  private[FreeC] def transformWith[F2[x] >: F[x], O2 >: O, R2](
      f: Result[R] => FreeC[F2, O2, R2]
  ): FreeC[F2, O2, R2] =
    new Bind[F2, O2, R, R2](this) {
      def cont(r: Result[R]): FreeC[F2, O2, R2] =
        try f(r)
        catch { case NonFatal(e) => FreeC.Result.Fail(e) }
    }

  def map[O2 >: O, R2](f: R => R2): FreeC[F, O2, R2] =
    new Bind[F, O2, R, R2](this) {
      def cont(e: Result[R]): FreeC[F, O2, R2] = Result.map(e)(f)
    }

  def handleErrorWith[F2[x] >: F[x], O2 >: O, R2 >: R](
      h: Throwable => FreeC[F2, O2, R2]
  ): FreeC[F2, O2, R2] =
    new Bind[F2, O2, R2, R2](this) {
      def cont(e: Result[R2]): FreeC[F2, O2, R2] =
        e match {
          case Result.Fail(e) =>
            try h(e)
            catch { case NonFatal(e) => FreeC.Result.Fail(e) }
          case other => other
        }
    }

  def asHandler(e: Throwable): FreeC[F, O, R] =
    ViewL(this) match {
      case Result.Pure(_)  => Result.Fail(e)
      case Result.Fail(e2) => Result.Fail(CompositeFailure(e2, e))
      case Result.Interrupted(ctx, err) =>
        Result.Interrupted(ctx, err.map(t => CompositeFailure(e, t)).orElse(Some(e)))
      case v @ ViewL.View(_) => v.next(Result.Fail(e))
    }

  def viewL[F2[x] >: F[x], O2 >: O, R2 >: R]: ViewL[F2, O2, R2] = ViewL(this)

  def mapOutput[P](f: O => P): FreeC[F, P, R]
}

private[fs2] object FreeC {

  /* A FreeC can be one of the following:
   *  - A Result or terminal, the end result of a pulling. This may have ended in:
   *    - Succeeded with a result of type R.
   *    - Failed with an exception
   *    - Interrupted from another thread with a known `scopeId`
   *
   *  - A Bind, that binds a first computation(another FreeC) with a method to _continue_
   *    the computation from the result of the first one `step`.
   *
   *  - A single Action, which can be one of following:
   *
   *    - Eval (or lift) an effectful operation of type `F[R]`
   *    - Output some values of type O.
   *    - Acquire a new resource and add its cleanup to the current scope.
   *    - Open, Close, or Access to the resource scope.
   *    - side-Step or fork to a different computation
   */

  /** A Result, or terminal, indicates how a pull or Free evaluation ended.
    * A FreeC may have succeeded with a result, failed with an exception,
    * or interrupted from another concurrent pull.
    */
  sealed abstract class Result[+R]
      extends FreeC[PureK, INothing, R]
      with ViewL[PureK, INothing, R] {
    override def mapOutput[P](f: INothing => P): FreeC[PureK, INothing, R] = this
  }

  object Result {
    val unit: Result[Unit] = Result.Pure(())

    def fromEither[R](either: Either[Throwable, R]): Result[R] =
      either.fold(Result.Fail(_), Result.Pure(_))

    final case class Pure[+R](r: R) extends Result[R] {
      override def toString: String = s"FreeC.Pure($r)"
    }

    final case class Fail(error: Throwable) extends Result[INothing] {
      override def toString: String = s"FreeC.Fail($error)"
    }

    /** Signals that FreeC evaluation was interrupted.
      *
      * @param context Any user specific context that needs to be captured during interruption
      *                for eventual resume of the operation.
      *
      * @param deferredError Any errors, accumulated during resume of the interruption.
      *                      Instead throwing errors immediately during interruption,
      *                      signalling of the errors may be deferred until the Interruption resumes.
      */
    final case class Interrupted(context: Token, deferredError: Option[Throwable])
        extends Result[INothing] {
      override def toString: String =
        s"FreeC.Interrupted($context, ${deferredError.map(_.getMessage)})"
    }

    private[FreeC] def map[A, B](fa: Result[A])(f: A => B): Result[B] =
      fa match {
        case Result.Pure(r) =>
          try Result.Pure(f(r))
          catch { case NonFatal(err) => Result.Fail(err) }
        case failure @ Result.Fail(_)               => failure
        case interrupted @ Result.Interrupted(_, _) => interrupted
      }
  }

  abstract class Bind[+F[_], +O, X, +R](val step: FreeC[F, O, X]) extends FreeC[F, O, R] {
    def cont(r: Result[X]): FreeC[F, O, R]
    def delegate: Bind[F, O, X, R] = this

    override def mapOutput[P](f: O => P): FreeC[F, P, R] =
      suspend {
        viewL match {
          case v: ViewL.View[F, O, x, R] =>
            new Bind[F, P, x, R](v.step.mapOutput(f)) {
              def cont(e: Result[x]) = v.next(e).mapOutput(f)
            }
          case r: Result[R] => r
        }
      }

    override def toString: String = s"FreeC.Bind($step)"
  }

  def suspend[F[_], O, R](fr: => FreeC[F, O, R]): FreeC[F, O, R] =
    new Bind[F, O, Unit, R](Result.unit) {
      def cont(r: Result[Unit]): FreeC[F, O, R] = fr
    }

  /** Unrolled view of a `FreeC` structure. may be `Result` or `EvalBind`
    */
  sealed trait ViewL[+F[_], +O, +R]

  object ViewL {

    /** unrolled view of FreeC `bind` structure * */
    sealed abstract case class View[+F[_], +O, X, +R](step: Action[F, O, X])
        extends ViewL[F, O, R] {
      def next(r: Result[X]): FreeC[F, O, R]
    }

    private[ViewL] final class EvalView[+F[_], +O, R](step: Action[F, O, R])
        extends View[F, O, R, R](step) {
      def next(r: Result[R]): FreeC[F, O, R] = r
    }

    private[fs2] def apply[F[_], O, R](free: FreeC[F, O, R]): ViewL[F, O, R] = mk(free)

    @tailrec
    private def mk[F[_], O, Z](free: FreeC[F, O, Z]): ViewL[F, O, Z] =
      free match {
        case r: Result[Z]       => r
        case e: Action[F, O, Z] => new EvalView[F, O, Z](e)
        case b: FreeC.Bind[F, O, y, Z] =>
          b.step match {
            case r: Result[_] =>
              val ry: Result[y] = r.asInstanceOf[Result[y]]
              mk(b.cont(ry))
            case e: Action[F, O, y2] =>
              new ViewL.View[F, O, y2, Z](e) {
                def next(r: Result[y2]): FreeC[F, O, Z] = b.cont(r.asInstanceOf[Result[y]])
              }
            case bb: FreeC.Bind[F, O, x, _] =>
              val nb = new Bind[F, O, x, Z](bb.step) {
                private[this] val bdel: Bind[F, O, y, Z] = b.delegate
                def cont(zr: Result[x]): FreeC[F, O, Z] =
                  new Bind[F, O, y, Z](bb.cont(zr).asInstanceOf[FreeC[F, O, y]]) {
                    override val delegate: Bind[F, O, y, Z] = bdel
                    def cont(yr: Result[y]): FreeC[F, O, Z] = delegate.cont(yr)
                  }
              }
              mk(nb)
          }
      }
  }

  def bracketCase[F[_], O, A, B](
      acquire: FreeC[F, O, A],
      use: A => FreeC[F, O, B],
      release: (A, ExitCase[Throwable]) => FreeC[F, O, Unit]
  ): FreeC[F, O, B] =
    acquire.flatMap { a =>
      val used =
        try use(a)
        catch { case NonFatal(t) => FreeC.Result.Fail(t) }
      used.transformWith { result =>
        val exitCase: ExitCase[Throwable] = result match {
          case Result.Pure(_)           => ExitCase.Completed
          case Result.Fail(err)         => ExitCase.Error(err)
          case Result.Interrupted(_, _) => ExitCase.Canceled
        }

        release(a, exitCase).transformWith {
          case Result.Fail(t2) =>
            result match {
              case Result.Fail(tres) => Result.Fail(CompositeFailure(tres, t2))
              case result            => result
            }
          case _ => result
        }
      }
    }

  /* An Action is an atomic instruction that can perform effects in `F`
   * to generate by-product outputs of type `O`.
   *
   * Each operation also generates an output of type `R` that is used
   * as control information for the rest of the interpretation or compilation.
   */
  abstract class Action[+F[_], +O, +R] extends FreeC[F, O, R]

  final case class Output[+O](values: Chunk[O]) extends Action[PureK, O, Unit] {
    override def mapOutput[P](f: O => P): FreeC[PureK, P, Unit] =
      FreeC.suspend {
        try Output(values.map(f))
        catch { case NonFatal(t) => Result.Fail(t) }
      }
  }

  /** Steps through the stream, providing either `uncons` or `stepLeg`.
    * Yields to head in form of chunk, then id of the scope that was active after step evaluated and tail of the `stream`.
    *
    * @param stream             Stream to step
    * @param scopeId            If scope has to be changed before this step is evaluated, id of the scope must be supplied
    */
  final case class Step[+F[_], X](stream: FreeC[F, X, Unit], scope: Option[Token])
      extends Action[PureK, INothing, Option[(Chunk[X], Token, FreeC[F, X, Unit])]] {
    /* NOTE: The use of `Any` and `PureK` done to by-pass an error in Scala 2.12 type-checker,
     * that produces a crash when dealing with Higher-Kinded GADTs in which the F parameter appears
     * Inside one of the values of the case class.      */
    override def mapOutput[P](f: INothing => P): Step[F, X] = this
  }

  /* The `AlgEffect` trait is for operations on the `F` effect that create no `O` output.
   * They are related to resources and scopes. */
  sealed abstract class AlgEffect[+F[_], R] extends Action[F, INothing, R] {
    final def mapOutput[P](f: INothing => P): FreeC[F, P, R] = this
  }

  final case class Eval[+F[_], R](value: F[R]) extends AlgEffect[F, R]

  final case class Acquire[+F[_], R](
      resource: F[R],
      release: (R, ExitCase[Throwable]) => F[Unit]
  ) extends AlgEffect[F, R]
  // NOTE: The use of a separate `G` and `PureK` is done o by-pass a compiler-crash in Scala 2.12,
  // involving GADTs with a covariant Higher-Kinded parameter. */
  final case class OpenScope[G[_]](interruptible: Option[Concurrent[G]])
      extends AlgEffect[PureK, Token]

  // `InterruptedScope` contains id of the scope currently being interrupted
  // together with any errors accumulated during interruption process
  final case class CloseScope(
      scopeId: Token,
      interruption: Option[Result.Interrupted],
      exitCase: ExitCase[Throwable]
  ) extends AlgEffect[PureK, Unit]

  final case class GetScope[F[_]]() extends AlgEffect[PureK, CompileScope[F]]

  final case class InterruptWhen[+F[_]](haltOnSignal: F[Either[Throwable, Unit]])
      extends AlgEffect[F, Unit]

  private[fs2] def interruptWhen[F[_], O](
      haltOnSignal: F[Either[Throwable, Unit]]
  ): FreeC[F, O, Unit] = InterruptWhen(haltOnSignal)

  def output1[O](value: O): FreeC[PureK, O, Unit] = Output(Chunk.singleton(value))

  def stepLeg[F[_], O](leg: Stream.StepLeg[F, O]): FreeC[F, Nothing, Option[Stream.StepLeg[F, O]]] =
    Step[F, O](leg.next, Some(leg.scopeId)).map {
      _.map { case (h, id, t) =>
        new Stream.StepLeg[F, O](h, id, t.asInstanceOf[FreeC[F, O, Unit]])
      }
    }

  /** Wraps supplied pull in new scope, that will be opened before this pull is evaluated
    * and closed once this pull either finishes its evaluation or when it fails.
    */
  def scope[F[_], O](s: FreeC[F, O, Unit]): FreeC[F, O, Unit] =
    scope0(s, None)

  /** Like `scope` but allows this scope to be interrupted.
    * Note that this may fail with `Interrupted` when interruption occurred
    */
  private[fs2] def interruptScope[F[_], O](
      s: FreeC[F, O, Unit]
  )(implicit F: Concurrent[F]): FreeC[F, O, Unit] =
    scope0(s, Some(F))

  private def scope0[F[_], O](
      s: FreeC[F, O, Unit],
      interruptible: Option[Concurrent[F]]
  ): FreeC[F, O, Unit] =
    OpenScope(interruptible).flatMap { scopeId =>
      s.transformWith {
        case Result.Pure(_) => CloseScope(scopeId, None, ExitCase.Completed)
        case interrupted @ Result.Interrupted(_, _) =>
          CloseScope(scopeId, Some(interrupted), ExitCase.Canceled)
        case Result.Fail(err) =>
          CloseScope(scopeId, None, ExitCase.Error(err)).transformWith {
            case Result.Pure(_)    => Result.Fail(err)
            case Result.Fail(err0) => Result.Fail(CompositeFailure(err, err0, Nil))
            case Result.Interrupted(interruptedScopeId, _) =>
              sys.error(
                s"Impossible, cannot interrupt when closing failed scope: $scopeId, $interruptedScopeId, $err"
              )
          }
      }
    }

  def uncons[F[_], X, O](s: FreeC[F, O, Unit]): FreeC[F, X, Option[(Chunk[O], FreeC[F, O, Unit])]] =
    Step(s, None).map(_.map { case (h, _, t) => (h, t.asInstanceOf[FreeC[F, O, Unit]]) })

  /* Left-folds the output of a stream.
   *
   * Interruption of the stream is tightly coupled between FreeC, Algebra and CompileScope
   * Reason for this is unlike interruption of `F` type (i.e. IO) we need to find
   * recovery point where stream evaluation has to continue in Stream algebra
   *
   * As such the `Token` is passed to FreeC.Interrupted as glue between FreeC/Algebra that allows pass-along
   * information for Algebra and scope to correctly compute recovery point after interruption was signalled via `CompilerScope`.
   *
   * This token indicates scope of the computation where interruption actually happened.
   * This is used to precisely find most relevant interruption scope where interruption shall be resumed
   * for normal continuation of the stream evaluation.
   *
   * Interpreter uses this to find any parents of this scope that has to be interrupted, and guards the
   * interruption so it won't propagate to scope that shall not be anymore interrupted.
   */
  def compile[F[_], O, B](
      stream: FreeC[F, O, Unit],
      initScope: CompileScope[F],
      extendLastTopLevelScope: Boolean,
      init: B
  )(g: (B, Chunk[O]) => B)(implicit F: MonadError[F, Throwable]): F[B] = {

    case class Done(scope: CompileScope[F]) extends R[INothing]
    case class Out[+X](head: Chunk[X], scope: CompileScope[F], tail: FreeC[F, X, Unit]) extends R[X]
    case class Interrupted(scopeId: Token, err: Option[Throwable]) extends R[INothing]
    sealed trait R[+X]

    def go[X](
        scope: CompileScope[F],
        extendedTopLevelScope: Option[CompileScope[F]],
        stream: FreeC[F, X, Unit]
    ): F[R[X]] =
      stream.viewL match {
        case _: FreeC.Result.Pure[Unit] =>
          F.pure(Done(scope))

        case failed: FreeC.Result.Fail =>
          F.raiseError(failed.error)

        case interrupted: FreeC.Result.Interrupted =>
          F.pure(Interrupted(interrupted.context, interrupted.deferredError))

        case view: ViewL.View[F, X, y, Unit] =>
          def resume(res: Result[y]): F[R[X]] =
            go[X](scope, extendedTopLevelScope, view.next(res))

          def interruptGuard(scope: CompileScope[F])(next: => F[R[X]]): F[R[X]] =
            F.flatMap(scope.isInterrupted) {
              case None => next
              case Some(Left(err)) =>
                go(scope, extendedTopLevelScope, view.next(Result.Fail(err)))
              case Some(Right(scopeId)) =>
                go(scope, extendedTopLevelScope, view.next(Result.Interrupted(scopeId, None)))
            }
          view.step match {
            case output: Output[X] =>
              interruptGuard(scope)(
                F.pure(Out(output.values, scope, view.next(FreeC.Result.unit)))
              )

            case uU: Step[f, y] =>
              val u: Step[F, y] = uU.asInstanceOf[Step[F, y]]
              // if scope was specified in step, try to find it, otherwise use the current scope.
              F.flatMap(u.scope.fold[F[Option[CompileScope[F]]]](F.pure(Some(scope))) { scopeId =>
                scope.findStepScope(scopeId)
              }) {
                case Some(stepScope) =>
                  val stepStream = u.stream.asInstanceOf[FreeC[F, y, Unit]]
                  F.flatMap(F.attempt(go[y](stepScope, extendedTopLevelScope, stepStream))) {
                    case Right(Done(scope)) =>
                      interruptGuard(scope)(
                        go(scope, extendedTopLevelScope, view.next(Result.Pure(None)))
                      )
                    case Right(Out(head, outScope, tail)) =>
                      // if we originally swapped scopes we want to return the original
                      // scope back to the go as that is the scope that is expected to be here.
                      val nextScope = if (u.scope.isEmpty) outScope else scope
                      val result = Result.Pure(
                        Some((head, outScope.id, tail.asInstanceOf[FreeC[f, y, Unit]]))
                      )
                      val next = view.next(result).asInstanceOf[FreeC[F, X, Unit]]
                      interruptGuard(nextScope)(
                        go(nextScope, extendedTopLevelScope, next)
                      )

                    case Right(Interrupted(scopeId, err)) =>
                      go(scope, extendedTopLevelScope, view.next(Result.Interrupted(scopeId, err)))

                    case Left(err) =>
                      go(scope, extendedTopLevelScope, view.next(Result.Fail(err)))
                  }

                case None =>
                  F.raiseError(
                    new RuntimeException(
                      s"""|Scope lookup failure!
                          |
                          |This is typically caused by uncons-ing from two or more streams in the same Pull.
                          |To do this safely, use `s.pull.stepLeg` instead of `s.pull.uncons` or a variant
                          |thereof. See the implementation of `Stream#zipWith_` for an example.
                          |
                          |Scope id: ${scope.id}
                          |Step: ${u}""".stripMargin
                    )
                  )
              }

            case eval: Eval[F, r] =>
              F.flatMap(scope.interruptibleEval(eval.value)) {
                case Right(r)           => resume(Result.Pure(r))
                case Left(Left(err))    => resume(Result.Fail(err))
                case Left(Right(token)) => resume(Result.Interrupted(token, None))
              }

            case acquire: Acquire[F, r] =>
              interruptGuard(scope) {
                F.flatMap(scope.acquireResource(acquire.resource, acquire.release)) { r =>
                  resume(Result.fromEither(r))
                }
              }

            case _: GetScope[_] =>
              resume(Result.Pure(scope.asInstanceOf[y]))

            case OpenScope(interruptibleX) =>
              val interruptible = interruptibleX.asInstanceOf[Option[Concurrent[F]]]
              interruptGuard(scope) {
                val maybeCloseExtendedScope: F[Boolean] =
                  // If we're opening a new top-level scope (aka, direct descendant of root),
                  // close the current extended top-level scope if it is defined.
                  if (scope.parent.isEmpty)
                    extendedTopLevelScope match {
                      case None    => false.pure[F]
                      case Some(s) => s.close(ExitCase.Completed).rethrow.as(true)
                    }
                  else F.pure(false)
                maybeCloseExtendedScope.flatMap { closedExtendedScope =>
                  val newExtendedScope = if (closedExtendedScope) None else extendedTopLevelScope
                  F.flatMap(scope.open(interruptible)) {
                    case Left(err) =>
                      go(scope, newExtendedScope, view.next(Result.Fail(err)))
                    case Right(childScope) =>
                      go(childScope, newExtendedScope, view.next(Result.Pure(childScope.id)))
                  }
                }
              }

            case close: CloseScope =>
              def closeAndGo(toClose: CompileScope[F], ec: ExitCase[Throwable]) =
                F.flatMap(toClose.close(ec)) { r =>
                  F.flatMap(toClose.openAncestor) { ancestor =>
                    val res = close.interruption match {
                      case None => Result.fromEither(r)
                      case Some(Result.Interrupted(interruptedScopeId, err)) =>
                        def err1 = CompositeFailure.fromList(r.swap.toOption.toList ++ err.toList)
                        if (ancestor.findSelfOrAncestor(interruptedScopeId).isDefined)
                          // we still have scopes to interrupt, lets build interrupted tail
                          Result.Interrupted(interruptedScopeId, err1)
                        else
                          // interrupts scope was already interrupted, resume operation
                          err1 match {
                            case None      => Result.unit
                            case Some(err) => Result.Fail(err)
                          }
                    }
                    go(ancestor, extendedTopLevelScope, view.next(res))
                  }
                }

              val scopeToClose: F[Option[CompileScope[F]]] = scope
                .findSelfOrAncestor(close.scopeId)
                .pure[F]
                .orElse(scope.findSelfOrChild(close.scopeId))
              F.flatMap(scopeToClose) {
                case Some(toClose) =>
                  if (toClose.parent.isEmpty)
                    // Impossible - don't close root scope as a result of a `CloseScope` call
                    go(scope, extendedTopLevelScope, view.next(Result.unit))
                  else if (extendLastTopLevelScope && toClose.parent.flatMap(_.parent).isEmpty)
                    // Request to close the current top-level scope - if we're supposed to extend
                    // it instead, leave the scope open and pass it to the continuation
                    extendedTopLevelScope.traverse_(_.close(ExitCase.Completed).rethrow) *>
                      F.flatMap(toClose.openAncestor)(ancestor =>
                        go(ancestor, Some(toClose), view.next(Result.unit))
                      )
                  else closeAndGo(toClose, close.exitCase)
                case None =>
                  // scope already closed, continue with current scope
                  val result = close.interruption.getOrElse(Result.unit)
                  go(scope, extendedTopLevelScope, view.next(result))
              }

            case int: InterruptWhen[F] =>
              interruptGuard(scope) {
                val acq = scope.acquireResource[Fiber[F, Unit]](
                  scope.interruptWhen(int.haltOnSignal),
                  (f, _) => f.cancel
                )
                F.flatMap(acq) { _ =>
                  resume(Result.unit)
                }
              }
          }
      }

    def outerLoop(scope: CompileScope[F], accB: B, stream: FreeC[F, O, Unit]): F[B] =
      F.flatMap(go(scope, None, stream)) {
        case Done(_) => F.pure(accB)
        case Out(head, scope, tail) =>
          try outerLoop(scope, g(accB, head), tail)
          catch {
            case NonFatal(err) => outerLoop(scope, accB, tail.asHandler(err))
          }
        case Interrupted(_, None)      => F.pure(accB)
        case Interrupted(_, Some(err)) => F.raiseError(err)
      }

    outerLoop(initScope, init, stream)
  }

  def flatMapOutput[F[_], F2[x] >: F[x], O, O2](
      freeC: FreeC[F, O, Unit],
      f: O => FreeC[F2, O2, Unit]
  ): FreeC[F2, O2, Unit] =
    Step(freeC, None).flatMap {
      case None => Result.unit

      case Some((chunk, _, FreeC.Result.Pure(_))) if chunk.size == 1 =>
        // nb: If tl is Pure, there's no need to propagate flatMap through the tail. Hence, we
        // check if hd has only a single element, and if so, process it directly instead of folding.
        // This allows recursive infinite streams of the form `def s: Stream[Pure,O] = Stream(o).flatMap { _ => s }`
        f(chunk(0))

      case Some((chunk, _, tail)) =>
        def go(idx: Int): FreeC[F2, O2, Unit] =
          if (idx == chunk.size)
            flatMapOutput[F, F2, O, O2](tail, f)
          else
            f(chunk(idx)).transformWith {
              case Result.Pure(_)   => go(idx + 1)
              case Result.Fail(err) => Result.Fail(err)
              case interruption @ Result.Interrupted(_, _) =>
                flatMapOutput[F, F2, O, O2](interruptBoundary(tail, interruption), f)
            }

        go(0)
    }

  /** Inject interruption to the tail used in flatMap.
    * Assures that close of the scope is invoked if at the flatMap tail, otherwise switches evaluation to `interrupted` path
    *
    * @param stream             tail to inject interruption into
    * @param interruptedScope   scopeId to interrupt
    * @param interruptedError   Additional finalizer errors
    * @tparam F
    * @tparam O
    * @return
    */
  private[this] def interruptBoundary[F[_], O](
      stream: FreeC[F, O, Unit],
      interruption: Result.Interrupted
  ): FreeC[F, O, Unit] =
    stream.viewL match {
      case _: FreeC.Result.Pure[Unit] =>
        interruption
      case failed: FreeC.Result.Fail =>
        Result.Fail(
          CompositeFailure
            .fromList(interruption.deferredError.toList :+ failed.error)
            .getOrElse(failed.error)
        )
      case interrupted: Result.Interrupted => interrupted // impossible

      case view: ViewL.View[F, O, _, Unit] =>
        view.step match {
          case CloseScope(scopeId, _, _) =>
            // Inner scope is getting closed b/c a parent was interrupted
            CloseScope(scopeId, Some(interruption), ExitCase.Canceled).transformWith(view.next)
          case _ =>
            // all other cases insert interruption cause
            view.next(interruption)
        }
    }

  def translate[F[_], G[_], O](
      stream: FreeC[F, O, Unit],
      fK: F ~> G
  )(implicit G: TranslateInterrupt[G]): FreeC[G, O, Unit] = {
    val concurrent: Option[Concurrent[G]] = G.concurrentInstance
    def translateAlgEffect[R](self: AlgEffect[F, R]): AlgEffect[G, R] =
      self match {
        // safe to cast, used in translate only
        // if interruption has to be supported concurrent for G has to be passed
        case a: Acquire[F, r] =>
          Acquire[G, r](fK(a.resource), (r, ec) => fK(a.release(r, ec)))
        case e: Eval[F, R]       => Eval[G, R](fK(e.value))
        case OpenScope(_)        => OpenScope[G](concurrent)
        case c: CloseScope       => c
        case g: GetScope[_]      => g
        case i: InterruptWhen[_] => InterruptWhen[G](fK(i.haltOnSignal))
      }

    def translateStep[X](next: FreeC[F, X, Unit], isMainLevel: Boolean): FreeC[G, X, Unit] =
      next.viewL match {
        case result: Result[Unit] => result

        case view: ViewL.View[F, X, y, Unit] =>
          view.step match {
            case output: Output[X] =>
              output.transformWith {
                case r @ Result.Pure(_) if isMainLevel =>
                  translateStep(view.next(r), isMainLevel)

                case r @ Result.Pure(_) =>
                  // Cast is safe here, as at this point the evaluation of this Step will end
                  // and the remainder of the free will be passed as a result in Bind. As such
                  // next Step will have this to evaluate, and will try to translate again.
                  view.next(r).asInstanceOf[FreeC[G, X, Unit]]

                case r @ Result.Fail(_) => translateStep(view.next(r), isMainLevel)

                case r @ Result.Interrupted(_, _) => translateStep(view.next(r), isMainLevel)
              }

            case stepU: Step[f, x] =>
              val step: Step[F, x] = stepU.asInstanceOf[Step[F, x]]
              Step[G, x](
                stream = translateStep[x](step.stream, false),
                scope = step.scope
              ).transformWith { r =>
                translateStep[X](view.next(r.asInstanceOf[Result[y]]), isMainLevel)
              }

            case alg: AlgEffect[F, r] =>
              translateAlgEffect(alg)
                .transformWith(r =>
                  translateStep(view.next(r.asInstanceOf[Result[y]]), isMainLevel)
                )
          }
      }

    translateStep[O](stream, true)
  }

}
