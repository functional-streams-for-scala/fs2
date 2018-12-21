package fs2.internal

import cats.~>
import cats.effect.{Concurrent, ExitCase, Sync}
import cats.implicits._
import fs2._
import fs2.internal.FreeC.{Result, ViewL}

import scala.util.control.NonFatal

/* `Algebra[F[_], O, R]` is a Generalised Algebraic Data Type (GADT)
 * of atomic instructions that can be evaluated in the effect `F`
 * to generate by-product outputs of type `O`.
 *
 * Each operation also generates an output of type `R` that is used
 * as control information for the rest of the interpretation or compilation.
 */
private[fs2] sealed trait Algebra[F[_], +O, R]

private[fs2] object Algebra {

  private[this] final case class Output[F[_], O](values: Chunk[O]) extends Algebra[F, O, Unit]

  private[this] final case class Step[F[_], X, O](
      stream: FreeC[Algebra[F, X, ?], Unit],
      scope: Option[Token]
  ) extends Algebra[F, O, Option[(Chunk[X], Token, FreeC[Algebra[F, X, ?], Unit])]]

  /* The `AlgEffect` trait is for operations on the `F` effect that create no `O` output.
   * They are related to resources and scopes. */
  private[this] sealed trait AlgEffect[F[_], R] extends Algebra[F, INothing, R]

  private[this] final case class Eval[F[_], R](value: F[R]) extends AlgEffect[F, R]

  private[this] final case class Acquire[F[_], R](resource: F[R],
                                                  release: (R, ExitCase[Throwable]) => F[Unit])
      extends AlgEffect[F, (R, Token)]

  private[this] final case class Release[F[_]](token: Token) extends AlgEffect[F, Unit]

  private[this] final case class OpenScope[F[_]](interruptible: Option[Concurrent[F]])
      extends AlgEffect[F, Token]

  // `InterruptedScope` contains id of the scope currently being interrupted
  // together with any errors accumulated during interruption process
  private[this] final case class CloseScope[F[_]](
      scopeId: Token,
      interruptedScope: Option[(Token, Option[Throwable])],
      exitCase: ExitCase[Throwable])
      extends AlgEffect[F, Unit]

  private[this] final case class GetScope[F[_]]() extends AlgEffect[F, CompileScope[F]]

  def output[F[_], O](values: Chunk[O]): FreeC[Algebra[F, O, ?], Unit] =
    FreeC.Eval[Algebra[F, O, ?], Unit](Output(values))

  def output1[F[_], O](value: O): FreeC[Algebra[F, O, ?], Unit] =
    output(Chunk.singleton(value))

  def eval[F[_], O, R](value: F[R]): FreeC[Algebra[F, O, ?], R] =
    FreeC.Eval[Algebra[F, O, ?], R](Eval(value))

  def acquire[F[_], O, R](
      resource: F[R],
      release: (R, ExitCase[Throwable]) => F[Unit]): FreeC[Algebra[F, O, ?], (R, Token)] =
    FreeC.Eval[Algebra[F, O, ?], (R, Token)](Acquire(resource, release))

  def release[F[_], O](token: Token): FreeC[Algebra[F, O, ?], Unit] =
    FreeC.Eval[Algebra[F, O, ?], Unit](Release(token))

  def mapOutput[F[_], A, B](fun: A => B): Algebra[F, A, ?] ~> Algebra[F, B, ?] =
    new (Algebra[F, A, ?] ~> Algebra[F, B, ?]) {
      def apply[R](alg: Algebra[F, A, R]): Algebra[F, B, R] = alg match {
        case o: Output[F, A] => Output[F, B](o.values.map(fun))
        case _               => alg.asInstanceOf[Algebra[F, B, R]]
      }
    }

  /**
    * Steps through the stream, providing either `uncons` or `stepLeg`.
    * Yields to head in form of chunk, then id of the scope that was active after step evaluated and tail of the `stream`.
    *
    * @param stream             Stream to step
    * @param scopeId            If scope has to be changed before this step is evaluated, id of the scope must be supplied
    */
  private def step[F[_], O, X](
      stream: FreeC[Algebra[F, O, ?], Unit],
      scopeId: Option[Token]
  ): FreeC[Algebra[F, X, ?], Option[(Chunk[O], Token, FreeC[Algebra[F, O, ?], Unit])]] =
    FreeC
      .Eval[Algebra[F, X, ?], Option[(Chunk[O], Token, FreeC[Algebra[F, O, ?], Unit])]](
        Step[F, O, X](stream, scopeId))

  def stepLeg[F[_], O](
      leg: Stream.StepLeg[F, O]): FreeC[Algebra[F, Nothing, ?], Option[Stream.StepLeg[F, O]]] =
    step[F, O, Nothing](
      leg.next,
      Some(leg.scopeId)
    ).map {
      _.map { case (h, id, t) => new Stream.StepLeg[F, O](h, id, t) }
    }

  /**
    * Wraps supplied pull in new scope, that will be opened before this pull is evaluated
    * and closed once this pull either finishes its evaluation or when it fails.
    */
  def scope[F[_], O](s: FreeC[Algebra[F, O, ?], Unit]): FreeC[Algebra[F, O, ?], Unit] =
    scope0(s, None)

  /**
    * Like `scope` but allows this scope to be interrupted.
    * Note that this may fail with `Interrupted` when interruption occurred
    */
  private[fs2] def interruptScope[F[_], O](s: FreeC[Algebra[F, O, ?], Unit])(
      implicit F: Concurrent[F]): FreeC[Algebra[F, O, ?], Unit] =
    scope0(s, Some(F))

  private[this] def openScope[F[_], O](
      interruptible: Option[Concurrent[F]]): FreeC[Algebra[F, O, ?], Token] =
    FreeC.Eval[Algebra[F, O, ?], Token](OpenScope(interruptible))

  private[fs2] def closeScope[F[_], O](
      token: Token,
      interruptedScope: Option[(Token, Option[Throwable])],
      exitCase: ExitCase[Throwable]): FreeC[Algebra[F, O, ?], Unit] =
    FreeC.Eval[Algebra[F, O, ?], Unit](CloseScope(token, interruptedScope, exitCase))

  private def scope0[F[_], O](s: FreeC[Algebra[F, O, ?], Unit],
                              interruptible: Option[Concurrent[F]]): FreeC[Algebra[F, O, ?], Unit] =
    openScope(interruptible).flatMap { scopeId =>
      s.transformWith {
        case Result.Pure(_) => closeScope(scopeId, interruptedScope = None, ExitCase.Completed)
        case Result.Interrupted(interruptedScopeId: Token, err) =>
          closeScope(scopeId, interruptedScope = Some((interruptedScopeId, err)), ExitCase.Canceled)
        case Result.Fail(err) =>
          closeScope(scopeId, interruptedScope = None, ExitCase.Error(err)).transformWith {
            case Result.Pure(_)    => raiseError(err)
            case Result.Fail(err0) => raiseError(CompositeFailure(err, err0, Nil))
            case Result.Interrupted(interruptedScopeId, _) =>
              sys.error(
                s"Impossible, cannot interrupt when closing failed scope: $scopeId, $interruptedScopeId $err")
          }

        case Result.Interrupted(ctx, err) => sys.error(s"Impossible context: $ctx")
      }
    }

  def getScope[F[_], O]: FreeC[Algebra[F, O, ?], CompileScope[F]] =
    FreeC.eval[Algebra[F, O, ?], CompileScope[F]](GetScope())

  def pure[F[_], O, R](r: R): FreeC[Algebra[F, O, ?], R] =
    FreeC.pure[Algebra[F, O, ?], R](r)

  def raiseError[F[_], O, R](t: Throwable): FreeC[Algebra[F, O, ?], R] =
    FreeC.raiseError[Algebra[F, O, ?], R](t)

  def suspend[F[_], O, R](f: => FreeC[Algebra[F, O, ?], R]): FreeC[Algebra[F, O, ?], R] =
    FreeC.suspend(f)

  def translate[F[_], G[_], O](
      s: FreeC[Algebra[F, O, ?], Unit],
      u: F ~> G
  )(implicit G: TranslateInterrupt[G]): FreeC[Algebra[G, O, ?], Unit] =
    translate0[F, G, O](u, s, G.concurrentInstance)

  def uncons[F[_], X, O](s: FreeC[Algebra[F, O, ?], Unit])
    : FreeC[Algebra[F, X, ?], Option[(Chunk[O], FreeC[Algebra[F, O, ?], Unit])]] =
    step(s, None).map { _.map { case (h, _, t) => (h, t) } }

  /** Left-folds the output of a stream. */
  def compile[F[_], O, B](stream: FreeC[Algebra[F, O, ?], Unit], init: B)(f: (B, Chunk[O]) => B)(
      implicit F: Sync[F]): F[B] =
    F.bracketCase(F.delay(CompileScope.newRoot[F]))(scope =>
      compileScope[F, O, B](scope, stream, init)(f))((scope, ec) => scope.close(ec).rethrow)

  private[this] def compileScope[F[_], O, B](
      scope: CompileScope[F],
      stream: FreeC[Algebra[F, O, ?], Unit],
      init: B)(g: (B, Chunk[O]) => B)(implicit F: Sync[F]): F[B] =
    compileLoop[F, O](scope, stream).flatMap {
      case Some((output, scope, tail)) =>
        try {
          val b = g(init, output)
          compileScope(scope, tail, b)(g)
        } catch {
          case NonFatal(err) =>
            compileScope(scope, tail.asHandler(err), init)(g)
        }
      case None =>
        F.pure(init)
    }

  /*
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
   *
   */

  private[this] def compileLoop[F[_], O](
      compileScope: CompileScope[F],
      stream: FreeC[Algebra[F, O, ?], Unit]
  )(implicit F: Sync[F]): F[Option[(Chunk[O], CompileScope[F], FreeC[Algebra[F, O, ?], Unit])]] = {

    case class Done[X](scope: CompileScope[F]) extends R[X]
    case class Out[X](head: Chunk[X], scope: CompileScope[F], tail: FreeC[Algebra[F, X, ?], Unit])
        extends R[X]
    case class Interrupted[X](scopeId: Token, err: Option[Throwable]) extends R[X]

    sealed trait R[X]

    def go[X](
        goScope: CompileScope[F],
        stream: FreeC[Algebra[F, X, ?], Unit]
    ): F[R[X]] = {
      stream.viewL match {
        case _: FreeC.Result.Pure[Algebra[F, X, ?], Unit] =>
          F.pure(Done(goScope))

        case failed: FreeC.Result.Fail[Algebra[F, X, ?], Unit] =>
          F.raiseError(failed.error)

        case interrupted: FreeC.Result.Interrupted[Algebra[F, X, ?], _, Unit] =>
          interrupted.context match {
            case scopeId: Token => F.pure(Interrupted(scopeId, interrupted.deferredError))
            case other          => sys.error(s"Unexpected interruption context: $other (compileLoop)")
          }

        case view: ViewL.View[Algebra[F, X, ?], y, Unit] =>
          def interruptGuard(guardScope: CompileScope[F])(next: => F[R[X]]): F[R[X]] =
            F.flatMap(guardScope.isInterrupted) {
              case None => next
              case Some(Left(err)) =>
                go(guardScope, view.next(Result.raiseError(err)))
              case Some(Right(scopeId)) =>
                go(guardScope, view.next(Result.interrupted(scopeId, None)))
            }

          view.step match {
            case output: Output[F, X] =>
              interruptGuard(goScope)(
                F.pure(Out(output.values, goScope, view.next(FreeC.Result.unit)))
              )

            case u: Step[F, y, X] =>
              // if scope was specified in step, try to find it, otherwise use the current scope.
              F.flatMap(u.scope.fold[F[Option[CompileScope[F]]]](F.pure(Some(goScope))) { scopeId =>
                goScope.findStepScope(scopeId)
              }) {
                case Some(stepScope) =>
                  F.flatMap(F.attempt(go[y](stepScope, u.stream))) {
                    case Right(Done(doneScope)) =>
                      interruptGuard(doneScope)(
                        go(doneScope, view.next(Result.pure(None)))
                      )
                    case Right(Out(head, outScope, tail)) =>
                      // if we originally swapped scopes we want to return the original
                      // scope back to the go as that is the scope that is expected to be here.
                      val nextScope = if (u.scope.isEmpty) outScope else goScope
                      interruptGuard(nextScope)(
                        go(nextScope, view.next(Result.pure(Some((head, outScope.id, tail)))))
                      )

                    case Right(Interrupted(scopeId, err)) =>
                      go(goScope, view.next(Result.interrupted(scopeId, err)))

                    case Left(err) =>
                      go(goScope, view.next(Result.raiseError(err)))
                  }
                case None =>
                  F.raiseError(
                    new Throwable(
                      s"Fail to find scope for next step: current: ${goScope.id}, step: $u"))
              }

            case eval: Eval[F, r] =>
              F.flatMap(goScope.interruptibleEval(eval.value)) { eit =>
                val res: Result[r] = eit match {
                  case Right(r)           => Result.pure(r)
                  case Left(Left(err))    => Result.raiseError(err)
                  case Left(Right(token)) => Result.interrupted(token, None)

                }
                go[X](goScope, view.next(res))
              }

            case acquire: Acquire[F, r] =>
              interruptGuard(goScope) {
                F.flatMap(goScope.acquireResource(acquire.resource, acquire.release)) { r =>
                  go[X](goScope, view.next(Result.fromEither(r)))
                }
              }

            case release: Release[F] =>
              F.flatMap(goScope.releaseResource(release.token, ExitCase.Completed)) { r =>
                go[X](goScope, view.next(Result.fromEither(r)))
              }

            case _: GetScope[F] =>
              F.suspend(go(goScope, view.next(Result.pure(goScope.asInstanceOf[y]))))

            case open: OpenScope[F] =>
              interruptGuard(goScope) {
                F.flatMap(goScope.open(open.interruptible)) {
                  case Left(err) =>
                    go(goScope, view.next(Result.raiseError(err)))
                  case Right(childScope) =>
                    go(childScope, view.next(Result.pure(childScope.id)))
                }
              }

            case close: CloseScope[F] =>
              def closeAndGo(toClose: CompileScope[F], ec: ExitCase[Throwable]) =
                F.flatMap(toClose.close(ec)) { r =>
                  F.flatMap(toClose.openAncestor) { ancestor =>
                    close.interruptedScope match {
                      case None => go(ancestor, view.next(Result.fromEither(r)))
                      case Some((interruptedScopeId, err)) =>
                        def err1 = CompositeFailure.fromList(r.left.toOption.toList ++ err.toList)
                        if (ancestor.findSelfOrAncestor(interruptedScopeId).isDefined) {
                          // we still have scopes to interrupt, lets build interrupted tail
                          go(ancestor, view.next(Result.interrupted(interruptedScopeId, err1)))
                        } else {
                          // interrupts scope was already interrupted, resume operation
                          err1 match {
                            case None      => go(ancestor, view.next(Result.unit))
                            case Some(err) => go(ancestor, view.next(Result.raiseError(err)))
                          }

                        }
                    }

                  }
                }

              goScope.findSelfOrAncestor(close.scopeId) match {
                case Some(toClose) => closeAndGo(toClose, close.exitCase)
                case None =>
                  goScope.findSelfOrChild(close.scopeId).flatMap {
                    case Some(toClose) =>
                      closeAndGo(toClose, close.exitCase)
                    case None =>
                      // scope already closed, continue with current scope
                      def result =
                        close.interruptedScope
                          .map { Result.interrupted _ tupled }
                          .getOrElse(Result.unit)
                      go(goScope, view.next(result))
                  }
              }

          }

      }

    }

    F.flatMap(go(compileScope, stream)) {
      case Done(_)                   => F.pure(None)
      case Out(head, outScope, tail) => F.pure(Some((head, outScope, tail)))
      case Interrupted(_, err) =>
        err match {
          case None      => F.pure(None)
          case Some(err) => F.raiseError(err)
        }
    }
  }

  /**
    * Inject interruption to the tail used in flatMap.
    * Assures that close of the scope is invoked if at the flatMap tail, otherwise switches evaluation to `interrupted` path
    *
    * @param stream             tail to inject interruption into
    * @param interruptedScope   scopeId to interrupt
    * @param interruptedError   Additional finalizer errors
    * @tparam F
    * @tparam O
    * @return
    */
  def interruptBoundary[F[_], O](
      stream: FreeC[Algebra[F, O, ?], Unit],
      interruptedScope: Token,
      interruptedError: Option[Throwable]
  ): FreeC[Algebra[F, O, ?], Unit] =
    stream.viewL match {
      case _: FreeC.Result.Pure[Algebra[F, O, ?], Unit] =>
        FreeC.interrupted(interruptedScope, interruptedError)
      case failed: FreeC.Result.Fail[Algebra[F, O, ?], Unit] =>
        Algebra.raiseError(
          CompositeFailure
            .fromList(interruptedError.toList :+ failed.error)
            .getOrElse(failed.error))
      case interrupted: FreeC.Result.Interrupted[Algebra[F, O, ?], _, Unit] =>
        // impossible
        FreeC.interrupted(interrupted.context, interrupted.deferredError)

      case view: ViewL.View[Algebra[F, O, ?], _, Unit] =>
        view.step match {
          case close: CloseScope[F] =>
            Algebra
              .closeScope(close.scopeId, Some((interruptedScope, interruptedError)), close.exitCase) // assumes it is impossible so the `close` will be already from interrupted stream
              .transformWith(view.next)
          case _ =>
            // all other cases insert interruption cause
            view.next(Result.interrupted(interruptedScope, interruptedError))

        }

    }

  private def translateStep[F[_], G[_], X](
      fK: F ~> G,
      next: FreeC[Algebra[F, X, ?], Unit],
      concurrent: Option[Concurrent[G]]
  ): FreeC[Algebra[G, X, ?], Unit] =
    next.viewL match {
      case _: FreeC.Result.Pure[Algebra[F, X, ?], Unit] =>
        FreeC.pure[Algebra[G, X, ?], Unit](())

      case failed: FreeC.Result.Fail[Algebra[F, X, ?], Unit] =>
        Algebra.raiseError(failed.error)

      case interrupted: FreeC.Result.Interrupted[Algebra[F, X, ?], _, Unit] =>
        FreeC.interrupted(interrupted.context, interrupted.deferredError)

      case view: ViewL.View[Algebra[F, X, ?], y, Unit] =>
        view.step match {
          case output: Output[F, X] =>
            Algebra.output[G, X](output.values).transformWith {
              case r @ Result.Pure(v) =>
                // Cast is safe here, as at this point the evaluation of this Step will end
                // and the remainder of the free will be passed as a result in Bind. As such
                // next Step will have this to evaluate, and will try to translate again.
                view
                  .next(r)
                  .asInstanceOf[FreeC[Algebra[G, X, ?], Unit]]

              case r @ Result.Fail(err) => translateStep(fK, view.next(r), concurrent)

              case r @ Result.Interrupted(_, _) => translateStep(fK, view.next(r), concurrent)
            }

          case step: Step[F, x, X] =>
            FreeC
              .Eval[Algebra[G, X, ?], Option[(Chunk[x], Token, FreeC[Algebra[G, x, ?], Unit])]](
                Step[G, x, X](
                  stream = translateStep[F, G, x](fK, step.stream, concurrent),
                  scope = step.scope
                ))
              .transformWith { r =>
                translateStep[F, G, X](fK, view.next(r.asInstanceOf[Result[y]]), concurrent)
              }

          case alg: AlgEffect[F, r] =>
            FreeC
              .Eval[Algebra[G, X, ?], r](translateAlgEffect(alg, concurrent, fK))
              .transformWith(r => translateStep(fK, view.next(r), concurrent))

        }

    }

  private def translate0[F[_], G[_], O](
      fK: F ~> G,
      s: FreeC[Algebra[F, O, ?], Unit],
      concurrent: Option[Concurrent[G]]
  ): FreeC[Algebra[G, O, ?], Unit] =
    s.viewL match {
      case _: FreeC.Result.Pure[Algebra[F, O, ?], Unit] =>
        FreeC.pure[Algebra[G, O, ?], Unit](())

      case failed: FreeC.Result.Fail[Algebra[F, O, ?], Unit] =>
        Algebra.raiseError(failed.error)

      case interrupted: FreeC.Result.Interrupted[Algebra[F, O, ?], _, Unit] =>
        FreeC.interrupted(interrupted.context, interrupted.deferredError)

      case view: ViewL.View[Algebra[F, O, ?], y, Unit] =>
        view.step match {
          case output: Output[F, O] =>
            Algebra.output[G, O](output.values).transformWith { r =>
              translate0(fK, view.next(r), concurrent)
            }

          case step: Step[F, x, O] =>
            FreeC
              .Eval[Algebra[G, O, ?], Option[(Chunk[x], Token, FreeC[Algebra[G, x, ?], Unit])]](
                Step[G, x, O](
                  stream = translateStep[F, G, x](fK, step.stream, concurrent),
                  scope = step.scope
                ))
              .transformWith { r =>
                translate0(fK, view.next(r.asInstanceOf[Result[y]]), concurrent)
              }

          case alg: AlgEffect[F, r] =>
            FreeC
              .Eval[Algebra[G, O, ?], r](translateAlgEffect(alg, concurrent, fK))
              .transformWith(r => translate0(fK, view.next(r), concurrent))

        }

    }

  private[this] def translateAlgEffect[F[_], G[_], R](
      self: AlgEffect[F, R],
      concurrent: Option[Concurrent[G]],
      fK: F ~> G
  ): AlgEffect[G, R] = self match {
    // safe to cast, used in translate only
    // if interruption has to be supported concurrent for G has to be passed
    case a: Acquire[F, r] =>
      Acquire[G, r](fK(a.resource), (r, ec) => fK(a.release(r, ec)))
        .asInstanceOf[AlgEffect[G, R]]
    case e: Eval[F, R]    => Eval[G, R](fK(e.value))
    case o: OpenScope[F]  => OpenScope[G](concurrent).asInstanceOf[AlgEffect[G, R]]
    case r: Release[F]    => r.asInstanceOf[AlgEffect[G, R]]
    case c: CloseScope[F] => c.asInstanceOf[AlgEffect[G, R]]
    case g: GetScope[F]   => g.asInstanceOf[AlgEffect[G, R]]
  }

}
