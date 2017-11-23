package fs2.internal

import cats.data.NonEmptyList

import scala.concurrent.ExecutionContext
import cats.~>
import cats.effect.{Effect, Sync}
import cats.implicits._
import fs2.{AsyncPull, Catenable, CompositeFailure, Segment, async}

private[fs2] sealed trait Algebra[F[_],O,R]

private[fs2] object Algebra {

  final case class Output[F[_],O](values: Segment[O,Unit]) extends Algebra[F,O,Unit]
  final case class Run[F[_],O,R](values: Segment[O,R]) extends Algebra[F,O,R]
  final case class Eval[F[_],O,R](value: F[R]) extends Algebra[F,O,R]

  final case class Acquire[F[_],O,R](resource: F[R], release: R => F[Unit]) extends Algebra[F,O,(R,Token)]
  final case class Release[F[_],O](token: Token) extends Algebra[F,O,Unit]
  final case class OpenScope[F[_],O]() extends Algebra[F,O,RunFoldScope[F]]
  final case class CloseScope[F[_],O](toClose: RunFoldScope[F]) extends Algebra[F,O,Unit]
  final case class GetScope[F[_],O]() extends Algebra[F,O,RunFoldScope[F]]

  final case class UnconsAsync[F[_],X,Y,O](s: FreeC[Algebra[F,O,?],Unit], ec: ExecutionContext, eff: Effect[F])
    extends Algebra[F,X,AsyncPull[F,Option[(Segment[O,Unit],FreeC[Algebra[F,O,?],Unit])]]]

  def output[F[_],O](values: Segment[O,Unit]): FreeC[Algebra[F,O,?],Unit] =
    FreeC.Eval[Algebra[F,O,?],Unit](Output(values))

  def output1[F[_],O](value: O): FreeC[Algebra[F,O,?],Unit] =
    output(Segment.singleton(value))

  def segment[F[_],O,R](values: Segment[O,R]): FreeC[Algebra[F,O,?],R] =
    FreeC.Eval[Algebra[F,O,?],R](Run(values))

  def eval[F[_],O,R](value: F[R]): FreeC[Algebra[F,O,?],R] =
    FreeC.Eval[Algebra[F,O,?],R](Eval(value))

  def acquire[F[_],O,R](resource: F[R], release: R => F[Unit]): FreeC[Algebra[F,O,?],(R,Token)] =
    FreeC.Eval[Algebra[F,O,?],(R,Token)](Acquire(resource, release))

  def release[F[_],O](token: Token): FreeC[Algebra[F,O,?],Unit] =
    FreeC.Eval[Algebra[F,O,?],Unit](Release(token))

  def unconsAsync[F[_],X,Y,O](s: FreeC[Algebra[F,O,?],Unit], ec: ExecutionContext)(implicit F: Effect[F]): FreeC[Algebra[F,X,?],AsyncPull[F,Option[(Segment[O,Unit], FreeC[Algebra[F,O,?],Unit])]]] =
    FreeC.Eval[Algebra[F,X,?],AsyncPull[F,Option[(Segment[O,Unit],FreeC[Algebra[F,O,?],Unit])]]](UnconsAsync(s, ec, F))

  private def openScope[F[_],O]: FreeC[Algebra[F,O,?],RunFoldScope[F]] =
    FreeC.Eval[Algebra[F,O,?],RunFoldScope[F]](OpenScope())

  private def closeScope[F[_],O](toClose: RunFoldScope[F]): FreeC[Algebra[F,O,?],Unit] =
    FreeC.Eval[Algebra[F,O,?],Unit](CloseScope(toClose))

  def scope[F[_],O,R](pull: FreeC[Algebra[F,O,?],R]): FreeC[Algebra[F,O,?],R] =
    openScope flatMap { newScope =>
      FreeC.Bind(pull, (e: Either[Throwable,R]) => e match {
        case Left(e) => closeScope(newScope) flatMap { _ => raiseError(e) }
        case Right(r) => closeScope(newScope) map { _ => r }
      })
    }

  def getScope[F[_],O]: FreeC[Algebra[F,O,?],RunFoldScope[F]] =
    FreeC.Eval[Algebra[F,O,?],RunFoldScope[F]](GetScope())

  def pure[F[_],O,R](r: R): FreeC[Algebra[F,O,?],R] =
    FreeC.Pure[Algebra[F,O,?],R](r)

  def raiseError[F[_],O,R](t: Throwable): FreeC[Algebra[F,O,?],R] =
    FreeC.Fail[Algebra[F,O,?],R](t)

  def suspend[F[_],O,R](f: => FreeC[Algebra[F,O,?],R]): FreeC[Algebra[F,O,?],R] =
    FreeC.suspend(f)

  final class Token {
    override def toString: String = s"T|${hashCode.toHexString}|"
  }


  def uncons[F[_],X,O](s: FreeC[Algebra[F,O,?],Unit], chunkSize: Int = 1024): FreeC[Algebra[F,X,?],Option[(Segment[O,Unit], FreeC[Algebra[F,O,?],Unit])]] = {
    s.viewL.get match {
      case done: FreeC.Pure[Algebra[F,O,?], Unit] => pure(None)
      case failed: FreeC.Fail[Algebra[F,O,?], _] => raiseError(failed.error)
      case bound: FreeC.Bind[Algebra[F,O,?],_,Unit] =>
        val f = bound.f.asInstanceOf[Either[Throwable,Any] => FreeC[Algebra[F,O,?],Unit]]
        val fx = bound.fx.asInstanceOf[FreeC.Eval[Algebra[F,O,?],_]].fr
        fx match {
          case os: Algebra.Output[F, O] =>
            pure[F,X,Option[(Segment[O,Unit], FreeC[Algebra[F,O,?],Unit])]](Some((os.values, f(Right(())))))
          case os: Algebra.Run[F, O, x] =>
            try {
              def asSegment(c: Catenable[Segment[O,Unit]]): Segment[O,Unit] =
                c.uncons.flatMap { case (h1,t1) => t1.uncons.map(_ => Segment.catenated(c)).orElse(Some(h1)) }.getOrElse(Segment.empty)
              os.values.splitAt(chunkSize) match {
                case Left((r,segments,rem)) =>
                  pure[F,X,Option[(Segment[O,Unit], FreeC[Algebra[F,O,?],Unit])]](Some(asSegment(segments) -> f(Right(r))))
                case Right((segments,tl)) =>
                  pure[F,X,Option[(Segment[O,Unit], FreeC[Algebra[F,O,?],Unit])]](Some(asSegment(segments) -> FreeC.Bind[Algebra[F,O,?],x,Unit](segment(tl), f)))
              }
            } catch { case NonFatal(e) => FreeC.suspend(uncons(f(Left(e)), chunkSize)) }
          case algebra => // Eval, Acquire, Release, OpenScope, CloseScope, UnconsAsync
            FreeC.Bind[Algebra[F,X,?],Any,Option[(Segment[O,Unit], FreeC[Algebra[F,O,?],Unit])]](
              FreeC.Eval[Algebra[F,X,?],Any](algebra.asInstanceOf[Algebra[F,X,Any]]),
              (x: Either[Throwable,Any]) => uncons[F,X,O](f(x), chunkSize)
            )
        }
      case e => sys.error("FreeC.ViewL structure must be Pure(a), Fail(e), or Bind(Eval(fx),k), was: " + e)
    }
  }


  /** Left-fold the output of a stream */
  def runFold[F[_],O,B](stream: FreeC[Algebra[F,O,?],Unit], init: B)(f: (B, O) => B)(implicit F: Sync[F]): F[B] =
    F.delay(RunFoldScope.newRoot).flatMap { scope =>
      runFoldScope[F,O,B](scope, stream, init)(f).attempt.flatMap {
        case Left(t) => scope.close *> F.raiseError(t)
        case Right(b) => scope.close as b
      }
    }

  private[fs2] def runFoldScope[F[_],O,B](scope: RunFoldScope[F], stream: FreeC[Algebra[F,O,?],Unit], init: B)(g: (B, O) => B)(implicit F: Sync[F]): F[B] =
    runFoldLoop[F,O,B](scope, init, g, uncons(stream).viewL)

  private def runFoldLoop[F[_],O,B](scope: RunFoldScope[F], acc: B, g: (B, O) => B, v: FreeC.ViewL[Algebra[F,O,?], Option[(Segment[O,Unit], FreeC[Algebra[F,O,?],Unit])]])(implicit F: Sync[F]): F[B] = {
    v.get match {
      case done: FreeC.Pure[Algebra[F,O,?], Option[(Segment[O,Unit], FreeC[Algebra[F,O,?],Unit])]] => done.r match {
        case None => F.pure(acc)
        case Some((hd, tl)) =>
          F.suspend {
            try runFoldLoop[F,O,B](scope, hd.fold(acc)(g).run, g, uncons(tl).viewL)
            catch { case NonFatal(e) => runFoldLoop[F,O,B](scope, acc, g, uncons(tl.asHandler(e)).viewL) }
          }
      }
      case failed: FreeC.Fail[Algebra[F,O,?], _] => F.raiseError(failed.error)
      case bound: FreeC.Bind[Algebra[F,O,?], _, Option[(Segment[O,Unit], FreeC[Algebra[F,O,?],Unit])]] =>
        val f = bound.f.asInstanceOf[
          Either[Throwable,Any] => FreeC[Algebra[F,O,?], Option[(Segment[O,Unit], FreeC[Algebra[F,O,?],Unit])]]]
        val fx = bound.fx.asInstanceOf[FreeC.Eval[Algebra[F,O,?],_]].fr
        fx match {
          case wrap: Algebra.Eval[F, O, _] =>
            F.flatMap(F.attempt(wrap.value)) { e => runFoldLoop(scope, acc, g, f(e).viewL) }

          case acquire: Algebra.Acquire[F,_,_] =>
            val acquireResource = acquire.resource
            val resource = Resource.create
            F.flatMap(scope.register(resource)) { mayAcquire =>
              if (mayAcquire) {
                F.flatMap(F.attempt(acquireResource)) {
                  case Right(r) =>
                    val finalizer = F.suspend { acquire.release(r) }
                    F.flatMap(resource.acquired(finalizer)) { result =>
                      runFoldLoop(scope, acc, g, f(result.right.map { _ => (r, resource.id) }).viewL)
                    }

                  case Left(err) =>
                    F.flatMap(scope.releaseResource(resource.id)) { result =>
                      val failedResult: Either[Throwable, Unit] =
                        result.left.toOption.map { err0 =>
                          Left(new CompositeFailure(NonEmptyList.of(err, err0)))
                         }.getOrElse(Left(err))
                      runFoldLoop(scope, acc, g, f(failedResult).viewL)
                    }
                }
              } else {
                F.raiseError(Interrupted) // todo: do we really need to signal this as an exception ?
              }
            }


          case release: Algebra.Release[F,_] =>
            F.flatMap(scope.releaseResource(release.token))  { result =>
              runFoldLoop(scope, acc, g, f(result).viewL)
            }

          case c: Algebra.CloseScope[F,_] =>
            F.flatMap(c.toClose.close) { result =>
              F.flatMap(c.toClose.openAncestor) { scopeAfterClose =>
                runFoldLoop(scopeAfterClose, acc, g, f(result).viewL)
              }
            }

          case o: Algebra.OpenScope[F,_] =>
            F.flatMap(scope.open) { innerScope =>
              runFoldLoop(innerScope, acc, g, f(Right(innerScope)).viewL)
            }

          case e: GetScope[F,_] =>
            F.suspend {
              runFoldLoop(scope, acc, g, f(Right(scope)).viewL)
            }

          case unconsAsync: Algebra.UnconsAsync[F,_,_,_] =>
            val s = unconsAsync.s
            implicit val ec = unconsAsync.ec
            implicit val F = unconsAsync.eff
            type UO = Option[(Segment[_,Unit], FreeC[Algebra[F,Any,?],Unit])]
            val asyncPull: F[AsyncPull[F,UO]] =
              F.flatMap(async.ref[F,Either[Throwable,UO]]) { ref =>
                val pull =
                  F.flatMap(F.attempt(
                    runFoldScope(
                      scope,
                      uncons(s.asInstanceOf[FreeC[Algebra[F,Any,?],Unit]]).flatMap(output1(_)),
                      None: UO
                    )((_, snd) => snd)
                  )) { o => ref.setAsyncPure(o) }

               F.map(async.fork(pull))(_ => AsyncPull.readAttemptRef(ref))
            }
            F.flatMap(asyncPull) { ap => runFoldLoop(scope, acc, g, f(Right(ap)).viewL) }



          case _ => sys.error("impossible Segment or Output following uncons")
        }
      case e => sys.error("FreeC.ViewL structure must be Pure(a), Fail(e), or Bind(Eval(fx),k), was: " + e)
    }
  }

  def translate[F[_],G[_],O,R](fr: FreeC[Algebra[F,O,?],R], u: F ~> G)(implicit eff: Effect[G]): FreeC[Algebra[G,O,?],R] = {
    def algFtoG[O2]: Algebra[F,O2,?] ~> Algebra[G,O2,?] = new (Algebra[F,O2,?] ~> Algebra[G,O2,?]) { self =>
      def apply[X](in: Algebra[F,O2,X]): Algebra[G,O2,X] = in match {
        case o: Output[F,O2] => Output[G,O2](o.values)
        case Run(values) => Run[G,O2,X](values)
        case Eval(value) => Eval[G,O2,X](u(value))
        case a: Acquire[F,O2,_] => Acquire(u(a.resource), r => u(a.release(r)))
        case r: Release[F,O2] => Release[G,O2](r.token)
        case os: OpenScope[F,O2] => os.asInstanceOf[Algebra[G,O2,X]]
        case cs: CloseScope[F,O2] => cs.asInstanceOf[CloseScope[G,O2]]
        case gs: GetScope[F,O2] => gs.asInstanceOf[Algebra[G,O2,X]]
        case ua: UnconsAsync[F,_,_,_] =>
          val uu: UnconsAsync[F,Any,Any,Any] = ua.asInstanceOf[UnconsAsync[F,Any,Any,Any]]
          UnconsAsync(uu.s.translate[Algebra[G,Any,?]](algFtoG), uu.ec, eff).asInstanceOf[Algebra[G,O2,X]]
      }
    }
    fr.translate[Algebra[G,O,?]](algFtoG)
  }
}
