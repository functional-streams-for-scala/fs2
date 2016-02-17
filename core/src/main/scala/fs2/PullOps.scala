package fs2

import fs2.util.{RealSupertype,Sub1}

private[fs2]
trait PullOps[+F[_],+W,+R] { self: Pull[F,W,R] =>

  /** Definition: `p as r == p map (_ => r)`. */
  def as[R2](r: R2): Pull[F,W,R2] = self map (_ => r)

  def covary[F2[_]](implicit S: Sub1[F,F2]): Pull[F2,W,R] = Sub1.substPull(self)

  def flatMap[F2[_],W2>:W,R2](f: R => Pull[F2,W2,R2])(implicit S: Sub1[F,F2], T: RealSupertype[W,W2]): Pull[F2,W2,R2] =
    Pull.flatMap(self.covary[F2]: Pull[F2,W2,R])(f)

  def filter(f: R => Boolean): Pull[F,W,R] = withFilter(f)

  def map[R2](f: R => R2): Pull[F,W,R2] =
    Pull.map(self)(f)

  def optional: Pull[F,W,Option[R]] =
    self.map(Some(_)).or(Pull.pure(None))

  def or[F2[x]>:F[x],W2>:W,R2>:R](p2: => Pull[F2,W2,R2])(
    implicit S1: RealSupertype[W,W2], R2: RealSupertype[R,R2])
    : Pull[F2,W2,R2]
    = Pull.or(self, p2)

  def withFilter(f: R => Boolean): Pull[F,W,R] =
    self.flatMap(r => if (f(r)) Pull.pure(r) else Pull.done)

  /** Defined as `p >> p2 == p flatMap { _ => p2 }`. */
  def >>[F2[x]>:F[x],W2>:W,R2](p2: => Pull[F2,W2,R2])(implicit S: RealSupertype[W,W2])
  : Pull[F2,W2,R2] = self flatMap { _ => p2 }
}
