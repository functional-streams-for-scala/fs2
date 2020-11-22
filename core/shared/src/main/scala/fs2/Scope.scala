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

package fs2

import cats.MonadError
import cats.syntax.all._

/** Represents a period of stream execution in which resources are acquired and released.
  *
  * Note: this type is generally used to implement low-level actions that manipulate
  * resource lifetimes and hence, isn't generally used by user-level code.
  */
abstract class Scope[F[_]] {

  /** Leases the resources of this scope until the returned lease is cancelled.
    *
    * Note that this leases all resources in this scope, resources in all parent scopes (up to root)
    * and resources of all child scopes.
    *
    * `None` is returned if this scope is already closed. Otherwise a lease is returned,
    * which must be cancelled. Upon cancellation, resource finalizers may be run, depending on the
    * state of the owning scopes.
    *
    * Resources may be finalized during the execution of this method and before the lease has been acquired
    * for a resource. In such an event, the already finalized resource won't be leased. As such, it is
    * important to call `lease` only when all resources are known to be non-finalized / non-finalizing.
    *
    * When the lease is returned, all resources available at the time `lease` was called have been
    * successfully leased.
    */
  def lease: F[Option[Scope.Lease[F]]]

  /** Like [[lease]], but fails with an error if the scope is closed. */
  def leaseOrError(implicit F: MonadError[F, Throwable]): F[Scope.Lease[F]] =
    lease.flatMap {
      case Some(l) => F.pure(l)
      case None    => F.raiseError(new Throwable("Scope closed at time of lease"))
    }

  /** Interrupts evaluation of the current scope. Only scopes previously indicated with Stream.interruptScope may be interrupted.
    * For other scopes this will fail.
    *
    * Interruption is final and may take two forms:
    *
    * When invoked on right side, that will interrupt only current scope evaluation, and will resume when control is given
    * to next scope.
    *
    * When invoked on left side, then this will inject given throwable like it will be caused by stream evaluation,
    * and then, without any error handling the whole stream will fail with supplied throwable.
    */
  @deprecated(
    "Unsound when used with translateInterruptible - use Stream.interruptWhen instead",
    "2.4.6"
  )
  def interrupt(cause: Either[Throwable, Unit]): F[Unit]
}

object Scope {

  /** Represents one or more resources that were leased from a scope, causing their
    * lifetimes to be extended until `cancel` is invoked on this lease.
    */
  abstract class Lease[F[_]] {

    /** Cancels the lease of all resources tracked by this lease.
      *
      * This may run finalizers on some of the resources (depending on the state of their owning scopes).
      * If one or more finalizers fail, the returned action completes with a `Left(t)`, providing the failure.
      */
    def cancel: F[Either[Throwable, Unit]]
  }
}
