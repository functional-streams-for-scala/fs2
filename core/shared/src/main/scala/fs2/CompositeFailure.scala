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

import cats.data.NonEmptyList

/** Represents multiple (>1) exceptions were thrown. */
case class CompositeFailure(
    head: Throwable,
    tail: NonEmptyList[Throwable]
) extends Throwable(
      s"Multiple exceptions were thrown (${1 + tail.size}), first ${head.getClass.getName}: ${head.getMessage}",
      head
    ) {

  /** Gets all causes (guaranteed to have at least 2 elements). */
  def all: NonEmptyList[Throwable] = head :: tail
}

object CompositeFailure {
  def apply(
      first: Throwable,
      second: Throwable,
      rest: List[Throwable] = List.empty
  ): CompositeFailure =
    apply(first, NonEmptyList(second, rest))

  def apply(first: Throwable, rest: NonEmptyList[Throwable]): CompositeFailure = {
    def flattenExceptions(throwable: Throwable): NonEmptyList[Throwable] =
      throwable match {
        case cf: CompositeFailure => cf.all
        case throwable            => NonEmptyList.one(throwable)
      }
    first match {
      case cf: CompositeFailure =>
        val tail = cf.tail ::: rest.flatMap(flattenExceptions)
        new CompositeFailure(cf.head, tail)
      case _ =>
        new CompositeFailure(first, rest.flatMap(flattenExceptions))
    }
  }

  def fromNel(errors: NonEmptyList[Throwable]): Throwable =
    errors match {
      case NonEmptyList(hd, Nil)               => hd
      case NonEmptyList(first, second :: rest) => apply(first, second, rest)
    }

  def fromList(errors: List[Throwable]): Option[Throwable] =
    errors match {
      case Nil                     => None
      case hd :: Nil               => Some(hd)
      case first :: second :: rest => Some(apply(first, second, rest))
    }

  /** Builds composite failure from the results supplied.
    *
    * - When any of the results are on left, then the Left(err) is returned
    * - When both results fail, the Left(CompositeFailure(_)) is returned
    * - When both results succeeds then Right(()) is returned
    */
  def fromResults(
      first: Either[Throwable, Unit],
      second: Either[Throwable, Unit]
  ): Either[Throwable, Unit] =
    first match {
      case Right(_) => second
      case Left(err) =>
        Left(second.fold(err1 => apply(err, err1, Nil), _ => err))
    }
}
