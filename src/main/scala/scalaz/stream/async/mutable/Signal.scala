package scalaz.stream.async.mutable

import scalaz.concurrent._
import scalaz.stream.Process._


/**
 * A signal whose value may be set asynchronously. Provides continuous 
 * and discrete streams for responding to changes to this value. 
 */
trait Signal[A] extends scalaz.stream.async.immutable.Signal[A] {



  /**
   * Returns sink that can be used to set this signal
   */
  def sink: Sink[Task, Signal.Msg[A]]


  /** 
   * Asynchronously refreshes the value of the signal, 
   * keep the value of this `Signal` the same, but notify any listeners.
   * If the `Signal` is not yet set, this is no-op
   */
  def refresh: Task[Unit] = compareAndSet(oa=>oa).map(_=>())

  /**
   * Asynchronously get the current value of this `Signal`
   */
  def get: Task[A]


  /**
   * Sets the value of this `Signal`. 
   */
  def set(a: A): Task[Unit]

  /**
   * Asynchronously sets the current value of this `Signal` and returns previous value of the `Signal`.
   * If this `Signal` has not been set yet, the Task will return None and value is set. If this `Signal`
   * is `finished` Task will fail with `End` exception. If this `Signal` is `failed` Task will fail
   * with `Signal` failure exception.
   *
   */
  def getAndSet(a:A) : Task[Option[A]]

  /**
   * Asynchronously sets the current value of this `Signal` and returns new value os this `Signal`.
   * If this `Signal` has not been set yet, the Task will return None and value is set. If this `Signal`
   * is `finished` Task will fail with `End` exception. If this `Signal` is `failed` Task will fail 
   * with `Signal` failure exception.
   *
   * Furthermore if `f` results in evaluating to None, this Task is no-op and will return current value of the
   * `Signal`.
   *
   */
  def compareAndSet(f: Option[A] => Option[A]) : Task[Option[A]]

  /**
   * Indicate that the value is no longer valid. Any attempts to `set` or `get` this
   * `Signal` after a `close` will fail with `End` exception. This `Signal` is `finished` from now on.
   *
   * Running this task once the `Signal` is `failed` or `finished` is no-op and this task will not fail.
   *
   * Please note this task will get completed _after_ all open gets and sets on the signal will get completed
   */
  def close : Task[Unit] = fail(End)

  /**
   * Raise an asynchronous error for readers of this `Signal`. Any attempts to 
   * `set` or `get` this `Ref` after the `fail` will result in task failing with `error`. 
   * This `Signal` is `failed` from now on. 
   * 
   * Running this task once the `Signal` is `failed` or `finished` is no-op and this task will not fail. 
   */
  def fail(error: Throwable): Task[Unit]


}


object Signal {

  sealed trait Msg[+A]

  /**
   * Sets the signal to given value 
   */
  case class Set[A](a:A) extends Msg[A]

  /**
   * Conditionally sets signal to given value. Acts similarly as `Signal.compareAndSet`
   */
  case class CompareAndSet[A](f:Option[A] => Option[A]) extends Msg[A]


  /**
   * Fails the current signal with supplied exception. Acts similarly as `Signal.fail`
   */
  case class Fail(err:Throwable) extends Msg[Nothing]


}
