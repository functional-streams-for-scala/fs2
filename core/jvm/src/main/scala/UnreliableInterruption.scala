import cats.effect.concurrent.{Deferred, Ref}
import cats.effect.{ExitCode, IO, IOApp}
import fs2.{Pipe, Pull, Stream}

object UnreliableInterruption extends IOApp {
  private def resume[A, B](mk: A => Stream[IO, B], checkpoint: B => A)(start: A): Stream[IO, B] = {
    def go(s: Stream[IO, Either[Throwable, B]], watermark: A): Pull[IO, B, Unit] = s.pull.uncons1.flatMap {
      case Some((Right(b), rest)) => Pull.output1(b) >> go(rest, checkpoint(b))
      case Some((Left(_), _)) => go(mk(watermark).attempt, watermark)
      case None => go(mk(watermark).attempt, watermark)
    }

    go(mk(start).attempt, start).stream
  }

  // Interrupt the stream after a five items, up to a max number of times
  private def interrupter[A](deferred: Deferred[IO, Unit], maxInterrupts: Int, interruptCount: Ref[IO, Int]): Pipe[IO, A, A] = {
    input: Stream[IO, A] =>
      input.zipWithIndex
        .evalTap {
          case (_, 5) => interruptCount.getAndUpdate(_ + 1).flatMap { i =>
            if (i < maxInterrupts) {
              deferred.complete(())
            } else IO.unit
          }
          case _ => IO.unit
        }
        .map(_._1)
        .interruptWhen(deferred.get.attempt)
  }


  def run(args: List[String]): IO[ExitCode] = {
    val stream: Int => Stream[IO, Int] = Stream.iterate(_)(_ + 1)

    val usuallyWorks = for {
      interruptCount <- Ref.of[IO, Int](0)
      msg <- resume[Int, Int](
        start => Stream.eval(Deferred[IO, Unit]).flatMap(d => stream(start).through(interrupter(d, 1, interruptCount))),
        _ + 1
      )(0)
        .take(1000)
        .compile
        .toList
        .map(lst => s"${lst.size} should be 1000, meaning it restarted once")
    } yield msg

    val usuallyDoesNot = for {
      interruptCount <- Ref.of[IO, Int](0)
      msg <- resume[Int, Int](
        start => Stream.eval(Deferred[IO, Unit]).flatMap(d => stream(start).through(interrupter(d, 10, interruptCount))),
        _ + 1
      )(0)
        .take(1000)
        .compile
        .toList
        .map(lst => s"${lst.size} should be 1000, meaning it restarted 10 times (but it's not)")
    } yield msg

    val output = for {
      a <- usuallyWorks
      b <- usuallyDoesNot
    } yield List(a, b).mkString("\n")

    output.map(println).as(ExitCode.Success)
  }
}
