package fs2
package time

import org.scalacheck.Gen
import scala.concurrent.duration._

import Stream._
import fs2.util.Task

class TimeSpec extends Fs2Spec {

  "time" - {

    "awakeEvery" in {
      time.awakeEvery[Task](100.millis).map(_.toMillis/100).take(5).runLog.unsafeRun shouldBe Vector(1,2,3,4,5)
    }

    "awakeEvery liveness" in {
      val s = time.awakeEvery[Task](1.milli).evalMap { i => Task.async[Unit](cb => S(cb(Right(())))) }.take(200)
      runLog { concurrent.join(5)(Stream(s, s, s, s, s)) }
    }

    "duration" in {
      val firstValueDiscrepancy = time.duration[Task].take(1).runLog.unsafeRun.last
      val reasonableErrorInMillis = 200
      val reasonableErrorInNanos = reasonableErrorInMillis * 1000000
      def p = firstValueDiscrepancy.toNanos < reasonableErrorInNanos

      withClue("first duration is near zero on first run") { assert(p) }
      Thread.sleep(reasonableErrorInMillis)
      withClue("first duration is near zero on second run") { assert(p) }
    }

    "every" in {
      val smallDelay = Gen.choose(10, 300) map {_.millis}
      forAll(smallDelay) { delay: FiniteDuration =>
        type BD = (Boolean, FiniteDuration)
        val durationSinceLastTrue: Pipe[Pure,BD,BD] = {
          def go(lastTrue: FiniteDuration): Handle[Pure,BD] => Pull[Pure,BD,Handle[Pure,BD]] = h => {
            h.receive1 {
              case pair #: tl =>
                pair match {
                  case (true , d) => Pull.output1((true , d - lastTrue)) >> go(d)(tl)
                  case (false, d) => Pull.output1((false, d - lastTrue)) >> go(lastTrue)(tl)
                }
            }
          }
          _ pull go(0.seconds)
        }

        val draws = (600.millis / delay) min 10 // don't take forever

        val durationsSinceSpike = time.every[Task](delay).
          zip(time.duration[Task]).
          take(draws.toInt).
          through(durationSinceLastTrue)

        val result = durationsSinceSpike.runLog.unsafeRun.toList
        val (head :: tail) = result

        withClue("every always emits true first") { assert(head._1) }
        withClue("true means the delay has passed") { assert(tail.filter(_._1).map(_._2).forall { _ >= delay }) }
        withClue("false means the delay has not passed") { assert(tail.filterNot(_._1).map(_._2).forall { _ <= delay }) }
      }
    }
  }
}

