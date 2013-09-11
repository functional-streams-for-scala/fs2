package scalaz.stream

import scalaz.{Equal, Nondeterminism}
import scalaz.syntax.equal._
import scalaz.std.anyVal._
import scalaz.std.list._
import scalaz.std.list.listSyntax._

import org.scalacheck._
import Prop._

object ProcessSpec extends Properties("Process1") {
  
  import Process._
  import process1._

  implicit def EqualProcess[A:Equal]: Equal[Process0[A]] = new Equal[Process0[A]] {
    def equal(a: Process0[A], b: Process0[A]): Boolean = 
      a.toList == b.toList
  }
  implicit def ArbProcess0[A:Arbitrary]: Arbitrary[Process0[A]] = 
    Arbitrary(Arbitrary.arbitrary[List[A]].map(a => Process(a: _*)))

  property("basic") = forAll { (p: Process0[Int], p2: Process0[String], n: Int) => 
    val f = (x: Int) => List.range(1, x.min(100))
    val g = (x: Int) => x % 7 == 0
    ("id" |: { 
      ((p |> id) === p) &&  ((id |> p) === p)
    }) &&
    ("map" |: {
      (p.toList.map(_ + 1) === p.map(_ + 1).toList) && 
      (p.map(_ + 1) === p.pipe(lift(_ + 1)))
    }) &&
    ("flatMap" |: {
      (p.toList.flatMap(f) === p.flatMap(f andThen Process.emitAll).toList)
    }) && 
    ("filter" |: {
      (p.toList.filter(g) === p.filter(g).toList)
    }) && 
    ("take" |: {
      (p.toList.take(n) === p.take(n).toList)
    }) && 
    ("takeWhile" |: {
      (p.toList.takeWhile(g) === p.takeWhile(g).toList)
    }) && 
    ("drop" |: {
      (p.toList.drop(n) === p.drop(n).toList)
    }) &&
    ("dropWhile" |: {
      (p.toList.dropWhile(g) === p.dropWhile(g).toList)
    }) && 
    ("zip" |: {
      (p.toList.zip(p2.toList) === p.zip(p2).toList)
    }) && 
    ("yip" |: {
      val l = p.toList.zip(p2.toList)
      val r = p.toSource.yip(p2.toSource).collect.run.toList
      (l === r)
    }) &&
    ("fold" |: {
      p.toList.scanLeft(0)(_ - _) ===
      p.toSource.fold(0)(_ - _).collect.run.toList
    }) &&
    ("sum" |: {
      p.toList.sum[Int] ===
      p.toSource.pipe(process1.sum).runLastOr(0).run
    }) &&
    ("intersperse" |: {
      p.intersperse(0).toList == p.toList.intersperse(0) 
    })
  }

  property("fill") = forAll(Gen.choose(0,30).map2(Gen.choose(0,50))((_,_))) { 
    case (n,chunkSize) => Process.fill(n)(42, chunkSize).collect.run.toList == List.fill(n)(42) 
  }

  import scalaz.concurrent.Task

  property("enqueue") = secure {
    val tasks = Process.range(0,1000).map(i => Task { Thread.sleep(1); 1 })
    tasks.sequence(50).pipe(processes.sum[Int].last).collect.run.head == 1000 &&
    tasks.gather(50).pipe(processes.sum[Int].last).collect.run.head == 1000
  }

  // ensure that zipping terminates when the smaller stream runs out
  property("zip one side infinite") = secure {
    val ones = Process.wrap(Task.now(1)).repeat 
    val p = Process(1,2,3) 
    ones.zip(p).collect.run == IndexedSeq(1 -> 1, 1 -> 2, 1 -> 3) &&
    p.zip(ones).collect.run == IndexedSeq(1 -> 1, 2 -> 1, 3 -> 1) 
  }

  property("merge") = secure {
    import concurrent.duration._
    val sleepsL = Process.awakeEvery(1 seconds).take(3)
    val sleepsR = Process.awakeEvery(100 milliseconds).take(30)
    val sleeps = sleepsL merge sleepsR
    val p = sleeps.toTask
    val tasks = List.fill(10)(p.timed(500 milliseconds).attemptRun)
    tasks.forall(_.isRight)
  }

  property("forwardFill") = secure {
    import concurrent.duration._
    val t2 = Process.awakeEvery(2 seconds).forwardFill.zip {
             Process.awakeEvery(100 milliseconds).take(100)
           }.run.run 
    true
  }

  property("range") = secure {
    Process.range(0, 100).collect.run == IndexedSeq.range(0, 100) &&
    Process.range(0, 1).collect.run == IndexedSeq.range(0, 1) && 
    Process.range(0, 0).collect.run == IndexedSeq.range(0, 0) 
  }

  property("ranges") = forAll(Gen.choose(1, 101)) { size => 
    Process.ranges(0, 100, size).flatMap { case (i,j) => emitSeq(i until j) }.collect.run ==
    IndexedSeq.range(0, 100)
  }

  property("liftL") = secure {
    import scalaz.\/._
    val s = Process.range(0, 100000)
    val p = s.map(left) pipe process1.id[Int].liftL
    true
  }

  property("feedL") = secure {
    val w = wye.feedL(List.fill(10)(1))(process1.id)
    val x = Process.range(0,100).wye(halt)(w).collect.run
    x.toList == (List.fill(10)(1) ++ List.range(0,100))
  }

  property("feedR") = secure {
    val w = wye.feedR(List.fill(10)(1))(wye.merge[Int])
    val x = Process.range(0,100).wye(halt)(w).collect.run
    x.toList == (List.fill(10)(1) ++ List.range(0,100))
  }

  property("either") = secure {
    val w = wye.either[Int,Int] 
    val s = Process.constant(1).take(1)
    s.wye(s)(w).collect.run.map(_.fold(identity, identity)).toList == List(1,1)
  }
}

