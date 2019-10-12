package fs2

import scala.reflect.ClassTag

import scodec.bits.ByteVector

/**
  * Supports building a result of type `Out` from zero or more `Chunk[A]`.
  *
  * This is similar to the standard library collection builders but optimized for
  * building a collection from a stream.
  *
  * The companion object provides implicit conversions (methods starting with `supports`),
  * which adapts various collections to the `Collector` trait.
  *
  * The output type is a type member instead of a type parameter to avoid overloading
  * resolution limitations with `s.compile.to[C]` vs `s.compile.to(C)`.
  */
trait Collector[A] {
  type Out
  def newBuilder: Collector.Builder[A, Out]
}

object Collector extends CollectorPlatform {
  type Aux[A, X] = Collector[A] { type Out = X }

  def string: Collector.Aux[String, String] =
    make(Builder.string)

  implicit def supportsArray[A: ClassTag](a: Array.type): Collector.Aux[A, Array[A]] = {
    val _ = a
    make(implicitly[ClassTag[A]] match {
      case ClassTag.Byte =>
        Builder.byteArray.asInstanceOf[Builder[A, Array[A]]]
      case _ => Builder.array[A]
    })
  }

  implicit def supportsChunk[A](c: Chunk.type): Collector.Aux[A, Chunk[A]] = {
    val _ = c
    make(Builder.chunk)
  }

  implicit def supportsByteVector(b: ByteVector.type): Collector.Aux[Byte, ByteVector] = {
    val _ = b
    make(Builder.byteVector)
  }

  protected def make[A, X](nb: => Builder[A, X]): Collector.Aux[A, X] =
    new Collector[A] {
      type Out = X
      def newBuilder = nb
    }

  /** Builds a value of type `X` from zero or more `Chunk[A]`. */
  trait Builder[A, X] { self =>
    def +=(c: Chunk[A]): Unit
    def result: X

    def mapResult[Y](f: X => Y): Builder[A, Y] = new Builder[A, Y] {
      def +=(c: Chunk[A]): Unit = self += c
      def result: Y = f(self.result)
    }
  }

  object Builder extends BuilderPlatform {
    def byteArray: Builder[Byte, Array[Byte]] =
      byteVector.mapResult(_.toArray)

    def chunk[A]: Builder[A, Chunk[A]] =
      new Builder[A, Chunk[A]] {
        private[this] var queue = Chunk.Queue.empty[A]
        def +=(c: Chunk[A]): Unit = queue = queue :+ c
        def result: Chunk[A] = queue.toChunk
      }

    def array[A: ClassTag]: Builder[A, Array[A]] =
      chunk.mapResult(_.toArray)

    protected def fromBuilder[A, C[_], B](
        builder: collection.mutable.Builder[A, C[B]]
    ): Builder[A, C[B]] =
      new Builder[A, C[B]] {
        def +=(c: Chunk[A]): Unit = builder ++= c.iterator
        def result: C[B] = builder.result
      }

    def string: Builder[String, String] =
      new Builder[String, String] {
        private[this] val builder = new StringBuilder
        def +=(c: Chunk[String]): Unit = c.foreach(s => builder ++= s)
        def result: String = builder.toString
      }

    def byteVector: Builder[Byte, ByteVector] =
      new Builder[Byte, ByteVector] {
        private[this] var acc = ByteVector.empty
        def +=(c: Chunk[Byte]): Unit = acc = acc ++ c.toByteVector
        def result: ByteVector = acc
      }
  }
}
