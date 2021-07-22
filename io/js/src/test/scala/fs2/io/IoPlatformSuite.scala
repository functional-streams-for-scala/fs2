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
package io

import cats.effect.IO
import fs2.Fs2Suite
import org.scalacheck.effect.PropF.forAllF

class IoPlatformSuite extends Fs2Suite {

  test("to/from Readable") {
    forAllF { bytes: Stream[Pure, Byte] =>
      bytes
        .through(toReadable[IO])
        .flatMap { readable =>
          readReadable(IO.pure(readable))
        }
        .compile
        .toVector
        .assertEquals(bytes.compile.toVector)
    }
  }

  test("mk/from Writable") {
    forAllF { bytes: Stream[Pure, Byte] =>
      readWritable[IO] { writable =>
        bytes.covary[IO].through(writeWritable(IO.pure(writable))).compile.drain
      }.compile.toVector.assertEquals(bytes.compile.toVector)
    }
  }

}
