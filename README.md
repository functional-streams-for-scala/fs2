FS2: Functional Streams for Scala (previously 'Scalaz-Stream')
=============

[![Build Status](https://travis-ci.org/functional-streams-for-scala/fs2.svg?branch=series/0.9)](http://travis-ci.org/functional-streams-for-scala/fs2)
[![Gitter Chat](https://badges.gitter.im/functional-streams-for-scala/fs2.svg)](https://gitter.im/functional-streams-for-scala/fs2)

Quick links:

* [About the library](#about)
* [How to get latest version](#getit)
* [API docs (fs2-core)][core-api], [API docs (fs2-io)][io-api]
* [Docs and getting help](#docs)

[io-api]: https://oss.sonatype.org/service/local/repositories/releases/archive/co/fs2/fs2-io_2.12/0.9.2/fs2-io_2.12-0.9.2-javadoc.jar/!/index.html#package
[core-api]: https://oss.sonatype.org/service/local/repositories/releases/archive/co/fs2/fs2-core_2.12/0.9.2/fs2-core_2.12-0.9.2-javadoc.jar/!/index.html#package

### <a id="about"></a>About the library ###

FS2 is a streaming I/O library. The design goals are compositionality, expressiveness, resource safety, and speed. Here's a simple example of its use:

``` scala
import fs2.{io, text, Task}
import java.nio.file.Paths

def fahrenheitToCelsius(f: Double): Double =
  (f - 32.0) * (5.0/9.0)

val converter: Task[Unit] =
  io.file.readAll[Task](Paths.get("testdata/fahrenheit.txt"), 4096)
    .through(text.utf8Decode)
    .through(text.lines)
    .filter(s => !s.trim.isEmpty && !s.startsWith("//"))
    .map(line => fahrenheitToCelsius(line.toDouble).toString)
    .intersperse("\n")
    .through(text.utf8Encode)
    .through(io.file.writeAll(Paths.get("testdata/celsius.txt")))
    .run

// at the end of the universe...
val u: Unit = converter.unsafeRun()
```

This will construct a `Task`, `converter`, which reads lines incrementally from `testdata/fahrenheit.txt`, skipping blanklines and commented lines. It then parses temperatures in degrees Fahrenheit, converts these to Celsius, UTF-8 encodes the output, and writes incrementally to `testdata/celsius.txt`, using constant memory. The input and output files will be closed upon normal termination or if exceptions occur.

The library supports a number of other interesting use cases:
I
* _Zipping and merging of streams:_ A streaming computation may read from multiple sources in a streaming fashion, zipping or merging their elements using an arbitrary `Tee`. In general, clients have a great deal of flexibility in what sort of topologies they can define--source, sink, and effectful channels are all first-class concepts in the library.
* _Dynamic resource allocation:_ A streaming computation may allocate resources dynamically (for instance, reading a list of files to process from a stream built off a network socket), and the library will ensure these resources get released upon normal termination or if exceptions occur.
* _Nondeterministic and concurrent processing:_ A computation may read from multiple input streams simultaneously, using whichever result comes back first, and a pipeline of transformations can allow for nondeterminism and queueing at each stage.

### <a id="docs"></a>Documentation and getting help ###

* [The official guide](docs/guide.md) is a good starting point for learning more about the library.
* Also feel free to come discuss and ask/answer questions in [the gitter channel](https://gitter.im/functional-streams-for-scala/fs2) and/or on StackOverflow using [the tag FS2](http://stackoverflow.com/tags/fs2).

Blog posts and other external resources are listed on the [Additional Resources](https://github.com/functional-streams-for-scala/fs2/wiki/Additional-Resources) page.

### <a id="getit"></a> Where to get the latest version ###

The 0.9 release is out and we recommend upgrading. You may want to first [read the migration guide](docs/migration-guide.md) if you are upgrading from 0.8 or earlier. To get 0.9, add the following to your SBT build:

```
// available for Scala 2.11.8, 2.12.0
libraryDependencies += "co.fs2" %% "fs2-core" % "0.9.2"

// optional I/O library
libraryDependencies += "co.fs2" %% "fs2-io" % "0.9.2"
```

The fs2-core library is also supported on Scala.js:

```
// available for Scala 2.11.8, 2.12.0
libraryDependencies += "co.fs2" %%% "fs2-core" % "0.9.2"
```

API docs:

* [The core library][core-api]
* [The `io` library][io-api], FS2 bindings for NIO-based file I/O and TCP/UDP networking

The previous stable release is 0.8.4 ([source](https://github.com/functional-streams-for-scala/fs2/tree/release/0.8.4)).

### Projects using FS2 ###

If you have a project you'd like to include in this list, either open a PR or let us know in [the gitter channel](https://gitter.im/functional-streams-for-scala/fs2) and we'll add a link to it here.

* [doobie](https://github.com/tpolecat/doobie): Pure functional JDBC built on fs2.
* [http4s](http://http4s.org/): Minimal, idiomatic Scala interface for HTTP services using fs2.
* [scodec-stream](https://github.com/scodec/scodec-stream): A library for streaming binary decoding and encoding, built using fs2 and [scodec](https://github.com/scodec/scodec).
* [streamz](https://github.com/krasserm/streamz): A library that supports the conversion of [Akka Stream](http://doc.akka.io/docs/akka/2.4/scala/stream/index.html) `Source`s, `Flow`s and `Sink`s to and from FS2 `Stream`s, `Pipe`s and `Sink`s, respectively. It also supports the usage of [Apache Camel](http://camel.apache.org/) endpoints in FS2 `Stream`s.

### Related projects ###

FS2 has evolved from earlier work on streaming APIs in Scala and Haskell and in Scala. Some influences:

* [Machines](https://github.com/ekmett/machines/), a Haskell library by Ed Kmett, which spawned [`scala-machines`](https://github.com/runarorama/scala-machines)
* [The FP in Scala stream processing library](https://github.com/fpinscala/fpinscala/blob/master/answers/src/main/scala/fpinscala/streamingio/StreamingIO.scala) developed for the book [FP in Scala](https://www.manning.com/books/functional-programming-in-scala)
* [Reflex](https://hackage.haskell.org/package/reflex), an FRP library in Haskell, by Ryan Trinkle
* There are various other iteratee-style libraries for doing compositional, streaming I/O in Scala, notably the [`scalaz/iteratee`](https://github.com/scalaz/scalaz/tree/scalaz-seven/iteratee) package and [iteratees in Play](https://www.playframework.com/documentation/2.0/Iteratees).

### Presentations, Blogs, etc. ###

See [Additional resources](https://github.com/functional-streams-for-scala/fs2/wiki/Additional-Resources).
