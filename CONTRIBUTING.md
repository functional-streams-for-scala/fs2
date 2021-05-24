# Contributing

If you want to contribute to the project, you can find some helpful information here.

## How can I help?

Thanks for taking interest in helping us develop FS2!

We welcome all kinds of contribution, including but not limited to:

- 📖 documentation improvements, explanatory images/diagrams, fixes in typos, useful links
- 🧹 refactorings of messy code, build structure, increasing test coverage or quality
- 🚀 new features and bugfixes (including [bug reports and feature requests][fs2-issues]).

Writing documentation is valuable for learning, so if you find some explanation insufficient, overly complicated or incorrect, it's a perfect opportunity to make a change to it!

If at any point you run into problems, you can always ask a question on [the fs2-dev channel on the Typelevel Discord][fs2-dev].

## How to submit a change

If you see something worth adding, make the relevant changes in a fork of the source code and [submit a pull request to the project](fs2-pulls). If you don't know what you could help with, take a look at [the issues marked as "help wanted"][low-hanging-fruit] or ask on [Discord][fs2-dev].

We follow similar rules to [the cats-effect project's](https://github.com/typelevel/cats-effect#development).
Most importantly, any contributions are expected to be made in the form of [GitHub pull requests to the FS2 repository][fs2-pulls].
Usually it takes two approvals for a change to be merged. If it takes too long to get it approved, feel free to ask on [Discord][fs2-dev].

Remember to follow the [code of conduct][coc] in online and offline discourse.

## Building the project locally

### Prerequisites

You'll need JDK 11, [sbt][sbt], [Node.js][node] (for running Scala.js tests) and [Jekyll][jekyll] (for building the microsite).

We use several sbt plugins to build and check the project, including [MiMa (Migration Manager)][mima], [scalafmt][scalafmt] and [sbt-microsites][sbt-microsites].

### Build process

To compile the code for the whole repository, you can start an interactive sbt shell:

```bash
$ sbt
[info] Loading global plugins from /Users/contributor/.sbt/1.0/plugins
[info] Loading settings for project fs2-build from plugins.sbt,metals.sbt ...
[info] Loading project definition from /Users/contributor/dev/fs2/project
[info] Loading settings for project root from version.sbt,build.sbt ...
[info] Set current project to root (in build file:/Users/contributor/dev/fs2/)
sbt:root>
```

Inside the shell, you can compile the sources for the currently selected Scala version using the `compile` command.
To compile the code for all Scala versions enabled in the build, use `+compile`. To include tests, `test:compile` or `+test:compile`, accordingly.


### Testing

To test the code, you can run the `test` command in sbt.
If you want the tests on a single plaform, you can use `testJVM` or `testJS` instead (these are defined as aliases in the `build.sbt` file).

It is possible to run a single test suite from a project on a single platform by [executing a more specific task](https://www.scala-sbt.org/1.x/docs/Testing.html#testOnly), like `coreJVM/testOnly fs2.PullSpec`.

You can list all available projects by executing the `projects` task:

```sbt
sbt:root> projects
[info] In file:/Users/kubukoz/IdeaProjects/fs2/
[info] 	   benchmark
[info] 	   coreJS
[info] 	   coreJVM
[info] 	   docs
[info] 	   experimental
[info] 	   io
[info] 	   microsite
[info] 	   reactiveStreams
[info] 	 * root
```

Before submitting a change for review, it's worth running some extra checks that will be triggered in Continuous Integration:

```sbt
sbt:root> fmtCheck; test; doc; mimaReportBinaryIssues; docs/mdoc; microsite/makeMicrosite
```

That will check the formatting, run all tests on the supported platforms, report any binary compatibility issues (as detected by [MiMa][mima]) and build the site.

If you run into any problems with tests, binary compatibility or other issues, feel free to ask questions on [Discord][fs2-dev].

### Website

To see how to build the microsite, check [here][fs2-build-site].

[fs2-issues]: https://github.com/functional-streams-for-scala/fs2/issues
[fs2-pulls]: https://github.com/functional-streams-for-scala/fs2/pulls
[fs2-dev]: https://discord.gg/72TuGwdRbW
[fs2-build-site]: https://github.com/functional-streams-for-scala/fs2/blob/main/build-site.md
[coc]: https://github.com/functional-streams-for-scala/fs2/blob/main/CODE_OF_CONDUCT.md
[sbt]: https://www.scala-sbt.org
[mima]: https://github.com/lightbend/mima
[scalafmt]: https://scalameta.org/scalafmt
[sbt-microsites]: https://47deg.github.io/sbt-microsites
[low-hanging-fruit]: https://github.com/functional-streams-for-scala/fs2/issues?q=is%3Aissue+is%3Aopen+sort%3Aupdated-desc+label%3A%22help+wanted%22
[node]: https://nodejs.org/en/
[jekyll]: https://jekyllrb.com/
