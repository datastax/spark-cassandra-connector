// pulls in: sbt-pgp, sbt-release, sbt-mima-plugin, sbt-dependency-graph, sbt-buildinfo, sbt-sonatype
// TODO use sbt-release plugin
addSbtPlugin("org.typelevel" % "sbt-typelevel" % "0.3.1")

addSbtPlugin("com.typesafe.sbt" % "sbt-git" % "0.6.4")

addSbtPlugin("com.scalapenos" % "sbt-prompt" % "0.2.1")

addSbtPlugin("com.typesafe.sbt" % "sbt-scalariform" % "1.3.0")

addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "0.6.0")

resolvers += "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/"

resolvers += Classpaths.sbtPluginReleases

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.0.4")

//SbtAssembly 0.12.0 is included in sbt-spark-package
resolvers += "Spark Packages Main repo" at "https://dl.bintray.com/spark-packages/maven" 

addSbtPlugin("org.spark-packages" %% "sbt-spark-package" % "0.2.5")

addSbtPlugin("pl.project13.scala" % "sbt-jmh" % "0.2.6")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.3")
