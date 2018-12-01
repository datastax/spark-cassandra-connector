// pulls in: sbt-pgp, sbt-release, sbt-mima-plugin, sbt-dependency-graph, sbt-buildinfo, sbt-sonatype
// TODO use sbt-release plugin
resolvers += "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/"

resolvers += Classpaths.sbtPluginReleases

resolvers += DefaultMavenRepository

resolvers += Resolver.sonatypeRepo("public")

resolvers += Resolver.typesafeRepo("releases")

resolvers += "Spark Packages Main repo" at "https://dl.bintray.com/spark-packages/maven"

addSbtPlugin("org.typelevel" % "sbt-typelevel" % "0.3.1")

addSbtPlugin("com.typesafe.sbt" % "sbt-git" % "0.8.5")

addSbtPlugin("com.scalapenos" % "sbt-prompt" % "1.0.0")

addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "0.8.0")

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.0.4")

//SbtAssembly 0.12.0 is included in sbt-spark-package
resolvers += "Spark Packages Main repo" at "https://dl.bintray.com/spark-packages/maven" 
addSbtPlugin("org.spark-packages" %% "sbt-spark-package" % "0.2.5")

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.8.2")

addSbtPlugin("io.get-coursier" % "sbt-coursier" % "1.0.0")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.3")
