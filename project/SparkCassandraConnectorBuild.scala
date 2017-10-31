/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

import java.io.File

import sbt._
import sbt.Keys._
import sbtassembly._
import sbtassembly.AssemblyKeys._
import sbtsparkpackage.SparkPackagePlugin.autoImport._
import pl.project13.scala.sbt.JmhPlugin

object CassandraSparkBuild extends Build {
  import Settings._
  import sbtassembly.AssemblyPlugin
  import Versions.scalaBinary
  import sbtsparkpackage.SparkPackagePlugin

  val namespace = "spark-cassandra-connector"

  val demosPath = file(s"$namespace-demos")

  lazy val root = RootProject(
    name = "root",
    dir = file("."),
    settings = rootSettings ++ Seq(cassandraServerClasspath := { "" }),
    contains = Seq(embedded, connectorDistribution, demos)
  ).disablePlugins(AssemblyPlugin, SparkPackagePlugin)

  lazy val cassandraServerProject = Project(
    id = "cassandra-server",
    base = file("cassandra-server"),
    settings = defaultSettings ++ Seq(
      libraryDependencies ++= Seq(Artifacts.cassandraServer % "it", Artifacts.airlift),
      cassandraServerClasspath := {
        (fullClasspath in IntegrationTest).value.map(_.data.getAbsoluteFile).mkString(File.pathSeparator)
      }
    )
  ) configs IntegrationTest

  lazy val embedded = CrossScalaVersionsProject(
    name = s"$namespace-embedded",
    conf = defaultSettings ++ Seq(
      libraryDependencies
      ++= Dependencies.embedded ++ Seq(
        "org.scala-lang" % "scala-reflect"  % scalaVersion.value,
        "org.scala-lang" % "scala-compiler" % scalaVersion.value))
  ).disablePlugins(AssemblyPlugin, SparkPackagePlugin) configs IntegrationTest

  /**
    * Do not included shaded dependencies so they will not be listed in the Pom created for this
    * project.
    *
    * Run the compile from the shaded project since we are no longer including shaded libs
    */
  lazy val connectorDistribution = CrossScalaVersionsProject(
    name = namespace,
    conf = assembledSettings ++ Seq(
      libraryDependencies ++= Dependencies.connectorDistribution ++ Seq(
        "org.scala-lang" % "scala-reflect"  % scalaVersion.value,
        "org.scala-lang" % "scala-compiler" % scalaVersion.value % "test,it"),
      //Use the assembly which contains all of the libs not just the shaded ones
      assembly in spPackage := (assembly in shadedConnector).value,
      assembly := (assembly in fullConnector).value,
      //Use the shaded jar as our packageTarget
      packageBin := {
        val shaded = (assembly in shadedConnector).value
        val targetName = target.value
        val expected = target.value / s"$namespace-${version.value}.jar"
        IO.move(shaded, expected)
        val log = streams.value.log
        log.info(s"""Shaded jar moved to $expected""".stripMargin)
        expected
      },
      packagedArtifact in packageBin in Compile := {
        (artifact.value, (assembly in shadedConnector).value)
      },
      sbt.Keys.`package` := packageBin.value)
      ++ pureCassandraSettings
  ).copy(dependencies = Seq(embedded % "test->test;it->it,test;")
  ) configs IntegrationTest

  /** Because the distribution project has to mark the shaded jars as provided to remove them from
    * the distribution dependencies we provide this additional project to build a fat jar which
    * includes everything. The artifact produced by this project is unshaded while the assembly
    * remains shaded.
    */
  lazy val fullConnector = CrossScalaVersionsProject(
    name = s"$namespace-unshaded",
    conf = assembledSettings ++ Seq(
      libraryDependencies ++= Dependencies.connectorAll
        ++ Dependencies.includedInShadedJar
        ++ Seq(
        "org.scala-lang" % "scala-reflect"  % scalaVersion.value,
        "org.scala-lang" % "scala-compiler" % scalaVersion.value % "test,it"),
      target := target.value / "full"
    )
      ++ pureCassandraSettings,
    base = Some(namespace)
  ).copy(dependencies = Seq(embedded % "test->test;it->it,test")) configs IntegrationTest


  lazy val shadedConnector = CrossScalaVersionsProject(
    name = s"$namespace-shaded",
    conf = assembledSettings ++ Seq(
      libraryDependencies ++= Dependencies.connectorNonShaded
        ++ Dependencies.includedInShadedJar
        ++ Seq(
        "org.scala-lang" % "scala-reflect"  % scalaVersion.value,
        "org.scala-lang" % "scala-compiler" % scalaVersion.value % "test,it"),
      target := target.value / "shaded",
      test in assembly := {},
      publishArtifact in (Compile, packageBin) := false)
      ++ pureCassandraSettings,
    base = Some(namespace)
  ).copy(dependencies = Seq(embedded % "test->test;it->it,test;")
  ) configs IntegrationTest


  lazy val demos = RootProject(
    name = "demos",
    dir = demosPath,
    contains = Seq(simpleDemos/*, kafkaStreaming*/, twitterStreaming)
  ).disablePlugins(AssemblyPlugin, SparkPackagePlugin)

  lazy val simpleDemos = Project(
    id = "simple-demos",
    base = demosPath / "simple-demos",
    settings = demoSettings,
    dependencies = Seq(connectorDistribution, embedded)
  ).disablePlugins(AssemblyPlugin, SparkPackagePlugin)
/*
  lazy val kafkaStreaming = CrossScalaVersionsProject(
    name = "kafka-streaming",
    conf = demoSettings ++ kafkaDemoSettings ++ Seq(
      libraryDependencies ++= (CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2, minor)) if minor < 11 => Dependencies.kafka
        case _ => Seq.empty
   }))).copy(base = demosPath / "kafka-streaming", dependencies = Seq(connectorAll, embedded))
*/
  lazy val twitterStreaming = Project(
    id = "twitter-streaming",
    base = demosPath / "twitter-streaming",
    settings = demoSettings ++ Seq(libraryDependencies ++= Dependencies.twitter),
    dependencies = Seq(connectorDistribution, embedded)
  ).disablePlugins(AssemblyPlugin, SparkPackagePlugin)

  lazy val refDoc = Project(
    id = s"$namespace-doc",
    base = file(s"$namespace-doc"),
    settings = defaultSettings ++ Seq(libraryDependencies ++= (Dependencies.spark ++ Dependencies.cassandra))
  ) dependsOn connectorDistribution

  lazy val perf = Project(
    id = s"$namespace-perf",
    base = file(s"$namespace-perf"),
    settings = projectSettings,
    dependencies = Seq(connectorDistribution, embedded)
  ) enablePlugins(JmhPlugin)

  def crossBuildPath(base: sbt.File, v: String): sbt.File = base / s"scala-$v" / "src"

  /* templates */
  def CrossScalaVersionsProject(name: String,
                                conf: Seq[Def.Setting[_]],
                                reliesOn: Seq[ClasspathDep[ProjectReference]] = Seq.empty,
                                base: Option[String] = None) =
    Project(id = name, base = file(base.getOrElse(name)), dependencies = reliesOn, settings = conf ++ Seq(
      unmanagedSourceDirectories in (Compile, packageBin) +=
        crossBuildPath(baseDirectory.value, scalaBinaryVersion.value),
      unmanagedSourceDirectories in (Compile, doc) +=
        crossBuildPath(baseDirectory.value, scalaBinaryVersion.value),
      unmanagedSourceDirectories in Compile +=
        crossBuildPath(baseDirectory.value, scalaBinaryVersion.value)
    ))

  def RootProject(
    name: String,
    dir: sbt.File, settings: =>
    scala.Seq[sbt.Def.Setting[_]] = Seq.empty,
    contains: Seq[ProjectReference]): Project =
      Project(
        id = name,
        base = dir,
        settings = parentSettings ++ settings,
        aggregate = contains)
}

object Artifacts {
  import Versions._

  implicit class Exclude(module: ModuleID) {
    def guavaExclude(): ModuleID =
      module exclude("com.google.guava", "guava")

    /**We will just include netty-all as a dependency **/
    def nettyExclude(): ModuleID = module
      .exclude("io.netty", "netty")
      .exclude("io.netty", "netty-buffer")
      .exclude("io.netty", "netty-codec")
      .exclude("io.netty", "netty-common")
      .exclude("io.netty", "netty-handler")
      .exclude("io.netty", "netty-transport")

    def driverExclusions(): ModuleID =
      guavaExclude().nettyExclude()
        .exclude("io.dropwizard.metrics", "metrics-core")
        .exclude("org.slf4j", "slf4j-api")


    def sparkExclusions(): ModuleID = module.nettyExclude
      .exclude("org.apache.spark", s"spark-core_$scalaBinary")

    def logbackExclude(): ModuleID = module
      .exclude("ch.qos.logback", "logback-classic")
      .exclude("ch.qos.logback", "logback-core")

    def replExclusions(): ModuleID = nettyExclude().guavaExclude()
      .exclude("org.apache.spark", s"spark-bagel_$scalaBinary")
      .exclude("org.apache.spark", s"spark-mllib_$scalaBinary")
      .exclude("org.scala-lang", "scala-compiler")

    def kafkaExclusions(): ModuleID = module
      .exclude("org.slf4j", "slf4j-simple")
      .exclude("com.sun.jmx", "jmxri")
      .exclude("com.sun.jdmk", "jmxtools")
      .exclude("net.sf.jopt-simple", "jopt-simple")
  }

  val akkaActor           = "com.typesafe.akka"       %% "akka-actor"            % Akka           % "provided"  // ApacheV2
  val akkaRemote          = "com.typesafe.akka"       %% "akka-remote"           % Akka           % "provided"  // ApacheV2
  val akkaSlf4j           = "com.typesafe.akka"       %% "akka-slf4j"            % Akka           % "provided"  // ApacheV2
  val cassandraDriver     = "com.datastax.cassandra"  % "cassandra-driver-core"  % CassandraDriver driverExclusions() // ApacheV2
  val config              = "com.typesafe"            % "config"                 % Config         % "provided"  // ApacheV2
  val guava               = "com.google.guava"        % "guava"                  % Guava
  val jodaC               = "org.joda"                % "joda-convert"           % JodaC
  val jodaT               = "joda-time"               % "joda-time"              % JodaT
  val lzf                 = "com.ning"                % "compress-lzf"           % Lzf            % "provided"
  val netty               = "io.netty"                % "netty-all"              % Netty
  val slf4jApi            = "org.slf4j"               % "slf4j-api"              % Slf4j          % "provided"  // MIT
  val jsr166e             = "com.twitter"             % "jsr166e"                % JSR166e                      // Creative Commons
  val airlift             = "io.airlift"              % "airline"                % Airlift

  /* To allow spark artifact inclusion in the demos at runtime, we set 'provided' below. */
  val sparkCore           = "org.apache.spark"        %% "spark-core"            % Spark guavaExclude           // ApacheV2
  val sparkRepl           = "org.apache.spark"        %% "spark-repl"            % Spark guavaExclude           // ApacheV2
  val sparkUnsafe         = "org.apache.spark"        %% "spark-unsafe"          % Spark guavaExclude           // ApacheV2
  val sparkStreaming      = "org.apache.spark"        %% "spark-streaming"       % Spark guavaExclude           // ApacheV2
  val sparkSql            = "org.apache.spark"        %% "spark-sql"             % Spark sparkExclusions        // ApacheV2
  val sparkCatalyst       = "org.apache.spark"        %% "spark-catalyst"        % Spark sparkExclusions        // ApacheV2
  val sparkHive           = "org.apache.spark"        %% "spark-hive"            % Spark sparkExclusions        // ApacheV2

  val cassandraServer     = "org.apache.cassandra"    % "cassandra-all"          % Settings.cassandraTestVersion      logbackExclude    // ApacheV2

  object Metrics {
    val metricsCore       = "com.codahale.metrics"    % "metrics-core"           % CodaHaleMetrics % "provided"
    val metricsJson       = "com.codahale.metrics"    % "metrics-json"           % CodaHaleMetrics % "provided"
  }

  object Jetty {
    val jettyServer       = "org.eclipse.jetty"       % "jetty-server"            % SparkJetty % "provided"
    val jettyServlet      = "org.eclipse.jetty"       % "jetty-servlet"           % SparkJetty % "provided"
  }

  object Embedded {
    val akkaCluster       = "com.typesafe.akka"       %% "akka-cluster"           % Akka                                    // ApacheV2
    val jopt              = "net.sf.jopt-simple"      % "jopt-simple"             % JOpt
    val kafka             = "org.apache.kafka"        %% "kafka"                  % Kafka                 kafkaExclusions   // ApacheV2
    val sparkRepl         = "org.apache.spark"        %% "spark-repl"             % Spark % "provided"    replExclusions    // ApacheV2
    val snappy            = "org.xerial.snappy"       % "snappy-java"             % "1.1.1.7"
  }

  object Demos {
    val kafka             = "org.apache.kafka"        % "kafka_2.10"                  % Kafka                 kafkaExclusions   // ApacheV2
    val kafkaStreaming    = "org.apache.spark"        % "spark-streaming-kafka_2.10"  % Spark   % "provided"  sparkExclusions   // ApacheV2
    val twitterStreaming  = "org.apache.spark"        %% "spark-streaming-twitter"    % Spark   % "provided"  sparkExclusions   // ApacheV2
  }

  object Test {
    val akkaTestKit       = "com.typesafe.akka"       %% "akka-testkit"                 % Akka      % "test,it"       // ApacheV2
    val commonsIO         = "commons-io"              % "commons-io"                    % CommonsIO % "test,it"       // ApacheV2
    val scalaCheck        = "org.scalacheck"          %% "scalacheck"                   % ScalaCheck % "test,it"      // BSD
    val scalaMock         = "org.scalamock"           %% "scalamock-scalatest-support"  % ScalaMock % "test,it"       // BSD
    val scalaTest         = "org.scalatest"           %% "scalatest"                    % ScalaTest % "test,it"       // ApacheV2
    val scalactic         = "org.scalactic"           %% "scalactic"                    % Scalactic % "test,it"       // ApacheV2
    val sparkCoreT        = "org.apache.spark"        %% "spark-core"                   % Spark     % "test,it" classifier "tests"
    val sparkStreamingT   = "org.apache.spark"        %% "spark-streaming"              % Spark     % "test,it" classifier "tests"
    val mockito           = "org.mockito"             % "mockito-all"                   % "1.10.19" % "test,it"       // MIT
    val junit             = "junit"                   % "junit"                         % "4.11"    % "test,it"
    val junitInterface    = "com.novocode"            % "junit-interface"               % "0.10"    % "test,it"
    val powerMock         = "org.powermock"           % "powermock-module-junit4"       % "1.6.2"   % "test,it"       // ApacheV2
    val powerMockMockito  = "org.powermock"           % "powermock-api-mockito"         % "1.6.2"   % "test,it"       // ApacheV2
  }
}


object Dependencies {

  import BuildUtil._
  import Artifacts._

  val logging = Seq(slf4jApi)

  val metrics = Seq(Metrics.metricsCore, Metrics.metricsJson)

  val jetty = Seq(Jetty.jettyServer, Jetty.jettyServlet)

  val testKit = Seq(
    sparkRepl % "test,it",
    Test.akkaTestKit,
    Test.commonsIO,
    Test.junit,
    Test.junitInterface,
    Test.scalaCheck,
    Test.scalaMock,
    Test.scalaTest,
    Test.scalactic,
    Test.sparkCoreT,
    Test.sparkStreamingT,
    Test.mockito,
    Test.powerMock,
    Test.powerMockMockito
  )

  val akka = Seq(akkaActor, akkaRemote, akkaSlf4j)

  val cassandra = Seq(cassandraDriver)

  val spark = Seq(sparkCore, sparkStreaming, sparkSql, sparkCatalyst, sparkHive, sparkUnsafe)

  /**
    * Dependencies which will be shaded in our distribution artifact and not listed on the
    * distribution artifact's dependency list.
    */
  val includedInShadedJar = Seq(guava, cassandraDriver)

  /**
    * This is the full dependency list required to build an assembly with all dependencies
    * required to run the connector not listed as provided except for those which will
    * be on the classpath because of Spark.
    */
  val connectorAll = testKit ++
    metrics ++
    jetty ++
    logging ++
    akka ++
    cassandra ++
    spark.map(_ % "provided") ++
    Seq(config, jodaC, jodaT, lzf, netty, jsr166e) ++
    includedInShadedJar

  /**
    * Mark the shaded dependencies as Provided, this removes them from the artifacts to be downloaded
    * by build systems. This will avoid downloading a Cassandra Driver which does not have it's guava
    * references shaded.
    */
  val connectorDistribution = (connectorAll.toSet -- includedInShadedJar.toSet).toSeq ++
    includedInShadedJar.map(_ % "provided")

  /**
    * When building the shaded jar we want the assembly task to ONLY include the shaded libs, to
    * accomplish this we set all other dependencies as provided.
    */
  val connectorNonShaded = (connectorAll.toSet -- includedInShadedJar.toSet).toSeq.map { dep =>
    dep.configurations match {
      case Some(conf) => dep
      case _ => dep % "provided"
    }
  }

  val embedded = logging ++ spark ++ cassandra ++ Seq(
    cassandraServer % "it,test", Embedded.jopt, Embedded.sparkRepl, Embedded.kafka, Embedded.snappy, guava, netty)

  val perf = logging ++ spark ++ cassandra

  val kafka = Seq(Demos.kafka, Demos.kafkaStreaming)

  val twitter = Seq(sparkStreaming, Demos.twitterStreaming)

  val documentationMappings = Seq(
    DocumentationMapping(url(s"http://spark.apache.org/docs/${Versions.Spark}/api/scala/"),
      sparkCore, sparkStreaming, sparkSql, sparkCatalyst, sparkHive
    ),
    DocumentationMapping(url(s"http://doc.akka.io/api/akka/${Versions.Akka}/"),
      akkaActor, akkaRemote, akkaSlf4j
    )
  )

}
