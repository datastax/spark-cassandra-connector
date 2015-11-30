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
import sbtsparkpackage.SparkPackagePlugin.autoImport._

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
    contains = Seq(embedded, connector, demos, jconnector)
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
    conf = defaultSettings ++ Seq(libraryDependencies ++= Dependencies.embedded)
  ).disablePlugins(AssemblyPlugin, SparkPackagePlugin) configs IntegrationTest

  lazy val connector = CrossScalaVersionsProject(
    name = namespace,
    conf = assembledSettings ++ Seq(libraryDependencies ++= Dependencies.connector ++ Seq(
        "org.scala-lang" % "scala-reflect"  % scalaVersion.value,
        "org.scala-lang" % "scala-compiler" % scalaVersion.value % "test,it")) ++ pureCassandraSettings
    ).copy(dependencies = Seq(embedded % "test->test;it->it,test;")
  ) configs IntegrationTest

  lazy val jconnector = Project(
    id = s"$namespace-java",
    base = file(s"$namespace-java"),
    settings = japiSettings ++ connector.settings :+ (spName := s"datastax/$namespace-java"),
    dependencies = Seq(connector % "compile;runtime->runtime;test->test;it->it,test;provided->provided")
  ) configs IntegrationTest

  lazy val demos = RootProject(
    name = "demos",
    dir = demosPath,
    contains = Seq(simpleDemos/*, kafkaStreaming*/, twitterStreaming)
  ).disablePlugins(AssemblyPlugin, SparkPackagePlugin)

  lazy val simpleDemos = Project(
    id = "simple-demos",
    base = demosPath / "simple-demos",
    settings = japiSettings ++ demoSettings,
    dependencies = Seq(connector, jconnector, embedded)
  ).disablePlugins(AssemblyPlugin, SparkPackagePlugin)
/*
  lazy val kafkaStreaming = CrossScalaVersionsProject(
    name = "kafka-streaming",
    conf = demoSettings ++ kafkaDemoSettings ++ Seq(
      libraryDependencies ++= (CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2, minor)) if minor < 11 => Dependencies.kafka
        case _ => Seq.empty
   }))).copy(base = demosPath / "kafka-streaming", dependencies = Seq(connector, embedded))
*/
  lazy val twitterStreaming = Project(
    id = "twitter-streaming",
    base = demosPath / "twitter-streaming",
    settings = demoSettings ++ Seq(libraryDependencies ++= Dependencies.twitter),
    dependencies = Seq(connector)
  ).disablePlugins(AssemblyPlugin, SparkPackagePlugin)

  lazy val refDoc = Project(
    id = s"$namespace-doc",
    base = file(s"$namespace-doc"),
    settings = defaultSettings ++ Seq(libraryDependencies ++= Dependencies.spark)
  ) dependsOn(connector)

  def crossBuildPath(base: sbt.File, v: String): sbt.File = base / s"scala-$v" / "src"

  /* templates */
  def CrossScalaVersionsProject(name: String,
                                conf: Seq[Def.Setting[_]],
                                reliesOn: Seq[ClasspathDep[ProjectReference]] = Seq.empty) =
    Project(id = name, base = file(name), dependencies = reliesOn, settings = conf ++ Seq(
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
    def guavaExclude: ModuleID =
      module exclude("com.google.guava", "guava")

    def sparkExclusions: ModuleID = module.guavaExclude
      .exclude("org.apache.spark", s"spark-core_$scalaBinary")

    def logbackExclude: ModuleID = module
      .exclude("ch.qos.logback", "logback-classic")
      .exclude("ch.qos.logback", "logback-core")

    def replExclusions: ModuleID = module.guavaExclude
      .exclude("org.apache.spark", s"spark-bagel_$scalaBinary")
      .exclude("org.apache.spark", s"spark-mllib_$scalaBinary")
      .exclude("org.scala-lang", "scala-compiler")

    def kafkaExclusions: ModuleID = module
      .exclude("org.slf4j", "slf4j-simple")
      .exclude("com.sun.jmx", "jmxri")
      .exclude("com.sun.jdmk", "jmxtools")
      .exclude("net.sf.jopt-simple", "jopt-simple")
  }

  val akkaActor           = "com.typesafe.akka"       %% "akka-actor"            % Akka           % "provided"  // ApacheV2
  val akkaRemote          = "com.typesafe.akka"       %% "akka-remote"           % Akka           % "provided"  // ApacheV2
  val akkaSlf4j           = "com.typesafe.akka"       %% "akka-slf4j"            % Akka           % "provided"  // ApacheV2
  val cassandraClient     = "org.apache.cassandra"    % "cassandra-clientutil"   % Cassandra       guavaExclude // ApacheV2
  val cassandraDriver     = "com.datastax.cassandra"  % "cassandra-driver-core"  % CassandraDriver guavaExclude // ApacheV2
  val commonsLang3        = "org.apache.commons"      % "commons-lang3"          % CommonsLang3                 // ApacheV2
  val config              = "com.typesafe"            % "config"                 % Config         % "provided"  // ApacheV2
  val guava               = "com.google.guava"        % "guava"                  % Guava
  val jodaC               = "org.joda"                % "joda-convert"           % JodaC
  val jodaT               = "joda-time"               % "joda-time"              % JodaT
  val lzf                 = "com.ning"                % "compress-lzf"           % Lzf            % "provided"
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

  val cassandraServer     = "org.apache.cassandra"    % "cassandra-all"          % Cassandra      logbackExclude    // ApacheV2

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
    sparkRepl,
    Test.akkaTestKit,
    Test.commonsIO,
    Test.junit,
    Test.junitInterface,
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

  val cassandra = Seq(cassandraClient, cassandraDriver)

  val spark = Seq(sparkCore, sparkStreaming, sparkSql, sparkCatalyst, sparkHive, sparkUnsafe)

  val connector = testKit ++ metrics ++ jetty ++ logging ++ akka ++ cassandra ++ spark.map(_ % "provided") ++ Seq(
    commonsLang3, config, guava, jodaC, jodaT, lzf, jsr166e)

  val embedded = logging ++ spark ++ cassandra ++ Seq(
    cassandraServer % "it,test", Embedded.jopt, Embedded.sparkRepl, Embedded.kafka, Embedded.snappy, guava)

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
