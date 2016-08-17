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

import pl.project13.scala.sbt.JmhPlugin
import sbt.Keys._
import sbt._
import sbt.Keys._
import sbtassembly._
import sbtassembly.AssemblyKeys._
import sbtsparkpackage.SparkPackagePlugin.autoImport._
import pl.project13.scala.sbt.JmhPlugin

object CassandraSparkBuild extends Build {
  import Settings._
  import sbtassembly.AssemblyPlugin
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
      assembly in spPackage := (assembly in shadedConnector).value,
      //Use the assembly which contains all of the libs not just the shaded ones
      assembly := (assembly in fullConnector).value,
      //Use the shaded jar as our packageTarget
      packageBin := {
        val shaded = (assembly in shadedConnector).value
        val targetName = target.value
        val expected = target.value / s"$namespace-${version.value}.jar"
        IO.move(shaded, expected)
        expected
      },
      //Update the distribution tasks to use the shaded jar
      sbt.Keys.`package` := packageBin.value)
      ++ pureCassandraSettings
      ++ {for (taskKey <- Seq(publishLocal in Compile, publish in Compile, publishM2 in Compile)) yield {
        packagedArtifacts in taskKey := {
          val previous = (packagedArtifacts in Compile).value
          val shadedJar = (artifact.value.copy(configurations = Seq(Compile)) -> packageBin.value)
          previous + shadedJar
        }
    }}
    ).copy(dependencies = Seq(embedded % "test->test;it->it,test;")
  ) configs IntegrationTest

  /** Because the distribution project has to mark the shaded jars as provided to remove them from
    * the distribution dependencies we provide this additional project to build a fat jar which
    * includes everything. The artifact produced by this project is unshaded while the assembly
    * remains shaded.
    */
  lazy val fullConnector = CrossScalaVersionsProject(
    name = s"$namespace-full",
    conf = assembledSettings ++ Seq(
      libraryDependencies ++= Dependencies.connector
        ++ Dependencies.shaded
        ++ Seq(
        "org.scala-lang" % "scala-reflect"  % scalaVersion.value,
        "org.scala-lang" % "scala-compiler" % scalaVersion.value % "test,it"),
      target := target.value / "unshaded"
        )
      ++ pureCassandraSettings,
    base = Some(namespace)
    ).copy(dependencies = Seq(embedded % "test->test;it->it,test;"))


  lazy val shadedConnector = CrossScalaVersionsProject(
    name = s"$namespace-shaded",
    conf = assembledSettings ++ Seq(
      libraryDependencies ++= Dependencies.connectorNonShaded
        ++ Dependencies.shaded
        ++ Seq(
        "org.scala-lang" % "scala-reflect"  % scalaVersion.value,
        "org.scala-lang" % "scala-compiler" % scalaVersion.value % "test,it"))
      ++ pureCassandraSettings
      ++ Seq(
        target := target.value / "shaded",
        test in assembly := {},
        publishArtifact in (Compile, packageBin) := false),
      base = Some(namespace)
    ).copy(dependencies = Seq(embedded % "test->test;it->it,test;")
  ) configs IntegrationTest

  lazy val demos = RootProject(
    name = "demos",
    dir = demosPath,
    contains = Seq(simpleDemos, kafkaStreaming)
  ).disablePlugins(AssemblyPlugin, SparkPackagePlugin)

  lazy val simpleDemos = Project(
    id = "simple-demos",
    base = demosPath / "simple-demos",
    settings = demoSettings,
    dependencies = Seq(connectorDistribution, embedded)
  ).disablePlugins(AssemblyPlugin, SparkPackagePlugin)

  lazy val kafkaStreaming = Project(
    id = "kafka-streaming",
    base = demosPath / "kafka-streaming",
    settings = demoSettings ++ Seq(libraryDependencies ++= Seq(Artifacts.Demos.kafka, Artifacts.Demos.kafkaStreaming)),
    dependencies = Seq(connectorDistribution, embedded))
      .disablePlugins(AssemblyPlugin, SparkPackagePlugin)

  lazy val refDoc = Project(
    id = s"$namespace-doc",
    base = file(s"$namespace-doc"),
    settings = defaultSettings ++ Seq(libraryDependencies ++= Dependencies.spark)
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
      .exclude("io.netty", "netty-all")

    def glassfishExclude(): ModuleID = {
      module
          .exclude("org.glassfish.jersey.containers", "jersey-container-servlet-core")
          .exclude("org.glassfish.jersey.containers", "jersey-container-servlet")
          .exclude("org.glassfish.jersey.core", "jersey-client")
          .exclude("org.glassfish.jersey.core", "jersey-common")
          .exclude("org.glassfish.jersey.core", "jersey-server")
    }

    def sparkCoreExclusions(): ModuleID = module.guavaExclude().nettyExclude().glassfishExclude()
        .exclude("commons-collections", "commons-collections")
        .exclude("commons-beanutils", "commons-beanutils-core")
        .exclude("commons-logging", "commons-logging")
        .exclude("org.apache.curator", "curator-recipes")

    def sparkExclusions(): ModuleID = module.sparkCoreExclusions()
    
    def logbackExclude(): ModuleID = module
      .exclude("ch.qos.logback", "logback-classic")
      .exclude("ch.qos.logback", "logback-core")

    def replExclusions(): ModuleID = module.nettyExclude
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
  val cassandraDriver     = "com.datastax.cassandra"  % "cassandra-driver-core"  % CassandraDriver nettyExclude() guavaExclude() // ApacheV2
  val cassandraClient     = "org.apache.cassandra"    % "cassandra-clientutil"   % Settings.cassandraTestVersion  nettyExclude() guavaExclude() // ApacheV2
  val commonsBeanUtils    = "commons-beanutils"       % "commons-beanutils"      % CommonsBeanUtils exclude("commons-logging", "commons-logging") // ApacheV2
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
  val sparkCore           = "org.apache.spark"        %% "spark-core"            % Spark sparkCoreExclusions() // ApacheV2
  val sparkRepl           = "org.apache.spark"        %% "spark-repl"            % Spark sparkExclusions()     // ApacheV2
  val sparkUnsafe         = "org.apache.spark"        %% "spark-unsafe"          % Spark sparkExclusions()     // ApacheV2
  val sparkStreaming      = "org.apache.spark"        %% "spark-streaming"       % Spark sparkExclusions()     // ApacheV2
  val sparkSql            = "org.apache.spark"        %% "spark-sql"             % Spark sparkExclusions()        // ApacheV2
  val sparkCatalyst       = "org.apache.spark"        %% "spark-catalyst"        % Spark sparkExclusions()        // ApacheV2
  val sparkHive           = "org.apache.spark"        %% "spark-hive"            % Spark sparkExclusions()        // ApacheV2

  val cassandraServer     = "org.apache.cassandra"    % "cassandra-all"          % Settings.cassandraTestVersion      logbackExclude()  exclude(org = "org.slf4j", name = "log4j-over-slf4j")  // ApacheV2

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
    val kafka             = "org.apache.kafka"        %% "kafka"                      % Kafka                 kafkaExclusions   // ApacheV2
    val kafkaStreaming    = "org.apache.spark"        %% "spark-streaming-kafka-0-8"  % Spark   % "provided"  sparkExclusions   // ApacheV2
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

  import Artifacts._
  import BuildUtil._

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

  val cassandra = Seq(cassandraClient, cassandraDriver)

  val spark = Seq(sparkCore, sparkStreaming, sparkSql, sparkCatalyst, sparkHive, sparkUnsafe)

 /**
    * We will mark these dependencies as provided for normal compilation so that they will be on the
    * classpath but they will not be downloaded as depenendencies when grabbed from maven.
    *
    * In the shaded build we will place theses in the assembly jar and remove all other libs
    */
  val shaded = Seq(guava, cassandraDriver)

  val connector = testKit ++
    metrics ++
    jetty ++
    logging ++
    akka ++
    cassandra ++
    spark.map(_ % "provided") ++
    shaded ++
    Seq(commonsBeanUtils, netty, config, jodaC, jodaT, lzf, jsr166e)
      .map(_ exclude(org ="org.sl4j", name = "log4j-over-slf4j"))

  val connectorDistribution = (connector.toSet -- shaded.toSet).toSeq ++ shaded.map(_ % "provided")

  val connectorNonShaded = (connector.toSet -- shaded.toSet).toSeq.map { dep =>
    dep.configurations match {
      case Some(conf) => dep
      case _ => dep % "provided"
    }
  }

  val embedded = logging ++ spark ++ cassandra ++ Seq(
    cassandraServer % "it,test", Embedded.jopt, Embedded.sparkRepl, Embedded.kafka, Embedded.snappy, Embedded.akkaCluster, guava, netty)

  val perf = logging ++ spark ++ cassandra

  val kafka = Seq(Demos.kafka, Demos.kafkaStreaming)

  val documentationMappings = Seq(
    DocumentationMapping(url(s"http://spark.apache.org/docs/${Versions.Spark}/api/scala/"),
      sparkCore, sparkStreaming, sparkSql, sparkCatalyst, sparkHive
    ),
    DocumentationMapping(url(s"http://doc.akka.io/api/akka/${Versions.Akka}/"),
      akkaActor, akkaRemote, akkaSlf4j
    )
  )

}
