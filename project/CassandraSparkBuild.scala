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

import sbt._
import sbt.Keys._

object CassandraSparkBuild extends Build {
  import Settings._

  val namespace = "spark-cassandra-connector"

  val demosPath = file(s"$namespace-demos")

  lazy val root = RootProject("root", file("."), Seq(connector, jconnector, embedded, demos))

  lazy val connector = Project(
    id = namespace,
    base = file(namespace),
    settings = assembledSettings ++ Seq(libraryDependencies ++= Dependencies.connector),
    dependencies = Seq(embedded % "test->test;it->it,test;")
  ) configs (IntegrationTest, ClusterIntegrationTest)

  lazy val jconnector = Project(
    id = s"$namespace-java",
    base = file(s"$namespace-java"),
    settings = connector.settings,
    dependencies = Seq(connector % "compile;runtime->runtime;test->test;it->it,test;provided->provided")
  ) configs (IntegrationTest, ClusterIntegrationTest)

  lazy val embedded = Project(
    id = s"$namespace-embedded",
    base = file(s"$namespace-embedded"),
    settings = defaultSettings ++ Seq(libraryDependencies ++= Dependencies.embedded)
  ) configs (IntegrationTest, ClusterIntegrationTest)

  lazy val demos = RootProject("demos", demosPath, Seq(simpleDemos, kafkaStreaming, twitterStreaming))

  lazy val simpleDemos = Project(
    id = "simple-demos",
    base = demosPath / "simple-demos",
    settings = demoSettings,
    dependencies = Seq(connector, jconnector, embedded))

  lazy val kafkaStreaming = Project(
    id = "kafka-streaming",
    base = demosPath / "kafka-streaming",
    settings = demoSettings ++ sbtAssemblySettings ++ Seq(libraryDependencies ++= Dependencies.kafka),
    dependencies = Seq(connector, embedded))

  lazy val twitterStreaming = Project(
    id = "twitter-streaming",
    base = demosPath / "twitter-streaming",
    settings = demoSettings ++ sbtAssemblySettings ++ Seq(libraryDependencies ++= Dependencies.twitter),
    dependencies = Seq(connector))

  def RootProject(name: String, dir: sbt.File, contains: Seq[ProjectReference]): Project =
    Project(id = name, base = dir, settings = parentSettings, aggregate = contains)

}

object Dependencies {
  import Versions._

  object Compile {

    val akkaActor           = "com.typesafe.akka"       %% "akka-actor"            % Akka           % "provided"                 // ApacheV2
    val akkaRemote          = "com.typesafe.akka"       %% "akka-remote"           % Akka           % "provided"                 // ApacheV2
    val akkaSlf4j           = "com.typesafe.akka"       %% "akka-slf4j"            % Akka           % "provided"                 // ApacheV2
    val cassandraThrift     = "org.apache.cassandra"    % "cassandra-thrift"       % Cassandra        exclude("com.google.guava", "guava") // ApacheV2
    val cassandraClient     = "org.apache.cassandra"    % "cassandra-clientutil"   % Cassandra        exclude("com.google.guava", "guava") // ApacheV2
    val cassandraDriver     = "com.datastax.cassandra"  % "cassandra-driver-core"  % CassandraDriver  exclude("com.google.guava", "guava") // ApacheV2
    val commonsLang3        = "org.apache.commons"      % "commons-lang3"          % CommonsLang3                                // ApacheV2
    val config              = "com.typesafe"            % "config"                 % Config         % "provided"                 // ApacheV2
    val guava               = "com.google.guava"        % "guava"                  % Guava
    val jodaC               = "org.joda"                % "joda-convert"           % JodaC
    val jodaT               = "joda-time"               % "joda-time"              % JodaT
    val lzf                 = "com.ning"                % "compress-lzf"           % Lzf            % "provided"
    val reflect             = "org.scala-lang"          % "scala-reflect"          % Scala
    val slf4jApi            = "org.slf4j"               % "slf4j-api"              % Slf4j          % "provided"                 // MIT
    /* To allow spark artifact inclusion in the demo module at runtime, we set 'provided'
       scope on the connector below, specifically, versus globally here. */
    val sparkCore           = "org.apache.spark"        %% "spark-core"            % Spark exclude("com.google.guava", "guava") // ApacheV2
    val sparkStreaming      = "org.apache.spark"        %% "spark-streaming"       % Spark exclude("com.google.guava", "guava") // ApacheV2
    val sparkSql            = "org.apache.spark"        %% "spark-sql"             % Spark exclude("com.google.guava", "guava") exclude("org.apache.spark", "spark-core")
    val sparkCatalyst       = "org.apache.spark"        %% "spark-catalyst"        % Spark exclude("com.google.guava", "guava") exclude("org.apache.spark", "spark-core")
    val sparkHive           = "org.apache.spark"        %% "spark-hive"            % Spark exclude("com.google.guava", "guava") exclude("org.apache.spark", "spark-core")

    object Metrics {
      val metricsCore       = "com.codahale.metrics"    % "metrics-core"           % CodaHaleMetrics
      val metricsJson       = "com.codahale.metrics"    % "metrics-json"           % CodaHaleMetrics % "provided"
    }

    object Embedded {
      val akkaCluster       = "com.typesafe.akka"       %% "akka-cluster"          % Akka                                      // ApacheV2
      val kafka             = "org.apache.kafka"        %% "kafka"                 % Kafka exclude("org.slf4j", "slf4j-simple") // ApacheV2
      val cassandraServer   = "org.apache.cassandra"    % "cassandra-all"          % Cassandra exclude("ch.qos.logback", "logback-classic") exclude("ch.qos.logback", "logback-core") // ApacheV2
      val jopt              = "net.sf.jopt-simple"      % "jopt-simple"            % JOpt // For kafka command line work
      val sparkRepl         = "org.apache.spark"        %% "spark-repl"            % SparkRepl exclude("com.google.guava", "guava") exclude("org.apache.spark", "spark-core_2.10") exclude("org.apache.spark", "spark-bagel_2.10") exclude("org.apache.spark", "spark-mllib_2.10") exclude("org.scala-lang", "scala-compiler") // ApacheV2
    }

    object Demos {
      val kafkaStreaming    = "org.apache.spark"        %% "spark-streaming-kafka" % Spark exclude("com.google.guava", "guava") exclude("org.apache.spark", "spark-core") // ApacheV2
      val twitterStreaming  = "org.apache.spark"        %% "spark-streaming-twitter" % Spark exclude("com.google.guava", "guava") exclude("org.apache.spark", "spark-core") // ApacheV2
    }

    object Test {
      val akkaTestKit       = "com.typesafe.akka"       %% "akka-testkit"         % Akka            % "test,it"                   // ApacheV2
      val commonsIO         = "commons-io"              % "commons-io"            % CommonsIO       % "test,it"                   // ApacheV2
      // Eventually migrate junit out in favor of the scala test APIs
      val junit             = "junit"                   % "junit"                 % "4.11"          % "test,it"                   // for now
      val junitInterface    = "com.novocode"            % "junit-interface"       % "0.10"          % "test,it"
      val scalatest         = "org.scalatest"           %% "scalatest"            % ScalaTest       % "test,it"                   // ApacheV2
      val scalactic         = "org.scalactic"           %% "scalactic"            % Scalactic       % "test,it"                   // ApacheV2
      val scalaCompiler     = "org.scala-lang"          % "scala-compiler"        % Scala
      val mockito           = "org.mockito"             % "mockito-all"           % "1.10.19"       % "test,it"                   // MIT
    }
  }

  import Compile._

  val logging = Seq(slf4jApi)

  val metrics = Seq(Metrics.metricsCore, Metrics.metricsJson)

  val testKit = Seq(Test.akkaTestKit, Test.commonsIO, Test.junit,
    Test.junitInterface, Test.scalatest, Test.scalaCompiler, Test.scalactic, Test.mockito)

  val akka = Seq(akkaActor, akkaRemote, akkaSlf4j)

  val cassandra = Seq(cassandraThrift, cassandraClient, cassandraDriver)

  val spark = Seq(sparkCore, sparkStreaming, sparkSql, sparkCatalyst, sparkHive)

  val connector = testKit ++ metrics ++ logging ++ akka ++ cassandra ++ spark.map(_ % "provided") ++ Seq(
    commonsLang3, config, guava, jodaC, jodaT, lzf, reflect)

  val embedded = logging ++ spark ++ cassandra ++ Seq(
    Embedded.cassandraServer, Embedded.jopt, Embedded.kafka, Embedded.sparkRepl)

  val kafka = Seq(Demos.kafkaStreaming)

  val twitter = Seq(sparkStreaming, Demos.twitterStreaming)
}
