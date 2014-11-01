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
import sbtassembly.Plugin.AssemblyKeys

object CassandraSparkBuild extends Build {
  import Settings._

  lazy val root = Project(
    id = "root",
    base = file("."),
    settings = parentSettings,
    aggregate = Seq(connector, jconnector, embedded, demos)
  )

  lazy val connector = Project(
    id = "connector",
    base = file("spark-cassandra-connector"),
    settings = defaultSettings ++ Seq(libraryDependencies ++= Dependencies.connector) ++ sbtAssemblySettings,
    dependencies = Seq(embedded % "test->test;it->it,test;")
  ) configs IntegrationTest

  lazy val jconnector = Project(
    id = "jconnector",
    base = file("spark-cassandra-connector-java"),
    settings = defaultSettings ++ Seq(libraryDependencies ++= Dependencies.connector) ++ sbtAssemblySettings,
    dependencies = Seq(connector % "compile;runtime->runtime;test->test;it->it,test;provided->provided")
  ) configs IntegrationTest

  lazy val embedded = Project(
    id = "embedded",
    base = file("spark-cassandra-connector-embedded"),
    settings = defaultSettings ++ Seq(libraryDependencies ++= Dependencies.embedded)
  ) configs IntegrationTest

  lazy val demos = Project(
    id = "demos",
    base = file("spark-cassandra-connector-demos"),
    settings = parentSettings,
    aggregate = Seq(simpleDemos, kafkaStream, twitterStream)
  )

  /* Partitioned to eventually run spark-submit, with only the necessary dependencies per demo. */
  import AssemblyKeys._

  lazy val simpleDemos = Project(
    id = "simple-demos",
    base = file("spark-cassandra-connector-demos/simple-demos"),
    settings = demoSettings ++ demoAssemblySettings ++ Seq(libraryDependencies ++= Dependencies.demos),
    dependencies = Seq(connector, jconnector, embedded))

  lazy val kafkaStream = Project(
    id = "kafka-stream",
    base = file("spark-cassandra-connector-demos/kafka-stream"),
    settings = demoSettings ++ demoAssemblySettings ++ Seq(
      libraryDependencies ++= Dependencies.kafka,
      mainClass in run := Some("com.datastax.spark.connector.demo.KafkaStreamingWordCountApp"),
      mainClass in assembly := Some("com.datastax.spark.connector.demo.KafkaStreamingWordCountApp")),
    dependencies = Seq(connector, embedded))

  lazy val twitterStream = Project(
    id = "twitter-stream",
    base = file("spark-cassandra-connector-demos/twitter-stream"),
    settings = demoSettings ++ demoAssemblySettings ++ Seq(
      libraryDependencies ++= Dependencies.twitter,
      mainClass in assembly := Some("com.datastax.spark.connector.demo.TwitterStreamingApp")),
    dependencies = Seq(connector))

}

object Dependencies {
  import Versions._

  object Compile {

    val akkaActor           = "com.typesafe.akka"       %% "akka-actor"            % Akka           % "provided"                 // ApacheV2
    val akkaRemote          = "com.typesafe.akka"       %% "akka-remote"           % Akka           % "provided"                 // ApacheV2
    val akkaSlf4j           = "com.typesafe.akka"       %% "akka-slf4j"            % Akka           % "provided"                 // ApacheV2
    val cassandraThrift     = "org.apache.cassandra"    % "cassandra-thrift"       % Cassandra exclude("com.google.guava", "guava") // ApacheV2
    val cassandraClient     = "org.apache.cassandra"    % "cassandra-clientutil"   % Cassandra exclude("com.google.guava", "guava") // ApacheV2
    val cassandraDriver     = "com.datastax.cassandra"  % "cassandra-driver-core"  % CassandraDriver exclude("com.google.guava", "guava") withSources()  // ApacheV2
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
    val sparkSql            = "org.apache.spark"        %% "spark-sql"             % Spark exclude("com.google.guava", "guava") exclude("org.apache.spark", "spark-core") // ApacheV2
    val sparkCatalyst       = "org.apache.spark"        %% "spark-catalyst"        % Spark exclude("com.google.guava", "guava") exclude("org.apache.spark", "spark-core") // ApacheV2
    val sparkHive           = "org.apache.spark"        %% "spark-hive"            % Spark exclude("com.google.guava", "guava") exclude("org.apache.spark", "spark-core") // ApacheV2

    object Metrics {
      val metricsCore       = "com.codahale.metrics"    % "metrics-core"           % CodaHaleMetrics
      val metricsJson       = "com.codahale.metrics"    % "metrics-json"           % CodaHaleMetrics % "provided"
    }

    object Embedded {
      val akkaCluster       = "com.typesafe.akka"       %% "akka-cluster"          % Akka                                      // ApacheV2
      val kafka             = "org.apache.kafka"        %% "kafka"                 % Kafka withSources()                       // ApacheV2
      val cassandraServer   = "org.apache.cassandra"    % "cassandra-all"          % Cassandra                                  // ApacheV2
      val jopt              = "net.sf.jopt-simple"      % "jopt-simple"            % JOpt // For kafka command line work
      val sparkRepl         = "org.apache.spark"        %% "spark-repl"            % Spark exclude("com.google.guava", "guava") exclude("org.apache.spark", "spark-core_2.10") exclude("org.apache.spark", "spark-bagel_2.10") exclude("org.apache.spark", "spark-mllib_2.10") exclude("org.scala-lang", "scala-compiler")          // ApacheV2
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
    }
  }

  import Compile._

  val logging = Seq(slf4jApi)

  val metrics = Seq(Metrics.metricsCore, Metrics.metricsJson)

  val testKit = Seq(Test.akkaTestKit, Test.commonsIO, Test.junit,
    Test.junitInterface, Test.scalatest, Test.scalaCompiler, Test.scalactic)

  val akka = Seq(akkaActor, akkaRemote, akkaSlf4j)

  val cassandra = Seq(cassandraThrift, cassandraClient, cassandraDriver)

  val spark = Seq(sparkCore, sparkStreaming, sparkSql, sparkCatalyst, sparkHive)

  val connector = testKit ++ metrics ++ logging ++ akka ++ cassandra ++ spark.map(_ % "provided") ++ Seq(
    commonsLang3, config, guava, jodaC, jodaT, lzf, reflect)

  val embedded = logging ++ spark ++ cassandra ++ Seq(
    Embedded.cassandraServer, Embedded.jopt, Embedded.kafka, Embedded.sparkRepl)

  val demos = metrics ++ logging ++ akka ++ cassandra ++ spark
    Seq(commonsLang3, config, guava, jodaC, jodaT, lzf, reflect)

  val kafka = logging ++ Seq(
    "org.apache.spark" %% "spark-streaming-kafka" % Spark withSources() exclude("com.google.guava", "guava") // ApacheV2
  )

  val twitter = cassandra ++ logging ++ spark ++ Seq(
    "org.apache.spark" %% "spark-streaming-twitter" % Spark exclude("com.google.guava", "guava"),
    "org.twitter4j" % "twitter4j" % "3.0.3",
    "org.twitter4j" % "twitter4j-stream" % "3.0.3"
  ) //.map(_ % "provided")
}
