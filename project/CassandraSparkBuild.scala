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

  lazy val root = Project(
    id = "root",
    base = file("."),
    settings = parentSettings,
    aggregate = Seq(connector, connectorJava, embedded, demos)
  )

  lazy val connector = LibraryProject("spark-cassandra-connector", Seq(libraryDependencies ++= Dependencies.connector),
    Seq(embedded % "test->test;it->it,test;"))

  lazy val connectorJava = LibraryProject("spark-cassandra-connector-java", Seq(libraryDependencies ++= Dependencies.connector),
    Seq(connector % "compile;runtime->runtime;test->test;it->it,test;provided->provided"))

  lazy val embedded = LibraryProject("spark-cassandra-connector-embedded", Seq(libraryDependencies ++= Dependencies.embedded))

  lazy val demos = Project(
    id = "spark-cassandra-connector-demos",
    base = file("spark-cassandra-connector-demos"),
    settings = demoSettings ++ Seq(libraryDependencies ++= Dependencies.demos),
    dependencies = Seq(connector, connectorJava, embedded))

  def LibraryProject(name: String, dsettings: Seq[Def.Setting[_]], cpd: Seq[ClasspathDep[ProjectReference]] = Seq.empty): Project =
    Project(name, file(name), settings = defaultSettings ++ sbtAssemblySettings ++ dsettings, dependencies = cpd) configs IntegrationTest

}

object Dependencies {
  import Versions._

  object Compile {

    val akkaActor           = "com.typesafe.akka"       %% "akka-actor"            % Akka           % "provided"                 // ApacheV2
    val akkaRemote          = "com.typesafe.akka"       %% "akka-remote"           % Akka           % "provided"                 // ApacheV2
    val akkaSlf4j           = "com.typesafe.akka"       %% "akka-slf4j"            % Akka           % "provided"                 // ApacheV2
    val cassandraThrift     = "org.apache.cassandra"    % "cassandra-thrift"       % Cassandra
    val cassandraClient     = "org.apache.cassandra"    % "cassandra-clientutil"   % Cassandra
    val cassandraDriver     = "com.datastax.cassandra"  % "cassandra-driver-core"  % Cassandra                    withSources()  // ApacheV2
    val commonsLang3        = "org.apache.commons"      % "commons-lang3"          % CommonsLang3                                // ApacheV2
    val config              = "com.typesafe"            % "config"                 % Config         % "provided"                 // ApacheV2
    val guava               = "com.google.guava"        % "guava"                  % Guava          % "provided"  force()
    val jodaC               = "org.joda"                % "joda-convert"           % JodaC
    val jodaT               = "joda-time"               % "joda-time"              % JodaT
    val lzf                 = "com.ning"                % "compress-lzf"           % Lzf            % "provided"
    val reflect             = "org.scala-lang"          % "scala-reflect"          % Scala
    val slf4jApi            = "org.slf4j"               % "slf4j-api"              % Slf4j          % "provided"                 // MIT
    /* To allow spark artifact inclusion in the demo module at runtime, we set 'provided'
       scope on the connector below, specifically, versus globally here. */
    val sparkCore           = "org.apache.spark"        %% "spark-core"            % Spark exclude("com.google.guava", "guava") // ApacheV2
    val sparkStreaming      = "org.apache.spark"        %% "spark-streaming"       % Spark exclude("com.google.guava", "guava") // ApacheV2

    object Metrics {
      val metricsJson       = "com.codahale.metrics"    % "metrics-json"           % MetricsJson    % "provided"
    }

    object Embedded {
      val sparkStreamingKafka   = "org.apache.spark"     %% "spark-streaming-kafka"   % Spark      exclude("com.google.guava", "guava") // ApacheV2
      val sparkStreamingTwitter = "org.apache.spark"     %% "spark-streaming-twitter" % Spark      exclude("com.google.guava", "guava") // ApacheV2
      val sparkStreamingZmq     = "org.apache.spark"     %% "spark-streaming-zeromq"  % Spark      exclude("com.google.guava", "guava") // ApacheV2
      val cassandraServer       = "org.apache.cassandra" % "cassandra-all"            % Cassandra                                  // ApacheV2
      val jopt                  =  "net.sf.jopt-simple"  % "jopt-simple"              % JOpt       // For kafka command line work
      val sparkRepl             = "org.apache.spark"     %% "spark-repl"              % Spark exclude("com.google.guava", "guava") exclude("org.apache.spark", "spark-core_2.10") exclude("org.apache.spark", "spark-bagel_2.10") exclude("org.apache.spark", "spark-mllib_2.10") exclude("org.scala-lang", "scala-compiler")          // ApacheV2
    }

    object Test {
      val akkaTestKit     = "com.typesafe.akka"       %% "akka-testkit"         % Akka           % "test,it"                 // ApacheV2
      val commonsIO       = "commons-io"              % "commons-io"            % CommonsIO      % "test,it"                 // ApacheV2
      // Eventually migrate junit out in favor of the scala test APIs
      val junit           = "junit"                   % "junit"                 % "4.11"         % "test,it"                 // for now
      val junitInterface  = "com.novocode"            % "junit-interface"       % "0.10"         % "test,it"
      val scalatest       = "org.scalatest"           %% "scalatest"            % ScalaTest      % "test,it"                 // ApacheV2
      val scalactic       = "org.scalactic"           %% "scalactic"            % Scalactic      % "test,it"                 // ApacheV2
      val scalaCompiler   = "org.scala-lang"          % "scala-compiler"        % Scala
    }
  }

  import Compile._

  val logging = Seq(slf4jApi)

  // Consider: Metrics.metricsJvm, Metrics.latencyUtils, Metrics.hdrHistogram
  val metrics = Seq(Metrics.metricsJson)

  val testKit = Seq(Test.akkaTestKit, Test.commonsIO, Test.junit,
    Test.junitInterface, Test.scalatest, Test.scalaCompiler, Test.scalactic)

  val akka = Seq(akkaActor, akkaRemote, akkaSlf4j)

  val cassandra = Seq(cassandraThrift, cassandraClient, cassandraDriver)

  val spark = Seq(sparkCore, sparkStreaming)

  val connector = testKit ++ metrics ++ logging ++ akka ++ cassandra ++ spark.map(_ % "provided") ++ Seq(
    commonsLang3, config, guava, jodaC, jodaT, lzf, reflect)

  import Embedded._
  val sparkExtStreaming = Seq(sparkStreamingKafka, sparkStreamingTwitter, sparkStreamingZmq)

  val embedded = logging ++ spark ++ sparkExtStreaming ++ cassandra ++ Seq(cassandraServer, jopt, sparkRepl)

  val demos = metrics ++ logging ++ akka ++ cassandra ++ spark ++ sparkExtStreaming ++
    Seq(commonsLang3, config, guava, jodaC, jodaT, lzf, reflect) ++
    /* Not using yet but soon. */
    Seq("com.typesafe.akka" %% "akka-cluster" % Versions.Akka,
        "com.typesafe.akka" %% "akka-contrib" % Versions.Akka)

}
