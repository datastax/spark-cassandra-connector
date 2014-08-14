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
    aggregate = Seq(connector, connectorJava, demos)
  )

  lazy val connector = LibraryProject("spark-cassandra-connector", Seq(libraryDependencies ++= Dependencies.connector))

  lazy val connectorJava = LibraryProject("spark-cassandra-connector-java", Seq(libraryDependencies ++= Dependencies.connector),
    Seq(connector))

  lazy val demos = LibraryProject("spark-cassandra-connector-demos",
    demoSettings ++ Seq(libraryDependencies ++= Dependencies.demos), Seq(connector, connectorJava))

  def LibraryProject(name: String, dsettings: Seq[Def.Setting[_]], cpd: Seq[ClasspathDep[ProjectReference]] = Seq.empty): Project =
    Project(name, file(name), settings = defaultSettings ++ sbtAssemblySettings ++ dsettings,
      dependencies = cpd.map(_.project % "compile;runtime->runtime;test->test;it->it,test;provided->provided")) configs IntegrationTest

}

object Dependencies {

  object Compile {
    import Versions._

    val akkaActor         = "com.typesafe.akka"       %% "akka-actor"           % Akka           % "provided"                 // ApacheV2
    val akkaRemote        = "com.typesafe.akka"       %% "akka-remote"          % Akka           % "provided"                 // ApacheV2
    val akkaSlf4j         = "com.typesafe.akka"       %% "akka-slf4j"           % Akka           % "provided"                 // ApacheV2
    val cassandraThrift   = "org.apache.cassandra"    % "cassandra-thrift"      % Cassandra
    val cassandraClient   = "org.apache.cassandra"    % "cassandra-clientutil"  % Cassandra
    val cassandraDriver   = "com.datastax.cassandra"  % "cassandra-driver-core" % CassandraDriver              withSources()  // ApacheV2
    val commonsLang3      = "org.apache.commons"      % "commons-lang3"         % CommonsLang3                                // ApacheV2
    val config            = "com.typesafe"            % "config"                % Config         % "provided"                 // ApacheV2
    val guava             = "com.google.guava"        % "guava"                 % Guava          % "provided"  force()
    val jodaC             = "org.joda"                % "joda-convert"          % JodaC
    val jodaT             = "joda-time"               % "joda-time"             % JodaT
    val lzf               = "com.ning"                % "compress-lzf"          % Lzf            % "provided"
    val reflect           = "org.scala-lang"          % "scala-reflect"         % Scala
    val slf4jApi          = "org.slf4j"               % "slf4j-api"             % Slf4j          % "provided"                 // MIT
    /* To allow spark artifact inclusion in the demo module at runtime, we set 'provided'
       scope on the connector below, specifically, versus globally here. */
    val sparkCore         = "org.apache.spark"        %% "spark-core"           % Spark                        exclude("com.google.guava", "guava") // ApacheV2
    val sparkStreaming    = "org.apache.spark"        %% "spark-streaming"      % Spark                        exclude("com.google.guava", "guava") // ApacheV2

    object Metrics {
      val metricsJson     = "com.codahale.metrics"    % "metrics-json"          % MetricsJson    % "provided"
    }

    object Test {
      val akkaTestKit     = "com.typesafe.akka"       %% "akka-testkit"         % Akka           % "test,it"                 // ApacheV2
      val cassandraServer = "org.apache.cassandra"    % "cassandra-all"         % Cassandra      % "test,it"                 // ApacheV2
      val commonsIO       = "commons-io"              % "commons-io"            % CommonsIO      % "test,it"                 // ApacheV2
      // Eventually migrate junit out in favor of the scala test APIs
      val junit           = "junit"                   % "junit"                 % "4.11"         % "test,it"                 // for now
      val junitInterface  = "com.novocode"            % "junit-interface"       % "0.10"         % "test,it"
      val scalatest       = "org.scalatest"           %% "scalatest"            % ScalaTest      % "test,it"                 // ApacheV2
    }
  }

  import Compile._

  val logging = Seq(slf4jApi)

  // Consider: Metrics.metricsJvm, Metrics.latencyUtils, Metrics.hdrHistogram
  val metrics = Seq(Metrics.metricsJson)

  val testKit = Seq(Test.akkaTestKit, Test.cassandraServer, Test.commonsIO, Test.junit, Test.junitInterface, Test.scalatest)

  val akka = Seq(akkaActor, akkaRemote, akkaSlf4j)

  val cassandra = Seq(cassandraThrift, cassandraClient, cassandraDriver)

  val spark = Seq(sparkCore, sparkStreaming)

  val connector = testKit ++ metrics ++ logging ++ akka ++ cassandra ++ spark.map(_ % "provided") ++ Seq(
    commonsLang3, config, guava, jodaC, jodaT, lzf, reflect)

  val demos = metrics ++ logging ++ akka ++ cassandra ++ spark ++
    Seq(commonsLang3, config, guava, jodaC, jodaT, lzf, reflect) ++
    Seq("com.typesafe.akka" %% "akka-cluster" % Versions.Akka,
     "com.typesafe.akka"    %% "akka-contrib" % Versions.Akka)

}
