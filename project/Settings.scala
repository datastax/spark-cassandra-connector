/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import sbtrelease.ReleasePlugin._
import sbtassembly.Plugin._
import AssemblyKeys._
import com.typesafe.tools.mima.plugin.MimaKeys._
import com.typesafe.tools.mima.plugin.MimaPlugin._
import com.typesafe.sbt.SbtScalariform
import com.typesafe.sbt.SbtScalariform._

import scala.language.postfixOps

import net.virtualvoid.sbt.graph.Plugin.graphSettings

object Settings extends Build {

  lazy val buildSettings = Seq(
    name := "DataStax Apache Cassandra connector for Apache Spark",
    normalizedName := "spark-cassandra-connector",
    description := "A library that exposes Cassandra tables as Spark RDDs, writes Spark RDDs to Cassandra tables, " +
      "and executes CQL queries in Spark applications.",
    organization := "com.datastax.spark",
    organizationHomepage := Some(url("http://www.datastax.com/")),
    version in ThisBuild := "1.1.0-SNAPSHOT",
    scalaVersion := Versions.Scala,
    homepage := Some(url("https://github.com/datastax/spark-cassandra-connector")),
    licenses := Seq(("Apache License, Version 2.0", url("http://www.apache.org/licenses/LICENSE-2.0")))
  )

  val parentSettings = buildSettings ++ assemblySettings ++ Seq(
    publishArtifact := false,
    publish := {}
  )

  lazy val defaultSettings = testSettings ++ mimaSettings ++ releaseSettings ++ graphSettings ++ Seq(
    scalacOptions in (Compile, doc) ++= Seq("-implicits","-doc-root-content", "rootdoc.txt"),
    scalacOptions ++= Seq("-encoding", "UTF-8", s"-target:jvm-${Versions.JDK}", "-deprecation", "-feature", "-language:_", "-unchecked", "-Xlint"),
    javacOptions in (Compile, doc) := Seq("-encoding", "UTF-8", "-source", Versions.JDK),
    javacOptions in Compile ++= Seq("-encoding", "UTF-8", "-source", Versions.JDK, "-target", Versions.JDK, "-Xlint:unchecked", "-Xlint:deprecation"),
    ivyLoggingLevel in ThisBuild := UpdateLogging.Quiet,
    // tbd: crossVersion := CrossVersion.binary,
    parallelExecution in ThisBuild := false,
    parallelExecution in Global := false,
    autoAPIMappings := true
  )

  lazy val demoSettings = defaultSettings ++ mimaSettings ++ releaseSettings ++ sbtAssemblyDemoSettings ++ Seq(
    javaOptions in run ++= Seq("-Djava.library.path=./sigar","-Xms128m",  "-Xms2G", "-Xmx2G", "-Xmn384M", "-XX:+UseConcMarkSweepGC", "-Xmx1024m"),
    scalacOptions ++= Seq("-encoding", "UTF-8", s"-target:jvm-${Versions.JDK}", "-deprecation", "-feature", "-language:_", "-unchecked", "-Xlint"),
    javacOptions in Compile ++= Seq("-encoding", "UTF-8", "-source", Versions.JDK, "-target", Versions.JDK, "-Xlint:unchecked", "-Xlint:deprecation"),
    ivyLoggingLevel in ThisBuild := UpdateLogging.Quiet,
    parallelExecution in ThisBuild := false,
    parallelExecution in Global := false
  )

  lazy val mimaSettings = mimaDefaultSettings ++ Seq(
    previousArtifact := None
  )

  val tests = inConfig(Test)(Defaults.testTasks) ++ inConfig(IntegrationTest)(Defaults.itSettings)

  val testOptionSettings = Seq(
    Tests.Argument(TestFrameworks.ScalaTest, "-oDF"),
    Tests.Argument(TestFrameworks.JUnit, "-oDF", "-v", "-a")
  )

  lazy val testSettings = tests ++ graphSettings ++ Seq(
    parallelExecution in Test := false,
    parallelExecution in IntegrationTest := false,
    testOptions in Test ++= testOptionSettings,
    testOptions in IntegrationTest ++= testOptionSettings,
    fork in Test := true,
    fork in IntegrationTest := true,
    (compile in IntegrationTest) <<= (compile in Test, compile in IntegrationTest) map { (_, c) => c },
    managedClasspath in IntegrationTest <<= Classpaths.concat(managedClasspath in IntegrationTest, exportedProducts in Test)
  )

  lazy val sbtAssemblySettings = assemblySettings ++ Seq(
    jarName in assembly <<= (normalizedName, version) map { (name, version) => s"$name-assembly-$version.jar" },
    assemblyOption in assembly ~= { _.copy(includeScala = false) },
      mergeStrategy in assembly <<= (mergeStrategy in assembly) {
      (old) => {
        case PathList("org", "jboss", "netty", xs @ _*) => MergeStrategy.discard
        case PathList("com", "google", xs @ _*) => MergeStrategy.discard
        case x => old(x)
      }
    }
  )

  /* By default, assembly is not enabled for the demos module, but it can be enabled with
    `-Dspark.cassandra.connector.demos.assembly=true`. From the command line this would be:
     sbt -Dspark.cassandra.connector.demos.assembly=true assembly */
  lazy val sbtAssemblyDemoSettings =
    if (System.getProperty("spark.cassandra.connector.demos.assembly", "false").toBoolean)
      sbtAssemblySettings ++ Seq(
        mergeStrategy in assembly <<= (mergeStrategy in assembly) {
          (old) => {
            case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
            case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
            case PathList("akka", "util", xs @ _*) => MergeStrategy.first
            case PathList("akka", "remote", xs @ _*) => MergeStrategy.first
            case PathList("akka", "routing", xs @ _*) => MergeStrategy.first
            case PathList("org", "apache", "commons", xs @ _*) => MergeStrategy.first
            case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.first
            case x => old(x)
          }
        })
    else Seq.empty

  lazy val formatSettings = SbtScalariform.scalariformSettings ++ Seq(
    ScalariformKeys.preferences in Compile  := formattingPreferences,
    ScalariformKeys.preferences in Test     := formattingPreferences
  )

  def formattingPreferences = {
    import scalariform.formatter.preferences._
    FormattingPreferences()
      .setPreference(RewriteArrowSymbols, false)
      .setPreference(AlignParameters, true)
      .setPreference(AlignSingleLineCaseStatements, true)
  }

  override lazy val settings = super.settings ++ buildSettings ++ Seq(shellPrompt := ShellPrompt.prompt)
}

/**
 * TODO make plugin
 * Shell prompt which shows the current project, git branch
 */
object ShellPrompt {

  def gitBranches = ("git branch" lines_! devnull).mkString

  def current: String = """\*\s+([\.\w-]+)""".r findFirstMatchIn gitBranches map (_ group 1) getOrElse "-"

  def currBranch: String = ("git status -sb" lines_! devnull headOption) getOrElse "-" stripPrefix "## "

  lazy val prompt = (state: State) =>
    "%s:%s:%s> ".format("spark-cassandra-connector", Project.extract (state).currentProject.id, currBranch)

  object devnull extends ProcessLogger {
    def info(s: => String) {}
    def error(s: => String) {}
    def buffer[T](f: => T): T = f
  }
}
