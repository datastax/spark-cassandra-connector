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

import scala.collection.mutable
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
    version in ThisBuild := "1.1.2-SNAPSHOT",
    scalaVersion := Versions.Scala,
    homepage := Some(url("https://github.com/datastax/spark-cassandra-connector")),
    licenses := Seq(("Apache License, Version 2.0", url("http://www.apache.org/licenses/LICENSE-2.0")))
  )

  val parentSettings = buildSettings ++ Seq(
    publishArtifact := false,
    publish := {}
  )

  override lazy val settings = super.settings ++ buildSettings

  lazy val moduleSettings = graphSettings ++ Seq(
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

  lazy val defaultSettings = moduleSettings ++ mimaSettings ++ releaseSettings ++ testSettings

  lazy val demoSettings = moduleSettings ++ Seq(
    publishArtifact in (Test,packageBin) := false,
    javaOptions in run ++= Seq("-Djava.library.path=./sigar","-Xms128m", "-Xmx1024m", "-XX:+UseConcMarkSweepGC")
  )

  lazy val mimaSettings = mimaDefaultSettings ++ Seq(
    previousArtifact := None
  )

  val tests = inConfig(Test)(Defaults.testTasks) ++ inConfig(IntegrationTest)(Defaults.itSettings)

  lazy val ClusterIntegrationTest = config("extit") extend IntegrationTest
  val itClusterTask = taskKey[Unit]("IntegrationTest in Cluster Task")

  val allArtifacts = mutable.HashSet[String]()
  lazy val jarsInCluster = Seq(
    itClusterTask := {
      val (_, moduleJar) = packagedArtifact.in(Compile,         packageBin).value
      val (_, itTestJar) = packagedArtifact.in(IntegrationTest, packageBin).value
      val (_, testJar)   = packagedArtifact.in(Test,            packageBin).value
      allArtifacts += moduleJar.getAbsolutePath
      allArtifacts += itTestJar.getAbsolutePath
      allArtifacts += testJar.getAbsolutePath
      println("All artifacts: " + allArtifacts.mkString(", "))
    }
  )
  lazy val assembledSettings = defaultSettings ++ jarsInCluster ++ sbtAssemblySettings ++ Seq(
    javaOptions in ClusterIntegrationTest ++= Seq(s"-Dspark.jars=${allArtifacts.mkString(",")}")
  )

  val testOptionSettings = Seq(
    Tests.Argument(TestFrameworks.ScalaTest, "-oDF"),
    Tests.Argument(TestFrameworks.JUnit, "-oDF", "-v", "-a")
  )

  lazy val testArtifacts = Seq(
    artifactName in (Test,packageBin) := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
     baseDirectory.value.name + "-test_" + sv.binary + "-" + module.revision + "." + artifact.extension
    },
    artifactName in (IntegrationTest,packageBin) := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
      baseDirectory.value.name + "-it_" + sv.binary + "-" + module.revision + "." + artifact.extension
    },
    publishArtifact in (Test,packageBin) := true,
    publishArtifact in (IntegrationTest,packageBin) := true,
    publish in (Test,packageBin) := {},
    publish in (IntegrationTest,packageBin) := {}
  )

  lazy val testSettings = tests ++ testArtifacts ++ graphSettings ++ Seq(
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
    jarName in assembly <<= (baseDirectory, version) map { (dir, version) => s"${dir.name}-assembly-$version.jar" },
    run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run)),
    assemblyOption in assembly ~= { _.copy(includeScala = false) },
    mergeStrategy in assembly <<= (mergeStrategy in assembly) {
      (old) => {
        case PathList("com", "google", xs @ _*) => MergeStrategy.last
        case x => old(x)
      }
    }
  )

  /* By default, assembly is not enabled for the demos module, but it can be enabled with
    `-Ddemos.assembly=true`. From the command line this would be:
     sbt -Ddemos.assembly=true twitter/assembly */
  lazy val demoAssemblySettings =
    if (System.getProperty("demos.assembly", "true").toBoolean) sbtAssemblySettings
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

}

object ShellPromptPlugin extends AutoPlugin {
  override def trigger = allRequirements
  override lazy val projectSettings = Seq(
    shellPrompt := buildShellPrompt
  )
  val devnull: ProcessLogger = new ProcessLogger {
    def info (s: => String) {}
    def error (s: => String) { }
    def buffer[T] (f: => T): T = f
  }
  def currBranch =
    ("git status -sb" lines_! devnull headOption).
      getOrElse("-").stripPrefix("## ")
  val buildShellPrompt: State => String = {
    case (state: State) =>
      val currProject = Project.extract (state).currentProject.id
      s"""$currProject:$currBranch> """
  }
}
