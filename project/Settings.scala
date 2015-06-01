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

import java.io.File

import scala.collection.mutable
import scala.language.postfixOps

import com.scalapenos.sbt.prompt.SbtPrompt.autoImport._
import com.typesafe.sbt.SbtScalariform
import com.typesafe.sbt.SbtScalariform._
import com.typesafe.tools.mima.plugin.MimaKeys._
import com.typesafe.tools.mima.plugin.MimaPlugin._
import net.virtualvoid.sbt.graph.Plugin.graphSettings
import sbt.Keys._
import sbt._
import sbtassembly.Plugin.AssemblyKeys._
import sbtassembly.Plugin._
import sbtrelease.ReleasePlugin._

object Settings extends Build {

  import BuildUtil._

  val versionStatus = settingKey[Unit]("The Scala version used in cross-build reapply for '+ package', '+ publish'.")

  def currentCommitSha = ("git rev-parse --short HEAD" !!).split('\n').head.trim

  def versionSuffix = {
    sys.props.get("publish.version.type").map(_.toLowerCase) match {
      case Some("release") ⇒ ""
      case Some("commit-release") ⇒ s"-$currentCommitSha"
      case _ ⇒ "-SNAPSHOT"
    }
  }

  lazy val buildSettings = Seq(
    organization         := "com.datastax.spark",
    version in ThisBuild := s"1.3.0-M2$versionSuffix",
    scalaVersion         := Versions.scalaVersion,
    crossScalaVersions   := Versions.crossScala,
    crossVersion         := CrossVersion.binary,
    versionStatus        := Versions.status(scalaVersion.value, scalaBinaryVersion.value)
  )

  override lazy val settings = super.settings ++ buildSettings ++ Seq(
    normalizedName := "spark-cassandra-connector",
    name := "DataStax Apache Cassandra connector for Apache Spark",
    organization := "com.datastax.spark",
    description  := """
                  |A library that exposes Cassandra tables as Spark RDDs, writes Spark RDDs to
                  |Cassandra tables, and executes CQL queries in Spark applications.""".stringPrefix,
    homepage := Some(url("https://github.com/datastax/spark-cassandra-connector")),
    licenses := Seq(("Apache License, Version 2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))),
    promptTheme := ScalapenosTheme
  )

  val parentSettings = noPublish ++ Seq(
    managedSourceDirectories := Nil,
    (unmanagedSourceDirectories in Compile) := Nil,
    (unmanagedSourceDirectories in Test) := Nil
  )

  lazy val noPublish = Seq(
    publish := {},
    publishLocal := {},
    publishArtifact := false
  )

  val encoding = Seq("-encoding", "UTF-8")

  lazy val projectSettings = graphSettings ++ Seq(

    aggregate in update := false,

    incOptions := incOptions.value.withNameHashing(true),

    ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) },

    // when sbt-release enabled: enableCrossBuild = true,

    /* Can not use -Xfatal-warnings until this known issue fixed:
      org.apache.cassandra.io.util.DataOutputPlus not found - continuing with a stub. */
    scalacOptions ++= encoding ++ Seq(
      s"-target:jvm-${Versions.JDK}",
      "-deprecation",
      "-feature",
      "-language:_",
      "-unchecked",
      "-Xlint"),

    scalacOptions in ThisBuild ++= Seq("-deprecation", "-feature"), // 2.11

    javacOptions ++= encoding ++ Seq(
      "-source", Versions.JDK,
      "-target", Versions.JDK,
      "-Xlint:unchecked",
      "-Xlint:deprecation"
    ),

    scalacOptions in (Compile, doc) ++= Seq(
      "-implicits",
      "-doc-root-content",
      "rootdoc.txt"
    ),

    javacOptions in (Compile, doc) := encoding ++ Seq(
      "-source", Versions.JDK
    ),

    evictionWarningOptions in update := EvictionWarningOptions.default
      .withWarnTransitiveEvictions(false)
      .withWarnDirectEvictions(false)
      .withWarnScalaVersionEviction(false),

    cleanKeepFiles ++= Seq("resolution-cache", "streams", "spark-archives").map(target.value / _),
    updateOptions := updateOptions.value.withCachedResolution(cachedResoluton = true),

    ivyLoggingLevel in ThisBuild := UpdateLogging.Quiet,
    parallelExecution in ThisBuild := false,
    parallelExecution in Global := false,
    apiMappings ++= DocumentationMapping.mapJarToDocURL(
      (managedClasspath in (Compile, doc)).value,
      Dependencies.documentationMappings)
  )

  lazy val mimaSettings = mimaDefaultSettings ++ Seq(
    previousArtifact := None, //Some("a" % "b_2.10.4" % "1.2"),
    binaryIssueFilters ++= Seq.empty
  )

  lazy val defaultSettings = projectSettings ++ mimaSettings ++ releaseSettings ++ testSettings

  lazy val rootSettings = Seq(
    cleanKeepFiles ++= Seq("resolution-cache", "streams", "spark-archives").map(target.value / _)
  )

  lazy val demoSettings = projectSettings ++ noPublish ++ Seq(
    publishArtifact in (Test,packageBin) := false,
    javaOptions in run ++= Seq("-Djava.library.path=./sigar","-Xms128m", "-Xmx1024m", "-XX:+UseConcMarkSweepGC")
  )

  val testConfigs = inConfig(Test)(Defaults.testTasks) ++ inConfig(IntegrationTest)(Defaults.itSettings)

  val pureTestClasspath = taskKey[Set[String]]("Show classpath which is obtained as (test:fullClasspath + it:fullClasspath) - compile:fullClasspath")

  lazy val customTasks = Seq(
    pureTestClasspath := {
      val testDeps = (fullClasspath in Test value) map (_.data.getAbsolutePath) toSet
      val itDeps = (fullClasspath in IntegrationTest value) map (_.data.getAbsolutePath) toSet
      val compileDeps = (fullClasspath in Compile value) map (_.data.getAbsolutePath) toSet

      val cp = (testDeps ++ itDeps) -- compileDeps

      println("TEST_CLASSPATH=" + cp.mkString(File.pathSeparator))

      cp
    }
  )
  lazy val assembledSettings = defaultSettings ++ customTasks ++ sbtAssemblySettings

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
    publishArtifact in Test := false,
    publishArtifact in (Test,packageBin) := true,
    publishArtifact in (IntegrationTest,packageBin) := true,
    publish in (Test,packageBin) := (),
    publish in (IntegrationTest,packageBin) := ()
  )

  lazy val testSettings = testConfigs ++ testArtifacts ++ graphSettings ++ Seq(
    parallelExecution in Test := false,
    parallelExecution in IntegrationTest := false,
    javaOptions in IntegrationTest ++= Seq(
      "-XX:MaxPermSize=256M", "-Xmx1g"
    ),
    testOptions in Test ++= testOptionSettings,
    testOptions in IntegrationTest ++= testOptionSettings,
    fork in Test := true,
    fork in IntegrationTest := true,
    managedSourceDirectories in Test := Nil,
    (compile in IntegrationTest) <<= (compile in Test, compile in IntegrationTest) map { (_, c) => c },
    managedClasspath in IntegrationTest <<= Classpaths.concat(managedClasspath in IntegrationTest, exportedProducts in Test)
  )

  lazy val japiSettings = Seq(
    publishArtifact := true
  )

  lazy val kafkaDemoSettings = Seq(
    excludeFilter in unmanagedSources := (CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, minor)) if minor < 11 => HiddenFileFilter || "*Scala211App*"
      case _ => HiddenFileFilter || "*WordCountApp*"
    }))

  lazy val sbtAssemblySettings = assemblySettings ++ Seq(
    parallelExecution in assembly := false,
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