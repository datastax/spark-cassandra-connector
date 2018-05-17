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

import scala.language.postfixOps

import com.scalapenos.sbt.prompt.SbtPrompt.autoImport._
import com.typesafe.tools.mima.plugin.MimaKeys._
import com.typesafe.tools.mima.plugin.MimaPlugin._
import sbt.Keys._
import sbt._
import sbtassembly.AssemblyKeys._
import sbtassembly.AssemblyPlugin._
import sbtassembly._
import sbtrelease.ReleasePlugin._
import sbtsparkpackage.SparkPackagePlugin.autoImport._

object Settings extends Build {

  import BuildUtil._

  val versionStatus = settingKey[Unit]("The Scala version used in cross-build reapply for '+ package', '+ publish'.")

  val mavenLocalResolver = BuildUtil.mavenLocalResolver

  val asfSnapshotsResolver = "ASF Snapshots" at "https://repository.apache.org/content/groups/snapshots"
  val asfStagingResolver = "ASF Staging" at "https://repository.apache.org/content/groups/staging"

  def currentVersion = ("git describe --tags --match v*" !!).trim.substring(1)

  lazy val buildSettings = Seq(
    organization         := "com.datastax.spark",
    version in ThisBuild := currentVersion,
    scalaVersion         := Versions.scalaVersion,
    crossVersion         := CrossVersion.binary,
    versionStatus        := Versions.status(scalaVersion.value, scalaBinaryVersion.value)
  )

  lazy val sparkPackageSettings = Seq(
    spName := "datastax/spark-cassandra-connector",
    sparkVersion := Versions.Spark,
    spAppendScalaVersion := true,
    spIncludeMaven := true,
    spIgnoreProvided := true,
    spShade := true,
    credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")
  )

  override lazy val settings = super.settings ++ buildSettings ++ Seq(
    normalizedName := "spark-cassandra-connector",
    name := "DataStax Apache Cassandra connector for Apache Spark",
    organization := "com.datastax.spark",
    description  := """
                  |A library that exposes Cassandra tables as Spark RDDs, writes Spark RDDs to
                  |Cassandra tables, and executes CQL queries in Spark applications.""".stringPrefix,
    homepage := Some(url("https://github.com/datastax/spark-cassandra-connector")),
    licenses := Seq(("Apache License 2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))),
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

  val installSparkTask = taskKey[Unit]("Optionally install Spark from Git to local Maven repository")

  lazy val projectSettings = Seq(

    concurrentRestrictions in Global += Tags.limit(Tags.Test, parallelTasks),

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
    parallelExecution in ThisBuild := true,
    parallelExecution in Global := true,
    apiMappings ++= DocumentationMapping.mapJarToDocURL(
      (managedClasspath in (Compile, doc)).value,
      Dependencies.documentationMappings),
    installSparkTask := {
      val dir = new File(".").toPath
      SparkInstaller(scalaBinaryVersion.value, dir)
    },
    resolvers ++= Seq(mavenLocalResolver, asfStagingResolver, asfSnapshotsResolver),
    update <<= (installSparkTask, update) map {(_, out) => out}
  )

  lazy val mimaSettings = mimaDefaultSettings ++ Seq(
    previousArtifact := None, //Some("a" % "b_2.10.4" % "1.2"),
    binaryIssueFilters ++= Seq.empty
  )

  lazy val defaultSettings = projectSettings ++ mimaSettings ++ releaseSettings ++ Testing.testSettings

  lazy val rootSettings = Seq(
    cleanKeepFiles ++= Seq("resolution-cache", "streams", "spark-archives").map(target.value / _),
    updateOptions := updateOptions.value.withCachedResolution(true)
  )

  lazy val sbtAssemblySettings = assemblySettings ++ Seq(
    parallelExecution in assembly := false,
    assemblyJarName in assembly <<= (baseDirectory, version) map { (dir, version) => s"${dir.name}-assembly-$version.jar" },
    run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run)),
    assemblyOption in assembly ~= { _.copy(includeScala = false) },
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
      case PathList("META-INF", xs @ _*) => MergeStrategy.last
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    },
    assemblyShadeRules in assembly := {
      val shadePackage = "shade.com.datastax.spark.connector"
      Seq(
        ShadeRule.rename("com.google.common.**" -> s"$shadePackage.google.common.@1").inAll,
        ShadeRule.rename("com.google.thirdparty.publicsuffix.**" -> s"$shadePackage.google.thirdparty.publicsuffix.@1").inAll
      )
    }
  )

  lazy val assembledSettings = defaultSettings ++ Testing.testTasks ++ sbtAssemblySettings ++ sparkPackageSettings

}
