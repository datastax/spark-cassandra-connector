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

import scala.collection.mutable
import scala.language.postfixOps

import sbt._
import sbt.Keys._
import sbtrelease.ReleasePlugin._
import sbtassembly.Plugin._
import AssemblyKeys._
import com.typesafe.tools.mima.plugin.MimaKeys._
import com.typesafe.tools.mima.plugin.MimaPlugin._
import com.typesafe.sbt.SbtScalariform
import com.typesafe.sbt.SbtScalariform._
import net.virtualvoid.sbt.graph.Plugin.graphSettings
import com.scalapenos.sbt.prompt.SbtPrompt.autoImport._
import com.scalapenos.sbt.prompt.PromptTheme

object Settings extends Build {

  override lazy val settings = super.settings ++ Seq(
    normalizedName := "spark-cassandra-connector",
    name := "DataStax Apache Cassandra connector for Apache Spark",
    description := """A library that exposes Cassandra tables as Spark RDDs, writes Spark RDDs to
                  Cassandra tables, and executes CQL queries in Spark applications.""",
    organization := "com.datastax.spark",
    organizationHomepage := Some(url("http://www.datastax.com/")),
    homepage := Some(url("https://github.com/datastax/spark-cassandra-connector")),
    licenses := Seq(("Apache License, Version 2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))),

    version in ThisBuild := "1.2.0-alpha1-SNAPSHOT",

    scalaVersion in GlobalScope := Versions.detectedScala,

    crossScalaVersions in GlobalScope := Versions.crossScala,

    crossVersion := {
      println(s"""
           |Running:
           |  Scala: ${scalaVersion.value} ${Versions.hint}
           |  Scala Binary: ${scalaBinaryVersion.value}
           |  Java: target=${Versions.JDK} user=${Versions.userJava}
         """.stripMargin)
      CrossVersion.binary
    },

    // when sbt-release enabled: enableCrossBuild = true,

    /* Can not use -Xfatal-warnings until this known issue fixed:
      org.apache.cassandra.io.util.DataOutputPlus not found - continuing with a stub. */
    scalacOptions ++= encoding ++ Seq(
      s"-target:jvm-${Versions.JDK}",
      "-deprecation",
      "-feature",
      "-language:_",
      "-unchecked",
      "-Xlint")
    ,

    scalacOptions in ThisBuild ++= Seq("-deprecation", "-feature"), // 2.11

    javacOptions ++= encoding ++ Seq(
      "-source", Versions.JDK,
      "-target", Versions.JDK,
      "-Xlint:unchecked",
      "-Xlint:deprecation"
    ),

    evictionWarningOptions in update := EvictionWarningOptions.default
      .withWarnTransitiveEvictions(true)
      .withWarnDirectEvictions(false)
      .withWarnScalaVersionEviction(true),

    promptTheme := theme
  )

  val parentSettings = noPublish ++ Seq(
    (unmanagedSourceDirectories in Compile) := Nil,
    (unmanagedSourceDirectories in Test) := Nil
  )

  lazy val noPublish = Seq(
    publish := (),
    publishLocal := (),
    publishArtifact := false
  )

  val encoding = Seq("-encoding", "UTF-8")
 
  lazy val moduleSettings = graphSettings ++ Seq(
    ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) },

    scalacOptions in (Compile, doc) ++= Seq(
      "-implicits",
      "-doc-root-content",
      "rootdoc.txt"
    ),

    javacOptions in (Compile, doc) := encoding ++ Seq(
      "-source", Versions.JDK
    ),
    ivyLoggingLevel in ThisBuild := UpdateLogging.Quiet,
    parallelExecution in ThisBuild := false,
    parallelExecution in Global := false,
    autoAPIMappings := true,
    compileOrder := CompileOrder.Mixed
  )

  lazy val defaultSettings = moduleSettings ++ mimaSettings ++ releaseSettings ++ testSettings

  lazy val demoSettings = moduleSettings ++ noPublish ++ Seq(
    publishArtifact in (Test,packageBin) := false,
    javaOptions in run ++= Seq("-Djava.library.path=./sigar","-Xms128m", "-Xmx1024m", "-XX:+UseConcMarkSweepGC")
  )

  lazy val mimaSettings = mimaDefaultSettings ++ Seq(
    previousArtifact := None
  )

  val testConfigs = inConfig(Test)(Defaults.testTasks) ++ inConfig(IntegrationTest)(Defaults.itSettings)

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

  lazy val theme = PromptTheme(List(
    text("[SBT] ", fg(magenta)),
    userName(fg(000)),
    text(":spark-cassandra-connector", fg(000)),
    text(":", fg(000)),
    currentProject(fg(000)),
    text(":", fg(000)),
    gitBranch(clean = fg(green), dirty = fg(20)),
    text("> ", fg(000))
  ))

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