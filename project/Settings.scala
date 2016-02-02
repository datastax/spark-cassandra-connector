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
import java.nio.file.{Paths, Files}

import scala.language.postfixOps

import com.scalapenos.sbt.prompt.SbtPrompt.autoImport._
import com.typesafe.sbt.SbtScalariform
import com.typesafe.sbt.SbtScalariform._
import com.typesafe.tools.mima.plugin.MimaKeys._
import com.typesafe.tools.mima.plugin.MimaPlugin._
import net.virtualvoid.sbt.graph.Plugin.graphSettings
import sbt.Keys._
import sbt.Tests._
import sbt._
import sbtassembly.AssemblyKeys._
import sbtassembly.AssemblyPlugin._
import sbtassembly._
import sbtrelease.ReleasePlugin._
import sbtsparkpackage.SparkPackagePlugin.autoImport._
import java.lang.management.ManagementFactory;
import com.sun.management.OperatingSystemMXBean;

object Settings extends Build {

  import BuildUtil._

  val versionStatus = settingKey[Unit]("The Scala version used in cross-build reapply for '+ package', '+ publish'.")

  val cassandraServerClasspath = taskKey[String]("Cassandra server classpath")

  val mavenLocalResolver = BuildUtil.mavenLocalResolver

  // Travis has limited quota, so we cannot use many C* instances simultaneously
  val isTravis = sys.props.getOrElse("travis", "false").toBoolean

  val osmxBean = ManagementFactory.getOperatingSystemMXBean.asInstanceOf[OperatingSystemMXBean]
  val sysMemoryInMB = osmxBean.getTotalPhysicalMemorySize >> 20
  val singleRunRequiredMem = 3 * 1024 + 512
  val parallelTasks = if (isTravis) 1 else Math.max(1, ((sysMemoryInMB - 1550) / singleRunRequiredMem).toInt)

  // Due to lack of entrophy on virtual machines we want to use /dev/urandom instead of /dev/random
  val useURandom = Files.exists(Paths.get("/dev/urandom"))
  val uRandomParams = if (useURandom) Seq("-Djava.security.egd=file:/dev/./urandom") else Seq.empty

  lazy val mainDir = {
    val dir = new File(".")
    IO.delete(new File(dir, "target/ports"))
    dir
  }

  val cassandraTestVersion = sys.props.get("test.cassandra.version").getOrElse(Versions.Cassandra)

  lazy val TEST_JAVA_OPTS = Seq(
    "-XX:MaxPermSize=256M",
    "-Xms256m",
    "-Xmx512m",
    "-Dsun.io.serialization.extendedDebugInfo=true",
    s"-DbaseDir=${mainDir.getAbsolutePath}") ++ uRandomParams

  var TEST_ENV: Option[Map[String, String]] = None

  val asfSnapshotsResolver = "ASF Snapshots" at "https://repository.apache.org/content/groups/snapshots"
  val asfStagingResolver = "ASF Staging" at "https://repository.apache.org/content/groups/staging"

  def currentCommitSha = ("git rev-parse --short HEAD" !!).split('\n').head.trim

  def versionSuffix = {
    sys.props.get("publish.version.type").map(_.toLowerCase) match {
      case Some("release") => ""
      case Some("commit-release") => s"-$currentCommitSha"
      case _ => "-SNAPSHOT"
    }
  }

  lazy val buildSettings = Seq(
    organization         := "com.datastax.spark",
    version in ThisBuild := s"1.6.0-M1$versionSuffix",
    scalaVersion         := Versions.scalaVersion,
    crossScalaVersions   := Versions.crossScala,
    crossVersion         := CrossVersion.binary,
    versionStatus        := Versions.status(scalaVersion.value, scalaBinaryVersion.value)
  )

  lazy val sparkPackageSettings = Seq(
    spName := "datastax/spark-cassandra-connector",
    sparkVersion := Versions.Spark,
    spAppendScalaVersion := true,
    spIncludeMaven := true,
    spIgnoreProvided := true,
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

  lazy val projectSettings = graphSettings ++ Seq(

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
  lazy val assembledSettings = defaultSettings ++ customTasks ++ sparkPackageSettings ++ sbtAssemblySettings

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

  def makeTestGroups(tests: Seq[TestDefinition]): Seq[Group] = {
    // if we have many C* instances and we can run multiple tests in parallel, then group by package name
    // additional groups for auth and ssl is just an optimisation
    def multiCInstanceGroupingFunction(test: TestDefinition): String = {
      if (test.name.toLowerCase.contains("auth")) "auth"
      else if (test.name.toLowerCase.contains("ssl")) "ssl"
      else test.name.reverse.dropWhile(_ != '.').reverse
    }

    // if we have a single C* create as little groups as possible to avoid restarting C*
    // the minimum - we need to run REPL and streaming tests in separate processes
    // additional groups for auth and ssl is just an optimisation
    def singleCInstanceGroupingFunction(test: TestDefinition): String = {
      val pkgName = test.name.reverse.dropWhile(_ != '.').reverse
      if (test.name.toLowerCase.contains("authenticate")) "auth"
      else if (test.name.toLowerCase.contains("ssl")) "ssl"
      else if (pkgName.contains(".repl")) "repl"
      else if (pkgName.contains(".streaming")) "streaming"
      else "other"
    }

    val groupingFunction = if (parallelTasks == 1) singleCInstanceGroupingFunction _ else multiCInstanceGroupingFunction _

    tests.groupBy(groupingFunction).map { case (pkg, testsSeq) =>
      new Group(
        name = pkg,
        tests = testsSeq,
        runPolicy = SubProcess(ForkOptions(
          runJVMOptions = TEST_JAVA_OPTS,
          envVars = TEST_ENV.getOrElse(sys.env),
          outputStrategy = Some(StdoutOutput))))
    }.toSeq
  }

  lazy val testSettings = testConfigs ++ testArtifacts ++ graphSettings ++ Seq(
    parallelExecution in Test := true,
    parallelExecution in IntegrationTest := true,
    javaOptions in IntegrationTest ++= TEST_JAVA_OPTS,
    testOptions in Test ++= testOptionSettings,
    testOptions in IntegrationTest ++= testOptionSettings,
    testGrouping in IntegrationTest <<= definedTests in IntegrationTest map makeTestGroups,
    fork in Test := true,
    fork in IntegrationTest := true,
    managedSourceDirectories in Test := Nil,
    (compile in IntegrationTest) <<= (compile in Test, compile in IntegrationTest) map { (_, c) => c },
    (internalDependencyClasspath in IntegrationTest) <<= Classpaths.concat(
      internalDependencyClasspath in IntegrationTest,
      exportedProducts in Test)
  )

  lazy val pureCassandraSettings = Seq(
    test in IntegrationTest <<= (
      cassandraServerClasspath in CassandraSparkBuild.cassandraServerProject in IntegrationTest,
      envVars in IntegrationTest,
      test in IntegrationTest) { case (cassandraServerClasspathTask, envVarsTask, testTask) =>
        cassandraServerClasspathTask.flatMap(_ => envVarsTask).flatMap(_ => testTask)
    },
    envVars in IntegrationTest := {
      val env = sys.env +
        ("CASSANDRA_CLASSPATH" ->
          (cassandraServerClasspath in CassandraSparkBuild.cassandraServerProject in IntegrationTest).value) +
        ("SPARK_LOCAL_IP" -> "127.0.0.1")
      TEST_ENV = Some(env)
      env
    }
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
    assemblyJarName in assembly <<= (baseDirectory, version) map { (dir, version) => s"${dir.name}-assembly-$version.jar" },
    run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run)),
    assemblyOption in assembly ~= { _.copy(includeScala = false) },
    assemblyMergeStrategy in assembly <<= (assemblyMergeStrategy in assembly) {
      (old) => {
        case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
        case PathList("META-INF", xs @ _*) => MergeStrategy.last
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
