import java.io.File
import java.nio.file.{Files, Paths}

import sbt.Keys._
import sbt.Tests.{Group, SubProcess}
import sbt.{Artifact, Classpaths, Defaults, ModuleID, ScalaVersion, TestFrameworks, Tests, _}
import BuildUtil.parallelTasks

object Testing extends Build {
  // Due to lack of entrophy on virtual machines we want to use /dev/urandom instead of /dev/random
  private val useURandom = Files.exists(Paths.get("/dev/urandom"))
  private val uRandomParams = if (useURandom) Seq("-Djava.security.egd=file:/dev/./urandom") else Seq.empty
  val cassandraServerClasspath = taskKey[String]("Cassandra server classpath")

  private lazy val mainDir = {
    val dir = new File(".")
    IO.delete(new File(dir, "target/ports"))
    dir
  }

  private var testEnvironment: Option[Map[String, String]] = None

  val cassandraTestVersion = sys.props.get("test.cassandra.version").getOrElse(Versions.Cassandra)

  private lazy val testJavaOptions = Seq(
    "-Xmx512m",
    s"-Dtest.cassandra.version=$cassandraTestVersion",
    "-Dsun.io.serialization.extendedDebugInfo=true",
    s"-DbaseDir=${mainDir.getAbsolutePath}") ++ uRandomParams

  private val testConfigs = inConfig(Test)(Defaults.testTasks) ++ inConfig(IntegrationTest)(Defaults.itSettings)

  private val testOptionSettings = Seq(
    Tests.Argument(TestFrameworks.ScalaTest, "-oDF"),
    Tests.Argument(TestFrameworks.JUnit, "-oDF", "-v", "-a")
  )

  private val pureTestClasspath = taskKey[Set[String]]("Show classpath which is obtained as (test:fullClasspath + it:fullClasspath) - compile:fullClasspath")

  lazy val testTasks = Seq(
    pureTestClasspath := {
      val testDeps = (fullClasspath in Test value) map (_.data.getAbsolutePath) toSet
      val itDeps = (fullClasspath in IntegrationTest value) map (_.data.getAbsolutePath) toSet
      val compileDeps = (fullClasspath in Compile value) map (_.data.getAbsolutePath) toSet

      val cp = (testDeps ++ itDeps) -- compileDeps

      println("TEST_CLASSPATH=" + cp.mkString(File.pathSeparator))

      cp
    }
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
      Testing.testEnvironment = Some(env)
      env
    }
  )

  private def makeTestGroups(tests: Seq[TestDefinition]): Seq[Group] = {
    // if we have many C* instances and we can run multiple tests in parallel, then group by package name
    // additional groups for auth and ssl is just an optimisation
    def multiCInstanceGroupingFunction(test: TestDefinition): String = {
      if (test.name.toLowerCase.contains("auth")) "auth"
      else if (test.name.toLowerCase.contains("ssl")) "ssl"
      else if (test.name.contains("CustomFromDriverSpec")) "customdriverspec"
      else if (test.name.contains("CETSpec") || test.name.contains("CETTest")) "cetspec"
      else if (test.name.contains("PSTSpec") || test.name.contains("PSTTest")) "pstspec"
      else if (test.name.contains("Connector")) "connector"
      else test.name.reverse.dropWhile(_ != '.').reverse
    }

    // if we have a single C* create as little groups as possible to avoid restarting C*
    // the minimum - we need to run REPL and streaming tests in separate processes
    // additional groups for auth and ssl is just an optimisation
    // A new group is made for CustomFromDriverSpec because the ColumnType needs to be
    // Initilized afresh
    def singleCInstanceGroupingFunction(test: TestDefinition): String = {
      val pkgName = test.name.reverse.dropWhile(_ != '.').reverse
      if (test.name.toLowerCase.contains("authenticate")) "auth"
      else if (test.name.toLowerCase.contains("ssl")) "ssl"
      else if (pkgName.contains(".repl")) "repl"
      else if (pkgName.contains(".streaming")) "streaming"
      else if (test.name.contains("CustomFromDriverSpec")) "customdriverspec"
      else if (test.name.contains("CETSpec") || test.name.contains("CETTest")) "cetspec"
      else if (test.name.contains("PSTSpec") || test.name.contains("PSTTest")) "pstspec"
      else if (test.name.contains("Connector")) "connector"
      else "other"
    }

    val groupingFunction = if (parallelTasks == 1)
      singleCInstanceGroupingFunction _ else multiCInstanceGroupingFunction _

    tests.groupBy(groupingFunction).map { case (pkg, testsSeq) =>
      new Group(
        name = pkg,
        tests = testsSeq,
        runPolicy = SubProcess(ForkOptions(
          runJVMOptions = testJavaOptions,
          envVars = testEnvironment.getOrElse(sys.env),
          outputStrategy = Some(StdoutOutput))))
    }.toSeq
  }

  private lazy val testArtifacts = Seq(
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

  lazy val testSettings = testConfigs ++ testArtifacts ++ Seq(
    parallelExecution in Test := true,
    parallelExecution in IntegrationTest := true,
    javaOptions in IntegrationTest ++= testJavaOptions,
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

}