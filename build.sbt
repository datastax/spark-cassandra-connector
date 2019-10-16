import com.timushev.sbt.updates.UpdatesPlugin.autoImport.dependencyUpdatesFilter
import sbt.moduleFilter
// factor out common settings
ThisBuild / scalaVersion := "2.11.12"
ThisBuild / scalacOptions += "-target:jvm-1.8"

ThisBuild / organization := "com.datastax.dse"
ThisBuild / publishMavenStyle := true
ThisBuild / pomExtra := Publishing.License
ThisBuild / publishTo := Publishing.Repository
ThisBuild / credentials ++= Publishing.Credentials
ThisBuild / version := Publishing.Version

Global / resolvers ++= Seq(
  DefaultMavenRepository,
  Resolver.sonatypeRepo("public"),
  "DataStax Repo" at "https://repo.datastax.com/dse"
)

lazy val IntegrationTest = config("it") extend Test

lazy val integrationTestsWithFixtures = taskKey[Map[TestDefinition, Seq[String]]]("Evaluates names of all " +
  "Fixtures sub-traits for each test. Sets of fixture sub-traits names are used to form group tests.")

lazy val commonSettings = Seq(
  // dependency updates check
  dependencyUpdatesFailBuild := true,
  dependencyUpdatesFilter -= moduleFilter(organization = "org.scala-lang" | "org.eclipse.jetty"),
  fork := true,
  parallelExecution := true,
  testForkedParallel := false,
  testOptions += Tests.Argument(TestFrameworks.JUnit, "-v")
)

val annotationProcessor = Seq(
  "-processor", "com.datastax.oss.driver.internal.mapper.processor.MapperProcessor"
)

lazy val root = (project in file("."))
  .aggregate(connector, testSupport, driver)
  .settings(
    publish / skip := true
  )

lazy val connector = (project in file("connector"))
  .configs(IntegrationTest)
  .settings(Defaults.itSettings: _*) //This and above enables the "it" suite
  .settings(commonSettings)
  .settings(
    // set the name of the project
    name := "dse-spark-connector",

    javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),

    // test grouping
    integrationTestsWithFixtures := {
      Testing.testsWithFixtures((testLoader in IntegrationTest).value, (definedTests in IntegrationTest).value)
    },

    IntegrationTest / testGrouping := Testing.makeTestGroups(integrationTestsWithFixtures.value),
    IntegrationTest / testOptions += Tests.Argument("-oF"),  // show full stack traces

    Test / javacOptions ++= annotationProcessor ++ Seq("-d", (classDirectory in Test).value.toString),

    Global / concurrentRestrictions := Seq(Tags.limitAll(Testing.parallelTasks)),

    libraryDependencies ++= Dependencies.Spark.dependencies
      ++ Dependencies.TestConnector.dependencies
      ++ Dependencies.Jetty.dependencies
  )
  .dependsOn(
    testSupport % "test",
    driver
  )

lazy val testSupport = (project in file("test-support"))
  .settings(commonSettings)
  .settings(
    name := "dse-spark-connector-test-support",
    libraryDependencies ++= Dependencies.TestSupport.dependencies
  )

lazy val driver = (project in file("driver"))
  .settings(commonSettings)
  .settings(
    name := "dse-spark-connector-driver",
    libraryDependencies ++= Dependencies.Driver.dependencies
      ++ Dependencies.TestDriver.dependencies
  )