import com.timushev.sbt.updates.UpdatesPlugin.autoImport.dependencyUpdatesFilter
import pl.project13.scala.sbt.JmhPlugin.JmhKeys.Jmh
import sbt.Keys.{libraryDependencies, parallelExecution}
import sbt.{Compile, moduleFilter, _}
import sbtassembly.AssemblyPlugin.autoImport.assembly

lazy val scala211 = "2.11.12"
lazy val scala212 = "2.12.10"
lazy val supportedScalaVersions = List(scala212, scala211)

// factor out common settings
ThisBuild / scalacOptions += "-target:jvm-1.8"

// Publishing Info
ThisBuild / credentials ++= Publishing.Creds
ThisBuild / homepage := Some(url("https://github.com/datastax/spark-cassandra-connector"))
ThisBuild / licenses := List("Apache 2" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt") )
ThisBuild / organization := "com.datastax.spark"
ThisBuild / organizationName := "Datastax"
ThisBuild / organizationHomepage := Some(url("https://www.datastax.com"))
ThisBuild / pomExtra := Publishing.OurDevelopers
ThisBuild / pomIncludeRepository := { _ => false }
ThisBuild / publishMavenStyle := true
ThisBuild / publishTo := Publishing.Repository
ThisBuild / scmInfo := Publishing.OurScmInfo
ThisBuild / version := Publishing.Version

Global / resolvers ++= Seq(
  DefaultMavenRepository,
  Resolver.sonatypeRepo("public")
)

lazy val IntegrationTest = config("it") extend Test

lazy val integrationTestsWithFixtures = taskKey[Map[TestDefinition, Seq[String]]]("Evaluates names of all " +
  "Fixtures sub-traits for each test. Sets of fixture sub-traits names are used to form group tests.")

lazy val assemblySettings = Seq(
  assembly / parallelExecution := false,
  assembly / test := {},
  assembly / assemblyMergeStrategy := {
    case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
    case PathList("META-INF", xs @ _*) => MergeStrategy.last
    case "module-info.class" => MergeStrategy.discard
    case x =>
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      oldStrategy(x)
  },
  assembly / assemblyOption := (assemblyOption in assembly).value.copy(includeScala = false),
  assembly / assemblyShadeRules := {
    Seq(
      ShadeRule.rename("com.typesafe.config.**" -> s"shade.com.datastax.spark.connector.@0").inAll
    )
  },
)

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

def scalacVersionDependantOptions(scalaBinary: String): Seq[String] = scalaBinary match {
  case "2.11" => Seq()
  case "2.12" => Seq("-no-java-comments") //Scala Bug on inner classes, CassandraJavaUtil,
}

lazy val root = (project in file("."))
  .disablePlugins(AssemblyPlugin)
  .aggregate(connector, testSupport, driver, performance, publishableAssembly)
  .settings(
    // crossScalaVersions must be set to Nil on the aggregating project
    crossScalaVersions := Nil,
    publish / skip := true
  )

lazy val connector = (project in file("connector"))
  .configs(IntegrationTest)
  .settings(Defaults.itSettings: _*) //This and above enables the "it" suite
  .settings(commonSettings)
  .settings(assemblySettings)
  .settings(
    crossScalaVersions := supportedScalaVersions,
    name := "spark-cassandra-connector",

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
      ++ Dependencies.Jetty.dependencies,

    scalacOptions in (Compile, doc) ++= scalacVersionDependantOptions(scalaBinaryVersion.value)
  )
  .dependsOn(
    testSupport % "test",
    driver
  )

lazy val testSupport = (project in file("test-support"))
  .disablePlugins(AssemblyPlugin)
  .settings(commonSettings)
  .settings(
    crossScalaVersions := supportedScalaVersions,
    name := "spark-cassandra-connector-test-support",
    libraryDependencies ++= Dependencies.TestSupport.dependencies
  )

lazy val driver = (project in file("driver"))
  .disablePlugins(AssemblyPlugin)
  .settings(commonSettings)
  .settings(
    crossScalaVersions := supportedScalaVersions,
    name := "spark-cassandra-connector-driver",
    libraryDependencies ++= Dependencies.Driver.dependencies
      ++ Dependencies.TestDriver.dependencies
      :+ ("org.scala-lang" % "scala-reflect" % scalaVersion.value)
  )

lazy val performance = (project in file("performance"))
  .configs(IntegrationTest)
  .enablePlugins(JmhPlugin)
  .disablePlugins(AssemblyPlugin)
  .settings(
    crossScalaVersions := supportedScalaVersions,
    publish / skip := true,
    name := "spark-cassandra-connector-performance",
    Jmh / sourceDirectory := (sourceDirectory in Test).value,
    Jmh / classDirectory := (classDirectory in Test).value,
    Jmh / dependencyClasspath := (dependencyClasspath in Test).value,
    Jmh / compile := (compile in Jmh).dependsOn(compile in Test).value,
    Jmh / run := (run in Jmh).dependsOn(Keys.compile in Jmh).evaluated,

    libraryDependencies ++= Dependencies.Spark.dependencies
      ++ Dependencies.TestConnector.dependencies
  )
  .dependsOn(
    connector % "test->it",
    testSupport % "test"
  )

/** Defines an extra artifact published alongside main 'spark-cassandra-connector'.
  * It's an assembled version of the main artifact. It contains all of the dependent classes, some of them
  * are shaded. */
lazy val publishableAssembly = project
  .disablePlugins(AssemblyPlugin)
  .settings(
    crossScalaVersions := supportedScalaVersions,
    name := "spark-cassandra-connector-assembly",
    Compile / packageBin := (assembly in (connector, Compile)).value
  )
