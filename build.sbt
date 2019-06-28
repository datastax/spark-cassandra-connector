// factor out common settings
ThisBuild / organization := "com.datastax"
ThisBuild / scalaVersion := "2.11.12"
// set the Scala version used for the project
ThisBuild / version      := "0.1.0-SNAPSHOT"

lazy val IntegrationTest = config("it") extend Test

lazy val integrationTestsWithFixtures = taskKey[Map[TestDefinition, Seq[String]]]("Evaluates names of all " +
  "Fixtures sub-traits for each test. Sets of fixture sub-traits names are used to form group tests.")

lazy val root = (project in file("connector"))
  .configs(IntegrationTest)
  .settings(Defaults.itSettings: _*) //This and above enables the "it" suite
  .settings(
    // set the name of the project
    name := "DS Analytics Connector",

    // append several options to the list of options passed to the Java compiler
    javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),

    // append -deprecation to the options passed to the Scala compiler
    scalacOptions += "-deprecation",

    // fork a new JVM for 'run' and 'test:run'
    fork := true,
    parallelExecution := true,
    testForkedParallel := false,

    // test grouping and parallel execution restrictions
    integrationTestsWithFixtures := {
      Testing.testsWithFixtures((testLoader in IntegrationTest).value, (definedTests in IntegrationTest).value)
    },

    IntegrationTest / testGrouping := Testing.makeTestGroups(integrationTestsWithFixtures.value),

    Global / concurrentRestrictions := Seq(Tags.limitAll(Testing.parallelTasks)),

    libraryDependencies ++=
      Dependencies.Spark.spark
        ++ Dependencies.DataStax.dataStax
        ++ Dependencies.Test.testDeps
        ++ Dependencies.Solr.solr
        ++ Dependencies.Jetty.jetty
  )
  .dependsOn(testSupport % "test->test")

lazy val testSupport = (project in file ("test-support"))
  .settings(
    libraryDependencies ++= Seq(
      "org.apache.commons" % "commons-exec" % "1.3" % Test,
      Dependencies.DataStax.driverCore % Test)
  )







