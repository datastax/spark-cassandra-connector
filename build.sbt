import scala.concurrent.duration._

import Tests._


// factor out common settings
ThisBuild / organization := "com.datastax"
ThisBuild / scalaVersion := "2.11.12"
// set the Scala version used for the project
ThisBuild / version      := "0.1.0-SNAPSHOT"

lazy val IntegrationTest = config("it") extend Test

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
    fork in Test := true,
    fork in IntegrationTest := true,

    parallelExecution := true,
    parallelExecution in Test := true,
    parallelExecution in IntegrationTest := true,

    testGrouping in IntegrationTest := Testing.makeTestGroups( (definedTests in IntegrationTest).value),

    concurrentRestrictions in Global += Tags.limit(Tags.Test, Testing.parallelTasks),

    libraryDependencies ++=
      Dependencies.Spark.spark
        ++ Dependencies.DataStax.dataStax
        ++ Dependencies.Test.testDeps
        ++ Dependencies.Solr.solr
        ++ Dependencies.Jetty.jetty
        ++ Dependencies.Embedded.embedded
  )
  .dependsOn(ccm % "test")

lazy val ccm = (project in file ("ccm"))






