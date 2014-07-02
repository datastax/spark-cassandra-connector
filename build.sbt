name := "cassandra-driver-spark"

organization := "com.datastax.cassandra"

version := "1.0.0-SNAPSHOT"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "com.datastax.cassandra" % "cassandra-driver-core" % "2.0.2",
  "com.google.guava" % "guava" % "16.0.1",
  "joda-time" % "joda-time" % "2.3",
  "org.apache.cassandra" % "cassandra-thrift" % "2.0.8",
  "org.apache.cassandra" % "cassandra-clientutil" % "2.0.8",
  "org.apache.spark" %% "spark-core" % "0.9.1" % "provided",
  "org.joda" % "joda-convert" % "1.2",
  "org.scala-lang" % "scala-reflect" % "2.10.4"
)

libraryDependencies ++= Seq(
  "junit" % "junit" % "4.11" % "test"
)

libraryDependencies ++= Seq(
  "com.codahale.metrics" % "metrics-json" % "3.0.2" % "provided",
  "com.ning" % "compress-lzf" % "1.0.1" % "provided",
  "com.typesafe" % "config" % "1.2.1" % "provided",
  "com.typesafe.akka" %% "akka-actor" % "2.2.3" % "provided",
  "com.typesafe.akka" %% "akka-remote" % "2.2.3"% "provided",
  "com.typesafe.akka" %% "akka-slf4j" % "2.2.3" % "provided",
  "org.slf4j" % "slf4j-api" % "1.7.7" % "provided"
)

scalacOptions in (Compile, doc) ++= Seq("-doc-root-content", "rootdoc.txt")

