name := "cassandra-driver-spark"

organization := "com.datastax.cassandra"

version := "0.9.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "com.codahale.metrics" % "metrics-json" % "3.0.2",
  "com.datastax.cassandra" % "cassandra-driver-core" % "2.0.2",
  "com.google.guava" % "guava" % "16.0.1",
  "com.ning" % "compress-lzf" % "1.0.1",
  "com.typesafe" % "config" % "1.2.1",
  "com.typesafe.akka" %% "akka-actor" % "2.2.3",
  "com.typesafe.akka" %% "akka-remote" % "2.2.3",
  "com.typesafe.akka" %% "akka-slf4j" % "2.2.3",
  "joda-time" % "joda-time" % "2.3",
  "org.apache.cassandra" % "cassandra-thrift" % "2.0.8",
  "org.apache.cassandra" % "cassandra-clientutil" % "2.0.8",
  "org.apache.spark" %% "spark-core" % "0.9.1",
  "org.joda" % "joda-convert" % "1.2",
  "org.scala-lang" % "scala-reflect" % "2.10.4",
  "org.slf4j" % "slf4j-api" % "1.7.7",
  "junit" % "junit" % "4.11" % "test"
)

