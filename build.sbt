name := "cassandra-driver-spark"

organization := "com.datastax.cassandra"

version := "0.9.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.cassandra" % "cassandra-thrift" % "2.0.8",
  "com.datastax.cassandra" % "cassandra-driver-core" % "2.0.2",
  "org.apache.spark" %% "spark-core" % "0.9.1"
)

