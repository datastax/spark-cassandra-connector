#!/bin/sh

# $1 if empty or 1=sbt, else if 2=dse

JAR="$(pwd)/spark-cassandra-connector-demos/twitter-stream/target/scala-2.10/twitter-stream-assembly-1.1.0-SNAPSHOT.jar"
echo "using $JAR"

export SPARK_CLIENT_CLASSPATH=$JAR

if [ $1 -eq 1 ]; then
  sbt twitter-stream/run
else
  nohup dse spark-class com.datastax.spark.connector.demo.TwitterStreamingApp > stream.log &
fi
