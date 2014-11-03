#!/bin/sh

sbt twitter/assembly

JAR="$(pwd)/spark-cassandra-connector-demos/twitter-streaming/target/scala-2.10/twitter-streaming-assembly-1.1.0-SNAPSHOT.jar"
echo "Using assembly jar: $JAR"

#spark-submit --class com.datastax.spark.connector.demo.TwitterStreamingApp --master local $JAR > stream.log


spark-submit \
--class com.datastax.spark.connector.demo.TwitterStreamingApp \
--deploy-mode client \
--master local \
$JAR 10