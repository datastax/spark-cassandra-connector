# Documentation

## The `spark-cassandra-connector-embedded` Artifact

The `spark-cassandra-connector-embedded` artifact can be used as a test 
or prototype dependency to spin up embedded servers for testing ideas, 
quickly learning, integration, etc.

Pulling this dependency in allows you to:

- Run Integration Tests (IT) tests with an embedded Cassandra instance
  - If your sbt project is configured to run IT configs
- Easily write and run a Spark Streaming app using 
  - Apache Kafka streams (including an embedded Zookeeper), all with no Ops work involved
  - And of course Cassandra but you currently need to spin up a local instance: [Download Cassandra latest](https://cassandra.apache.org/download/), open the tar, and run `sudo ./apache-cassandra-*/bin/cassandra`

## The Code
See: [https://github.com/datastax/spark-cassandra-connector/tree/master/spark-cassandra-connector-embedded/src/main/scala/com/datastax/spark/connector/embedded](https://github.com/datastax/spark-cassandra-connector/tree/master/spark-cassandra-connector-embedded/src/main/scala/com/datastax/spark/connector/embedded)

## How To Add The Dependency

Simply add this to your SBT build, or in the appropriate format for a Maven build:

    "com.datastax.spark"  %% "spark-cassandra-connector-embedded" % {latest.version}
    
## Examples
[Spark Build Examples](https://github.com/datastax/SparkBuildExamples)

[Next - Performance Monitoring](11_metrics.md)
