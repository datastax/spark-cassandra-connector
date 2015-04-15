# Documentation
## The `spark-cassandra-connector-embedded` Artifact
The `spark-cassandra-connector-embedded` artifact can be used as a test or prototype dependency to spin up embedded servers for testing ideas, quickly learning, integration, etc.
Pulling this dependency in allows you to do 

- Integration Tests (IT) tests with an embedded Cassandra instance 
  - if your sbt project is configured to [run IT configs](https://github.com/datastax/spark-cassandra-connector/blob/master/project/Settings.scala#L78-L94)
- Easily write and run a Spark Streaming app using 
  - Apache Kafka streams (including an embedded Zookeeper), all with no Ops work involved
  - Twitter streams (needs the 4 auth credentials required by twitter)
  - And of course Cassandra but you currently need to sping up a local instance: [Download Cassandra latest](http://cassandra.apache.org/download/), open the tar, and run `sudo ./apache-cassandra-2.1.0/bin/cassandra`

## The Code
See: [https://github.com/datastax/spark-cassandra-connector/tree/master/spark-cassandra-connector-embedded/src/main/scala/com/datastax/spark/connector/embedded](https://github.com/datastax/spark-cassandra-connector/tree/master/spark-cassandra-connector-embedded/src/main/scala/com/datastax/spark/connector/embedded)

## How To Add The Dependency
Simply add this to your SBT build, or in the appropriate format for a Maven build

    "com.datastax.spark"  %% "spark-cassandra-connector-embedded" % {latest.verson}

[Next - Performance Monitoring](11_metrics.md)
