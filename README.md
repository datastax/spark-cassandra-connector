# Spark Cassandra Connector [![Build Status](https://travis-ci.org/datastax/spark-cassandra-connector.svg)](http://travis-ci.org/datastax/spark-cassandra-connector)

## Important notice : Do NOT use GitHub issue tracker!
We are going to disable it and no issues created there will be accessible.

If you have problems and you are looking for help, please use a forum at: https://groups.google.com/a/lists.datastax.com/forum/#!forum/spark-connector-user

If you want to report bugs, improvement or feature requests, please use JIRA at:
https://datastax-oss.atlassian.net/browse/SPARKC

## Lightning-fast cluster computing with Spark and Cassandra

This library lets you expose Cassandra tables as Spark RDDs, write Spark RDDs to Cassandra tables, and
execute arbitrary CQL queries in your Spark applications.

## Features

 - Compatible with Apache Cassandra version 2.0 or higher and DataStax Enterprise 4.5 (see table below)
 - Compatible with Apache Spark 1.0 and 1.1 (see table below)
 - Compatible with Scala 2.10 and 2.11
 - Exposes Cassandra tables as Spark RDDs
 - Maps table rows to CassandraRow objects or tuples
 - Offers customizable object mapper for mapping rows to objects of user-defined classes
 - Saves RDDs back to Cassandra by implicit `saveToCassandra` call
 - Join with a subset of Cassandra data using `joinWithCassandraTable` call
 - Partition RDDs according to Cassandra replication using `repartitionByCassandraReplica` call
 - Converts data types between Cassandra and Scala
 - Supports all Cassandra data types including collections
 - Filters rows on the server side via the CQL `WHERE` clause 
 - Allows for execution of arbitrary CQL statements
 - Plays nice with Cassandra Virtual Nodes

## Version Compatibility

The connector project has several branches, each of which map into different supported versions of Spark and Cassandra. Refer to the compatibility table below which shows the major.minor version range supported between the connector, Spark, Cassandra, and the Cassandra Java driver:

| Connector | Spark         | Cassandra | Cassandra Java Driver |
| --------- | ------------- | --------- | --------------------- |
| 1.2       | 1.2           | 2.1, 2.0  | 2.1                   |
| 1.1       | 1.1, 1.0      | 2.1, 2.0  | 2.1                   |
| 1.0       | 1.0, 0.9      | 2.0       | 2.0                   |


## Download
This project has been published to the Maven Central Repository.
For SBT to download the connector binaries, sources and javadoc, put this in your project 
SBT config:
                                                                                                                           
    libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "1.2.0-rc3"

If you want to access the functionality of Connector from Java, you may want to add also a Java API module:

    libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector-java" % "1.2.0-rc3"

## Building
See [Building And Artifacts](doc/12_building_and_artifacts.md)
 
## Documentation

  - [Quick-start guide](doc/0_quick_start.md)
  - [Connecting to Cassandra](doc/1_connecting.md)
  - [Loading datasets from Cassandra](doc/2_loading.md)
  - [Server-side data selection and filtering](doc/3_selection.md)   
  - [Working with user-defined case classes and tuples](doc/4_mapper.md)
  - [Saving datasets to Cassandra](doc/5_saving.md)
  - [Customizing the object mapping](doc/6_advanced_mapper.md)
  - [Using Connector in Java](doc/7_java_api.md)
  - [Spark Streaming with Cassandra](doc/8_streaming.md)
  - [About The Demos](doc/9_demos.md)
  - [The spark-cassandra-connector-embedded Artifact](doc/10_embedded.md)
  - [Performance monitoring](doc/11_metrics.md)
  - [Building And Artifacts](doc/12_building_and_artifacts.md)
  - [The Spark Shell](doc/13_spark_shell.md)
  - [Frequently Asked Questions](doc/FAQ.md)
    
## Community
### Reporting Bugs
New issues should be reported using [JIRA](https://datastax-oss.atlassian.net/browse/SPARKC/).
Please do not use the built-in GitHub issue tracker.
It is left for archival purposes and it will be disabled soon.

### Mailing List
Questions etc can be submitted to the [user mailing list](http://groups.google.com/a/lists.datastax.com/forum/#!forum/spark-connector-user).

### Contributing
To develop this project, we recommend using IntelliJ IDEA. 
Make sure you have installed and enabled the Scala Plugin.
Open the project with IntelliJ IDEA and it will automatically create the project structure
from the provided SBT configuration.

Before contributing your changes to the project, please make sure that all unit tests and integration tests pass.
Don't forget to add an appropriate entry at the top of CHANGES.txt.
Finally open a pull-request on GitHub and await review. 

If your pull-request is going to resolve some opened issue, please add *Fixes \#xx* at the 
end of each commit message (where *xx* is the number of the issue).

## Testing
To run unit and integration tests:

    ./sbt/sbt test
    ./sbt/sbt it:test

By default, integration tests start up a separate, single Cassandra instance and run Spark in local mode.
It is possible to run integration tests with your own Cassandra and/or Spark cluster.
First, prepare a jar with testing code:
    
    ./sbt/sbt test:package
    
Then copy the generated test jar to your Spark nodes and run:    

    export IT_TEST_CASSANDRA_HOST=<IP of one of the Cassandra nodes>
    export IT_TEST_SPARK_MASTER=<Spark Master URL>
    ./sbt/sbt it:test

## License

Copyright 2014-2015, DataStax, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
