# Spark Cassandra Connector [![Build Status](https://travis-ci.org/datastax/spark-cassandra-connector.svg)](http://travis-ci.org/datastax/spark-cassandra-connector)
### [Spark Cassandra Connector Spark Packages Website](http://spark-packages.org/package/datastax/spark-cassandra-connector)

[![Join the chat at https://gitter.im/datastax/spark-cassandra-connector](https://badges.gitter.im/datastax/spark-cassandra-connector.svg)](https://gitter.im/datastax/spark-cassandra-connector?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

### Most Recent Release Scala Docs

### 2.0.0
* [Spark-Cassandra-Connector](http://datastax.github.io/spark-cassandra-connector/ApiDocs/2.0.0/spark-cassandra-connector/)
* [Embedded-Cassandra](http://datastax.github.io/spark-cassandra-connector/ApiDocs/2.0.0/spark-cassandra-connector-embedded/)

[All Versions API Docs](#hosted-api-docs)

## Lightning-fast cluster computing with Spark and Cassandra

This library lets you expose Cassandra tables as Spark RDDs, write Spark RDDs to Cassandra tables, and
execute arbitrary CQL queries in your Spark applications.

## Features

 - Compatible with Apache Cassandra version 2.0 or higher (see table below)
 - Compatible with Apache Spark 1.0 through 2.0 (see table below)
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
 - Works with PySpark DataFrames

## Version Compatibility

The connector project has several branches, each of which map into different supported versions of 
Spark and Cassandra. Refer to the compatibility table below which shows the major.minor 
version range supported between the connector, Spark, Cassandra, and the Cassandra Java driver:

| Connector | Spark         | Cassandra | Cassandra Java Driver |
| --------- | ------------- | --------- | --------------------- |
| 2.0       | 2.0           | 2.1.5*, 2.2, 3.0  | 3.0           |
| 1.6       | 1.6           | 2.1.5*, 2.2, 3.0  | 3.0           |
| 1.5       | 1.5, 1.6      | 2.1.5*, 2.2, 3.0  | 3.0           |
| 1.4       | 1.4           | 2.1.5*    | 2.1                   |
| 1.3       | 1.3           | 2.1.5*    | 2.1                   |
| 1.2       | 1.2           | 2.1, 2.0  | 2.1                   |
| 1.1       | 1.1, 1.0      | 2.1, 2.0  | 2.1                   |
| 1.0       | 1.0, 0.9      | 2.0       | 2.0                   |

**Compatible with 2.1.X where X >= 5*

## Hosted API Docs
API documentation for the Scala and Java interfaces are available online:

### 2.0.0
* [Spark-Cassandra-Connector](http://datastax.github.io/spark-cassandra-connector/ApiDocs/2.0.0/spark-cassandra-connector/)
* [Embedded-Cassandra](http://datastax.github.io/spark-cassandra-connector/ApiDocs/2.0.0/spark-cassandra-connector-embedded/)

### 1.6.0
* [Spark-Cassandra-Connector](http://datastax.github.io/spark-cassandra-connector/ApiDocs/1.6.0/spark-cassandra-connector/)
* [Embedded-Cassandra](http://datastax.github.io/spark-cassandra-connector/ApiDocs/1.6.0/spark-cassandra-connector-embedded/)

### 1.5.0
* [Spark-Cassandra-Connector](http://datastax.github.io/spark-cassandra-connector/ApiDocs/1.5.0/spark-cassandra-connector/)
* [Spark-Cassandra-Connector-Java](http://datastax.github.io/spark-cassandra-connector/ApiDocs/1.5.0/spark-cassandra-connector-java/)
* [Embedded-Cassandra](http://datastax.github.io/spark-cassandra-connector/ApiDocs/1.5.0/spark-cassandra-connector-embedded/)

### 1.4.2
* [Spark-Cassandra-Connector](http://datastax.github.io/spark-cassandra-connector/ApiDocs/1.4.2/spark-cassandra-connector/)
* [Spark-Cassandra-Connector-Java](http://datastax.github.io/spark-cassandra-connector/ApiDocs/1.4.2/spark-cassandra-connector-java/)
* [Embedded-Cassandra](http://datastax.github.io/spark-cassandra-connector/ApiDocs/1.4.2/spark-cassandra-connector-embedded/)

### 1.3.1
* [Spark-Cassandra-Connector](http://datastax.github.io/spark-cassandra-connector/ApiDocs/1.3.1/spark-cassandra-connector/)
* [Spark-Cassandra-Connector-Java](http://datastax.github.io/spark-cassandra-connector/ApiDocs/1.3.1/spark-cassandra-connector-java/)
* [Embedded-Cassandra](http://datastax.github.io/spark-cassandra-connector/ApiDocs/1.3.1/spark-cassandra-connector-embedded/)

### 1.2.0 
* [Spark-Cassandra-Connector](http://datastax.github.io/spark-cassandra-connector/ApiDocs/1.2.0/spark-cassandra-connector/)
* [Spark-Cassandra-Connector-Java](http://datastax.github.io/spark-cassandra-connector/ApiDocs/1.2.0/spark-cassandra-connector-java/)
* [Embedded-Cassandra](http://datastax.github.io/spark-cassandra-connector/ApiDocs/1.2.0/spark-cassandra-connector-embedded/)

## Download
This project is available on Spark Packages; this is the easiest way to start using the connector:
http://spark-packages.org/package/datastax/spark-cassandra-connector

This project has also been published to the Maven Central Repository.
For SBT to download the connector binaries, sources and javadoc, put this in your project 
SBT config:
                                                                                                                           
    libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.0-M3"

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
  - [The spark-cassandra-connector-embedded Artifact](doc/10_embedded.md)
  - [Performance monitoring](doc/11_metrics.md)
  - [Building And Artifacts](doc/12_building_and_artifacts.md)
  - [The Spark Shell](doc/13_spark_shell.md)
  - [DataFrames](doc/14_data_frames.md)
  - [Python](doc/15_python.md)
  - [Partitioner](doc/16_partitioning.md)
  - [Frequently Asked Questions](doc/FAQ.md)
  - [Configuration Parameter Reference Table](doc/reference.md)
  - [Tips for Developing the Spark Cassandra Connector](doc/developers.md)

## Online Training
### DataStax Academy
DataStax Academy provides free online training for Apache Cassandra and DataStax Enterprise. In [DS320: Analytics with Spark](https://academy.datastax.com/courses/ds320-analytics-with-apache-spark), you will learn how to effectively and efficiently solve analytical problems with Apache Spark, Apache Cassandra, and DataStax Enterprise. You will learn about Spark API, Spark-Cassandra Connector, Spark SQL, Spark Streaming, and crucial performance optimization techniques.

## Community
### Reporting Bugs
New issues may be reported using [JIRA](https://datastax-oss.atlassian.net/browse/SPARKC/). Please include
all relevant details including versions of Spark, Spark Cassandra Connector, Cassandra and/or DSE. A minimal
reproducible case with sample code is ideal.

### Mailing List
Questions and requests for help may be submitted to the [user mailing list](http://groups.google.com/a/lists.datastax.com/forum/#!forum/spark-connector-user).

### IRC
\#spark-cassandra-connector on irc.freenode.net. If you are new to IRC, you can use a [web-based client](http://webchat.freenode.net/?channels=#spark-cassandra-connector).

### Gitter
[![Join the chat at https://gitter.im/datastax/spark-cassandra-connector](https://badges.gitter.im/datastax/spark-cassandra-connector.svg)](https://gitter.im/datastax/spark-cassandra-connector?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)


### Contributing
To develop this project, we recommend using IntelliJ IDEA. 
Make sure you have installed and enabled the Scala Plugin.
Open the project with IntelliJ IDEA and it will automatically create the project structure
from the provided SBT configuration.

[Tips for Developing the Spark Cassandra Connector](doc/developers.md)

Before contributing your changes to the project, please make sure that all unit tests and integration tests pass.
Don't forget to add an appropriate entry at the top of CHANGES.txt.
Create a Jira at the [Spark Cassandra Connector Jira](https://datastax-oss.atlassian.net/projects/SPARKC/issues)
Finally open a pull-request on GitHub and await review. 

Please prefix pull request description with the JIRA number, for example: "SPARKC-123: Fix the ...".

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
    
## Generating Documents
To generate the Reference Document use 

    ./sbt/sbt spark-cassandra-connector-unshaded/run (outputLocation)
    
outputLocation defaults to doc/reference.md

## License

Copyright 2014-2016, DataStax, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
