# Spark Cassandra Connector [![Build Status](https://travis-ci.org/datastax/spark-cassandra-connector.svg)](https://travis-ci.org/datastax/spark-cassandra-connector)

## Quick Links

| What       | Where |
| ---------- | ----- |
| Packages   | [Spark Cassandra Connector Spark Packages Website](https://spark-packages.org/package/datastax/spark-cassandra-connector) |
| Community  | Chat with us at [DataStax Academy's #spark-connector Slack channel](#slack) |
| Scala Docs | Most Recent Release (2.3.0): [Spark-Cassandra-Connector](https://datastax.github.io/spark-cassandra-connector/ApiDocs/2.3.0/spark-cassandra-connector/), [Embedded-Cassandra](https://datastax.github.io/spark-cassandra-connector/ApiDocs/2.3.0/spark-cassandra-connector-embedded/) |

## Features

*Lightning-fast cluster computing with Apache Spark&trade; and Apache Cassandra&reg;.*

This library lets you expose Cassandra tables as Spark RDDs, write Spark RDDs to Cassandra tables, and
execute arbitrary CQL queries in your Spark applications.

 - Compatible with Apache Cassandra version 2.0 or higher (see table below)
 - Compatible with Apache Spark 1.0 through 2.0 (see table below)
 - Compatible with Scala 2.10 and 2.11
 - Exposes Cassandra tables as Spark RDDs
 - Maps table rows to CassandraRow objects or tuples
 - Offers customizable object mapper for mapping rows to objects of user-defined classes
 - Saves RDDs back to Cassandra by implicit `saveToCassandra` call
 - Delete rows and columns from cassandra by implicit `deleteFromCassandra` call
 - Join with a subset of Cassandra data using `joinWithCassandraTable` call
 - Partition RDDs according to Cassandra replication using `repartitionByCassandraReplica` call
 - Converts data types between Cassandra and Scala
 - Supports all Cassandra data types including collections
 - Filters rows on the server side via the CQL `WHERE` clause
 - Allows for execution of arbitrary CQL statements
 - Plays nice with Cassandra Virtual Nodes
 - Works with PySpark DataFrames

## Version Compatibility

The connector project has several branches, each of which map into different
supported versions of  Spark and Cassandra. For previous releases the branch is
named "bX.Y" where X.Y is the major+minor version; for example the "b1.6" branch
corresponds to the 1.6 release. The "master" branch will normally contain
development for the next connector release in progress.

| Connector | Spark         | Cassandra | Cassandra Java Driver | Minimum Java Version | Supported Scala Versions |
| --------- | ------------- | --------- | --------------------- | -------------------- | -----------------------  |
| 2.3       | 2.3           | 2.1.5*, 2.2, 3.0 | 3.0                   | 8             | 2.11
| 2.0       | 2.0, 2.1, 2.2 | 2.1.5*, 2.2, 3.0 | 3.0                   | 8             | 2.10, 2.11               |
| 1.6       | 1.6           | 2.1.5*, 2.2, 3.0 | 3.0                   | 7             | 2.10, 2.11               |
| 1.5       | 1.5, 1.6      | 2.1.5*, 2.2, 3.0 | 3.0                   | 7             | 2.10, 2.11               |
| 1.4       | 1.4           | 2.1.5*           | 2.1                   | 7             | 2.10, 2.11               |
| 1.3       | 1.3           | 2.1.5*           | 2.1                   | 7             | 2.10, 2.11               |
| 1.2       | 1.2           | 2.1, 2.0         | 2.1                   | 7             | 2.10, 2.11               |
| 1.1       | 1.1, 1.0      | 2.1, 2.0         | 2.1                   | 7             | 2.10, 2.11               |
| 1.0       | 1.0, 0.9      | 2.0              | 2.0                   | 7             | 2.10, 2.11               |

**Compatible with 2.1.X where X >= 5*

## Hosted API Docs
API documentation for the Scala and Java interfaces are available online:
### 2.3.0
* [Spark-Cassandra-Connector](http://datastax.github.io/spark-cassandra-connector/ApiDocs/2.3.0/spark-cassandra-connector/)
* [Embedded-Cassandra](http://datastax.github.io/spark-cassandra-connector/ApiDocs/2.3.0/spark-cassandra-connector-embedded/)

### 2.0.8
* [Spark-Cassandra-Connector](http://datastax.github.io/spark-cassandra-connector/ApiDocs/2.0.8/spark-cassandra-connector/)
* [Embedded-Cassandra](http://datastax.github.io/spark-cassandra-connector/ApiDocs/2.0.8/spark-cassandra-connector-embedded/)

### 1.6.11
* [Spark-Cassandra-Connector](http://datastax.github.io/spark-cassandra-connector/ApiDocs/1.6.11/spark-cassandra-connector/)
* [Embedded-Cassandra](http://datastax.github.io/spark-cassandra-connector/ApiDocs/1.6.11/spark-cassandra-connector-embedded/)

### 1.5.2
* [Spark-Cassandra-Connector](http://datastax.github.io/spark-cassandra-connector/ApiDocs/1.5.2/spark-cassandra-connector/)
* [Spark-Cassandra-Connector-Java](http://datastax.github.io/spark-cassandra-connector/ApiDocs/1.5.2/spark-cassandra-connector-java/)
* [Embedded-Cassandra](http://datastax.github.io/spark-cassandra-connector/ApiDocs/1.5.0/spark-cassandra-connector-embedded/)

### 1.4.5
* [Spark-Cassandra-Connector](http://datastax.github.io/spark-cassandra-connector/ApiDocs/1.4.5/spark-cassandra-connector/)
* [Spark-Cassandra-Connector-Java](http://datastax.github.io/spark-cassandra-connector/ApiDocs/1.4.5/spark-cassandra-connector-java/)
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
https://spark-packages.org/package/datastax/spark-cassandra-connector

This project has also been published to the Maven Central Repository.
For SBT to download the connector binaries, sources and javadoc, put this in your project
SBT config:

    libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.3.0"

* The default Scala version for Spark 2.0+ is 2.11 please choose the appropriate build. See the
[FAQ](doc/FAQ.md) for more information

## Building
See [Building And Artifacts](doc/12_building_and_artifacts.md)

## Documentation

  - [Quick-start guide](doc/0_quick_start.md)
  - [Connecting to Cassandra](doc/1_connecting.md)
  - [Loading datasets from Cassandra](doc/2_loading.md)
  - [Server-side data selection and filtering](doc/3_selection.md)   
  - [Working with user-defined case classes and tuples](doc/4_mapper.md)
  - [Saving and deleting datasets to/from Cassandra](doc/5_saving.md)
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

Questions and requests for help may be submitted to the [user mailing list](https://groups.google.com/a/lists.datastax.com/forum/#!forum/spark-connector-user).

### Slack

The project uses Slack to facilitate conversation in our community. Find us in the `#spark-connector` channel at [DataStax Academy Slack](https://academy.datastax.com/slack).

## Contributing

To protect the community, all contributors are required to sign the [DataStax Spark Cassandra Connector Contribution License Agreement](http://spark-cassandra-connector-cla.datastax.com/). The process is completely electronic and should only take a few minutes.

To develop this project, we recommend using IntelliJ IDEA. Make sure you have
installed and enabled the Scala Plugin. Open the project with IntelliJ IDEA and
it will automatically create the project structure from the provided SBT
configuration.

[Tips for Developing the Spark Cassandra Connector](doc/developers.md)

Checklist for contributing changes to the project:
* Create a [SPARKC JIRA](https://datastax-oss.atlassian.net/projects/SPARKC/issues)
* Make sure that all unit tests and integration tests pass
* Add an appropriate entry at the top of CHANGES.txt
* If the change has any end-user impacts, also include changes to the ./doc files as needed
* Prefix the pull request description with the JIRA number, for example: "SPARKC-123: Fix the ..."
* Open a pull-request on GitHub and await review

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

Copyright 2014-2017, DataStax, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
