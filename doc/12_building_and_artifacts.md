# Documentation

## Building

### Scala Versions
You can choose to build, assemble and run both Spark and the Spark Cassandra Connector against Scala 2.10 or 2.11.

#### Scala 2.11
Running SBT with the following builds against Scala 2.11 and the generated artifact paths coincide with
the `binary.version` thereof:

    sbt -Dscala-2.11=true

For Spark see: [Building Spark for Scala 2.11](http://spark.apache.org/docs/1.2.0/building-spark.html)

For Scala 2.11 tasks:

    sbt -Dscala-2.11=true doc
    sbt -Dscala-2.11=true package
    sbt -Dscala-2.11=true assembly

**Note:** The Spark Java API is currently not buildable with Scala 2.11; see [SPARKC-130](https://datastax-oss.atlassian.net/browse/SPARKC-130).

#### Scala 2.10
To use Scala 2.10 nothing extra is required.

#### Version Cross Build
This produces artifacts for both versions:
Start SBT:

     sbt -Dscala-2.11=true

Run in the SBT shell:

     + package


### Building The Assembly Jar
In the root directory run

    sbt assembly

To build the assembly jar against Scala 2.11:

     sbt -Dscala-2.11=true assembly

A fat jar will be generated to both of these directories:
   - `spark-cassandra-connector/target/scala-{binary.version}/`
   - `spark-cassandra-connector-java/target/scala-{binary.version}/`

Select the former for Scala apps, the later for Java.

### Building General Artifacts
All artifacts are generated to the standard output directories based on the Scala binary version you use.

In the root directory run:

    sbt package
    sbt doc

The library package jars will be generated to:
  - `spark-cassandra-connector/target/scala-{binary.version}/`
  - `spark-cassandra-connector-java/target/scala-{binary.version}/`

The documentation will be generated to:
  - `spark-cassandra-connector/target/scala-{binary.version}/api/`
  - `spark-cassandra-connector-java/target/scala-{binary.version}/api/`

#### Using the Assembly Jar With Spark Submit
The easiest way to do this is to make the assembled connector jar using

     sbt assembly

Remember that if you need to build the assembly jar against Scala 2.11:

     sbt -Dscala-2.11=true assembly

This will generate a jar file with all of the required dependencies in

     spark-cassandra-connector/spark-cassandra-connector/target/scala-{binary.version}/spark-cassandra-connector-assembly-*.jar

Then add this jar to your Spark executor classpath by adding the following line to your spark-default.conf

     spark.executor.extraClassPath  spark-cassandra-connector/spark-cassandra-connector/target/scala-{binary.version}/spark-cassandra-connector-assembly-$CurrentVersion-SNAPSHOT.jar

This driver is also compatible with Spark distribution provided in
[DataStax Enterprise](http://datastax.com/docs/latest-dse/).

[Next - The Spark Shell](13_spark_shell.md)    
