# Lightning-fast cluster computing with Spark and Cassandra

This library lets you expose Cassandra tables as Spark RDDs, write Spark RDDs to Cassandra tables, and 
execute arbitrary CQL queries in your Spark applications.

## Features

 - Compatible with Apache Cassandra version 2.0 or higher and DataStax Enterprise 4.5
 - Compatible with Apache Spark 0.9 and 1.0
 - Exposes Cassandra tables as Spark RDDs 
 - Maps table rows to CassandraRow objects or tuples
 - Offers customizable object mapper for mapping rows to objects of user-defined classes
 - Saves RDDs back to Cassandra by implicit `saveToCassandra` call
 - Converts data types between Cassandra and Scala
 - Supports all Cassandra data types including collections
 - Filters rows on the server side via the CQL `WHERE` clause 
 - Allows for execution of arbitrary CQL statements
 - Plays nice with Cassandra Virtual Nodes

## Download
This project has been published to the Maven Central Repository.
For SBT to download the connector binaries, sources and javadoc, put this in your project 
SBT config:
                                                                                                                           
    libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "1.0.0-beta2" withSources() withJavadoc()

## Building
You need to install SBT version 0.13 or newer to build this project.
In the project root directory run:

    sbt package
    sbt doc
    
The library package jar will be placed in `target/scala-2.10/`
The documentation will be generated to `target/scala-2.10/api/`    
     
## Documentation

  - [Quick-start guide](doc/0_quick_start.md)
  - [Connecting to Cassandra](doc/1_connecting.md)
  - [Loading datasets from Cassandra](doc/2_loading.md)
  - [Server-side data selection and filtering](doc/3_selection.md)   
  - [Working with user-defined case classes and tuples](doc/4_mapper.md)
  - [Saving datasets to Cassandra](doc/5_saving.md)
  - [Customizing the object mapping](doc/6_advanced_mapper.md)
  - [Using Connector in Java](doc/7_java_api.md)
    
## License
This software is available under the [Apache License, Version 2.0](LICENSE).    

## Reporting Bugs
Please use GitHub to report feature requests or bugs.  

## Contributing
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

    sbt test
    sbt it:test

By default, integration tests start up a separate, single Cassandra instance and run Spark in local mode.
It is possible to run integration tests with your own Cassandra and/or Spark cluster.
First, prepare a jar with testing code:
    
    sbt test:package
    
Then copy the generated test jar to your Spark nodes and run:    

    export IT_TEST_CASSANDRA_HOST=<IP of one of the Cassandra nodes>
    export IT_TEST_SPARK_MASTER=<Spark Master URL>
    sbt it:test    
