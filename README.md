# Lightning-fast cluster computing with Spark and Cassandra

This library allows creation of Spark applications that read/write data from/to Cassandra.

## Features

 - Compatible with vanilla Apache Cassandra 2.x and DataStax Enterprise 4.5
 - Compatible with Apache Spark 0.9 and 1.0
 - Exposes Cassandra tables as Spark RDDs 
 - Maps table rows to CassandraRow objects or tuples
 - Offers customizable object mapper for mapping rows to objects of user-defined classes
 - Saves RDDs back to Cassandra by implicit `saveToCassandra` call
 - Converts data types between Cassandra and Scala
 - Supports all Cassandra data types including collections
 - Filters rows on the server side via the CQL `WHERE` clause 
 - Plays nice with Cassandra Virtual Nodes    

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

## Reporting Bugs
Please use GitHub to report feature requests or bugs.  

## Contributing
To contribute back to this project, please open a pull-request on GitHub.   

To develop this project, we recommend using IntelliJ IDEA. 
Make sure you have installed and enabled the Scala Plugin 
from [here] (http://confluence.jetbrains.com/display/SCA/Scala+Plugin+for+IntelliJ+IDEA).
   