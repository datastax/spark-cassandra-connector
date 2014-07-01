# Lightning-fast cluster computing with Spark and Cassandra

This library allows creating Spark applications that read/write data from/to Cassandra.

Features:
 - Exposes Cassandra tables as Spark RDDs 
 - Map table rows to CassandraRow objects or tuples
 - Customizable object mapper for mapping rows to objects of user-defined classes
 - Save RDDs back to Cassandra by implicit `saveToCassandra` call
 - Data type conversions between Cassandra and Scala
 - Support for all Cassandra data types including collections
 - Server-side row filtering via CQL `WHERE` clause 
 - Optimizations for Cassandra Virtual Nodes    

## Building
You need to install SBT version 0.13 or newer to build spark-cassandra-driver.
In the project root directory run:

    sbt package
    
The library package jar will be placed in `target/scala-2.10/`    

## Setting up IntelliJ IDEA project
Make sure you have installed and enabled 
the Scala Plugin from [here] (http://confluence.jetbrains.com/display/SCA/Scala+Plugin+for+IntelliJ+IDEA).

To download the required dependencies and setup project files, in the project root directory, run: 

    sbt gen-idea        
     
## 5-minutes quick start guide
In this tutorial, you'll learn how to setup a basic Spark application reading and writing data from/to Cassandra.

### Prerequisites
Spark-Cassandra driver requires the following components:

 - Apache Spark 0.9.1
 - Apache Cassandra thrift and clientutil libraries matching the version of Cassandra  
 - DataStax Cassandra java driver for your Cassandra version 
 
This driver does not require full cassandra-all.jar nor any dependencies of the Cassandra server. 
For details, see project dependencies in the `build.sbt` file.

Put the spark-cassandra-driver jar and its dependency jars:

 - on the classpath of your project.
 - on the classpath of your Spark cluster nodes or use `sc.addJar`
 
### Setting up `SparkContext`   
Before creating the `SparkContext`, set the `cassandra.connection.host` property:
   
    val conf = new SparkConf(true)
       .set("cassandra.connection.host", "127.0.0.1")
       
Create the `SparkContext` as usual:
     
    val sc = new SparkContext("spark://127.0.0.1:7077", "test", conf)

To enable Cassandra-specific functions:
     
    import com.datastax.driver.spark._

### Loading and analyzing data from Cassandra
Assume there is a Cassandra table in keyspace `keyspace` named `table`.
Use `sc.cassandraTable` method to view this table as Spark `RDD`:

    val rdd = sc.cassandraTable("keyspace", "table")
    println(rdd.count)
    println(rdd.first)

### Saving data from RDD to Cassandra

Assume there is a Cassandra table in keyspace `keyspace` named `table` with 
at least two columns named `column1` and `column2` of type `text`. 
To save a collection of String pairs to this table:
                                     
    val collection = sc.parallelize(Seq(("key1", "value1"), ("key2", "value2")))
    collection.saveToCassandra("keyspace", "table", Seq("column1", "column2"))
    