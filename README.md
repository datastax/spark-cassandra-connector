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
In this tutorial, you'll learn how to setup a very simple Spark application reading and writing data from/to Cassandra.
Before you start, you need to have basic knowledge of Apache Cassandra and Apache Spark.
See [Cassandra documentation](http://www.datastax.com/documentation/cassandra/2.0/cassandra/gettingStartedCassandraIntro.html) 
and [Spark documentation](https://spark.apache.org/docs/0.9.1/). 

### Prerequisites
Install and launch a Cassandra 2.0 cluster and a Spark cluster.   

Configure a new Scala project with the following dependencies: 

 - Apache Spark 0.9.1 and its dependencies
 - Apache Cassandra thrift and clientutil libraries matching the version of Cassandra  
 - DataStax Cassandra java driver for your Cassandra version 
 
This driver does not require full cassandra-all.jar nor any dependencies of the Cassandra server. 
For a detailed dependency list, see project dependencies in the `build.sbt` file.

Put the spark-cassandra-driver jar and its dependency jars:

 - on the classpath of your project.
 - on the classpath of your Spark cluster nodes or use `sc.addJar`
 
This driver is also compatible with Spark distribution provided in DataStax Enterprise 4.5.
 
### Preparing example Cassandra schema
Create a simple keyspace and table in Cassandra. Run the following statements in `cqlsh`:
    
    CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };
    CREATE TABLE test.kv(key text PRIMARY KEY, value int);
      
Then insert some example data:

    INSERT INTO test.kv(key, value) VALUES ('key1', 1)
    INSERT INTO test.kv(key, value) VALUES ('key2', 2)        
 
Now you're ready to write your first Spark program using Cassandra.

### Setting up `SparkContext`   
Before creating the `SparkContext`, set the `cassandra.connection.host` property to the address of one 
of the Cassandra nodes:
   
    val conf = new SparkConf(true)
       .set("cassandra.connection.host", "127.0.0.1")
       
Create a `SparkContext`. Substitute `127.0.0.1` with the actual address of your Spark Master
(or use `"local"` to run in local mode): 
     
    val sc = new SparkContext("spark://127.0.0.1:7077", "test", conf)

Enable Cassandra-specific functions on `SparkContext` and `RDD`:
     
    import com.datastax.driver.spark._

### Loading and analyzing data from Cassandra
Use `sc.cassandraTable` method to view this table as Spark `RDD`:

    val rdd = sc.cassandraTable("test", "kv")
    println(rdd.count)
    println(rdd.first)
    println(rdd.map(_.getInt("value")).sum)        

### Saving data from RDD to Cassandra  
Add two more rows to the table:
                                     
    val collection = sc.parallelize(Seq(("key3", 3), ("key4", 4))
    collection.saveToCassandra("test", "kv", Seq("key", "value"))        
    