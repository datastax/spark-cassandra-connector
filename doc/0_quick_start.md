# Documentation

## 5-minute quick start guide

In this tutorial, you'll learn how to setup a very simple Spark application for reading and writing data from/to Cassandra.
Before you start, you need to have basic knowledge of Apache Cassandra and Apache Spark.
Refer to [Cassandra documentation](http://www.datastax.com/documentation/cassandra/2.0/cassandra/gettingStartedCassandraIntro.html) 
and [Spark documentation](https://spark.apache.org/docs/0.9.1/). 

### Prerequisites

Install and launch a Cassandra 2.0 cluster and a Spark cluster.   

Configure a new Scala project with the following dependencies: 

 - Apache Spark 0.9 or 1.0 and its dependencies
 - Apache Cassandra thrift and clientutil libraries matching the version of Cassandra  
 - DataStax Cassandra driver for your Cassandra version 
 
This driver does not depend on the Cassandra server code.   
For a detailed dependency list, see project dependencies in the `project/CassandraSparkBuild.scala` file.

Add the cassandra-driver-spark.jar and its dependency jars to the following classpaths:

 - the classpath of your project
 - the classpath of every Spark cluster node
 
This driver is also compatible with Spark distribution provided in 
[DataStax Enterprise 4.5](http://www.datastax.com/documentation/datastax_enterprise/4.5/datastax_enterprise/newFeatures.html).
 
### Preparing example Cassandra schema
Create a simple keyspace and table in Cassandra. Run the following statements in `cqlsh`:

```sql
CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };
CREATE TABLE test.kv(key text PRIMARY KEY, value int);
```
      
Then insert some example data:

```sql
INSERT INTO test.kv(key, value) VALUES ('key1', 1);
INSERT INTO test.kv(key, value) VALUES ('key2', 2);
```
 
Now you're ready to write your first Spark program using Cassandra.

### Setting up `SparkContext`   
Before creating the `SparkContext`, set the `spark.cassandra.connection.host` property to the address of one 
of the Cassandra nodes:

```scala
val conf = new SparkConf(true)
   .set("spark.cassandra.connection.host", "127.0.0.1")
```
       
Create a `SparkContext`. Substitute `127.0.0.1` with the actual address of your Spark Master
(or use `"local"` to run in local mode): 

```scala
val sc = new SparkContext("spark://127.0.0.1:7077", "test", conf)
```

Enable Cassandra-specific functions on the `SparkContext` and `RDD`:

```scala
import com.datastax.spark.connector._
```

### Setting up `StreamingContext`
Follow the directions above for creating a `SparkConf`

Create a `StreamingContext`:

```scala
val ssc = new StreamingContext(conf, Seconds(n))
```

Enable Cassandra-specific functions on the `StreamingContext`, `DStream` and `RDD`:

```scala
import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
```

Create any of the available or custom Spark streams, for example an Akka Actor stream:

```scala
val stream = ssc.actorStream[String](Props[SimpleActor], actorName, StorageLevel.MEMORY_AND_DISK)
```

Writing to Cassandra from a Stream:

```scala
val wc = stream.flatMap(_.split("\\s+"))
    .map(x => (x, 1))
    .reduceByKey(_ + _)
    .saveToCassandra("streaming_test", "words", Seq("word", "count"))
```

Where `saveToCassandra` accepts

- keyspaceName: String, tableName: String
- keyspaceName: String, tableName: String, columnNames: Seq[String]
- keyspaceName: String, tableName: String, columnNames: Seq[String], batchSize: Option[Int]

### Loading and analyzing data from Cassandra
Use the `sc.cassandraTable` method to view this table as a Spark `RDD`:

```scala
val rdd = sc.cassandraTable("test", "kv")
println(rdd.count)
println(rdd.first)
println(rdd.map(_.getInt("value")).sum)        
```

### Saving data from RDD to Cassandra  
Add two more rows to the table:

```scala
val collection = sc.parallelize(Seq(("key3", 3), ("key4", 4)))
collection.saveToCassandra("test", "kv", Seq("key", "value"))       
```


[Next - Connecting to Cassandra](1_connecting.md)
