# Documentation

## 5-minute quick start guide

In this tutorial, you'll learn how to setup a very simple Spark application for reading and writing data from/to Cassandra.
Before you start, you need to have basic knowledge of Apache Cassandra and Apache Spark.
Refer to [Cassandra documentation](http://www.datastax.com/documentation/cassandra/2.0/cassandra/gettingStartedCassandraIntro.html) 
and [Spark documentation](https://spark.apache.org/docs/0.9.1/). 

### Prerequisites

Install and launch a Cassandra cluster and a Spark cluster.   

Configure a new Scala project with the following dependencies:

 - Apache Spark and its dependencies
 - Apache Cassandra thrift and clientutil libraries matching the version of Cassandra  
 - DataStax Cassandra driver for your Cassandra version 
 
This driver does not depend on the Cassandra server code.

 - For a detailed dependency list, see [project/CassandraSparkBuild.scala](../project/CassandraSparkBuild.scala)
 - For dependency versions, see [project/Versions.scala](../project/Versions.scala)

Add the `spark-cassandra-connector` jar and its dependency jars to the following classpaths.
**Make sure the Connector version you use coincides with your Spark version (i.e. Spark 1.2.x with Connector 1.2.x)**:

    "com.datastax.spark" %% "spark-cassandra-connector" % Version

 - the classpath of your project
 - the classpath of every Spark cluster node

### Building
See [Building And Artifacts](12_building_and_artifacts.md)

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
collection.saveToCassandra("test", "kv", SomeColumns("key", "value"))       
```

[Next - Connecting to Cassandra](1_connecting.md)
