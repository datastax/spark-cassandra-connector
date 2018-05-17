# Documentation

## 5-minute quick start guide

In this tutorial, you'll learn how to setup a very simple Spark application for reading and writing data from/to Cassandra.
Before you start, you need to have basic knowledge of Apache Cassandra and Apache Spark.
Refer to [Datastax](https://docs.datastax.com/en/) and [Cassandra documentation](https://cassandra.apache.org/doc/latest/getting_started/index.html)
and [Spark documentation](https://spark.apache.org/docs/latest/). 

### Prerequisites

Install and launch a Cassandra cluster and a Spark cluster.   

Configure a new Scala project with the Apache Spark and dependency.

The dependencies are easily retrieved via the spark-packages.org website. For example, if you're using `sbt`, your build.sbt should include something like this:

    resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven"
    libraryDependencies += "datastax" % "spark-cassandra-connector" % "2.3.0-s_2.11"
 
The spark-packages libraries can also be used with spark-submit and spark shell, these
commands will place the connector and all of its dependencies on the path of the
Spark Driver and all Spark Executors.
   
    $SPARK_HOME/bin/spark-shell --packages datastax:spark-cassandra-connector:2.3.0-s_2.11
    $SPARK_HOME/bin/spark-submit --packages datastax:spark-cassandra-connector:2.3.0-s_2.11
   
For the list of available versions, see:
- https://spark-packages.org/package/datastax/spark-cassandra-connector
 
This driver does not depend on the Cassandra server code.

 - For a detailed dependency list, see [project/SparkCassandraConnectorBuild.scala](../project/SparkCassandraConnectorBuild.scala)
 - For dependency versions, see [project/Versions.scala](../project/Versions.scala)

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

### Loading up the Spark-Shell

Run the `spark-shell` with the packages line for your version. To configure
the default Spark Configuration pass key value pairs with `--conf`

    $SPARK_HOME/bin/spark-shell --conf spark.cassandra.connection.host=127.0.0.1 \
                                --packages datastax:spark-cassandra-connector:2.3.0-s_2.11

This command would set the Spark Cassandra Connector parameter 
`spark.cassandra.connection.host` to `127.0.0.1`. Change this
to the address of one of the nodes in your Cassandra cluster.
 
Enable Cassandra-specific functions on the `SparkContext`, `SparkSession`, `RDD`, and `DataFrame`:

```scala
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._
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
[Jump to - Accessing data with DataFrames](14_data_frames.md)
