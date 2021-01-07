# Documentation

## 5-minute quick start guide for Spark 3.0

In this tutorial, you'll learn how to setup a very simple Spark application for reading and writing data from/to Cassandra.
Before you start, you need to have basic knowledge of Apache Cassandra and Apache Spark.
Refer to [Datastax](https://docs.datastax.com/en/) and [Cassandra documentation](https://cassandra.apache.org/doc/latest/getting_started/index.html)
and [Spark documentation](https://spark.apache.org/docs/latest/). 

### Prerequisites

Install and launch a Cassandra cluster

### Loading up the Spark-Shell

Run `sparkr` with the packages line for your version. This will include the connector
and *all* of its dependencies on the Spark Class PathTo configure
the default Spark Configuration pass key value pairs with `--conf`

    $SPARK_HOME/bin/sparkr --conf spark.cassandra.connection.host=127.0.0.1 \
                                --packages com.datastax.spark:spark-cassandra-connector_2.12:3.0.0
                                --conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions

This command would set the Spark Cassandra Connector parameter 
`spark.cassandra.connection.host` to `127.0.0.1`. Change this
to the address of one of the nodes in your Cassandra cluster.

The extensions configuration option enables Cassandra Specific Catalyst
optimizations and functions.
 
Create a Catalog Reference to your Cassandra Cluster

```R
sparkR.session(sparkConfig=list(
    spark.sql.catalog.mycatalog="com.datastax.spark.connector.datasource.CassandraCatalog", 
    spark.sql.catalog.mycatalog.spark.cassandra.connection.host="127.0.0.1"
    ))
```

### Create a keyspace and table in Cassandra
These lines will create an actual Keyspace and Table in Cassandra.
```R
sql("CREATE DATABASE IF NOT EXISTS mycatalog.testks WITH DBPROPERTIES (class='SimpleStrategy',replication_factor='1')")
sql("CREATE TABLE mycatalog.testks.testtab (key Int, value STRING) USING cassandra PARTITIONED BY (key)")

//List their contents
head(sql("SHOW NAMESPACES FROM mycatalog"))
head(sql("SHOW TABLES FROM mycatalog.testks"))
```

### Loading and analyzing data from Cassandra
Use the SparkSQL or a DataframeReader to Load a table

```R
    // TODO - this commented code is the Scala code - is there a DSL for R? or only SQL commands allowed?
        // val df = spark.read.table("mycatalog.testks.testtab")
        // println(df.count)
        // df.show

//or

head(sql("SELECT * FROM mycatalog.testks.testtab"))
```

### Saving data from a dataframe to Cassandra  
// TODO is there a DSL for R like there is in Scala? 

Add 10 more rows to the table:

---
**NOTE**
The following code is for Scala, not R. It is left in as a reference for future translation to R

---

```R
spark
  .range(1, 10)
  .withColumnRenamed("id", "key")
  .withColumn("value", col("key").cast("string"))
  .writeTo("mycatalog.testks.testtab")
  .append
```

[Accessing data with DataFrames](14_data_frames.md)
[More details on Connecting to Cassandra](1_connecting.md)

