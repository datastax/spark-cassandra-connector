# Documentation

## PySpark with Data Frames

With the inclusion of the Cassandra Data Source, PySpark can now be used with the Connector to 
access Cassandra data. This does not require DataStax Enterprise but you are limited to DataFrame
only operations.

### Setup

To enable Cassandra access the Spark Cassandra Connector assembly jar must be included on both the
driver and executor classpath for the PySpark Java Gateway. This can be done by starting the PySpark
shell similarlly to how the spark shell is started. The preferred method is now to use the Spark Packages
website. 
http://spark-packages.org/package/datastax/spark-cassandra-connector

```bash
./bin/pyspark \
  --packages com.datastax.spark:spark-cassandra-connector_2.10:1.4.0
```

### Loading a DataFrame in Python

A DataFrame can be created which links to cassandra by using the the `org.apache.spark.sql.cassandra` 
source and by specifying keyword arguements for `keyspace` and `table`.

```python
 sqlContext.read\
    .format("org.apache.spark.sql.cassandra")\
    .options(table="kv", keyspace="test")\
    .load().show()
```

```
+-+-+
|k|v|
+-+-+
|5|5|
|1|1|
|2|2|
|4|4|
|3|3|
+-+-+
```

### Saving a DataFrame in Python to Cassandra

A DataFrame can be saved to an *existing* Cassandra table by using the the `org.apache.spark.sql.cassandra` source and by specifying keyword arguements for `keyspace` and `table` and saving mode (`append`, `overwrite`, `error` or `ignore`, see [Data Sources API doc](https://spark.apache.org/docs/latest/sql-programming-guide.html#save-modes)).

```python
 sqlContext.write\
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table="kv", keyspace="test")\
    .save()
```

The options and parameters are identical to the Scala Data Frames Api so
please see [Data Frames](14_data_frames.md) for more information.

[Next - Spark Partitioners](16_partitioning.md)
