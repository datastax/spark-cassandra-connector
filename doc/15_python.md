# Documentation

## PySpark with Data Frames

With the inclusion of the Cassandra Data Source, PySpark can now be used with the Connector to 
access Cassandra data. This does not require DataStax Enterprise but you are limited to DataFrame
only operations.

### Setup

To enable Cassandra access the Spark Cassandra Connector assembly jar must be included on both the
driver and executor classpath for the PySpark Java Gateway. This can be done by starting the PySpark
shell similarly to how the spark shell is started. The preferred method is now to use the Spark Packages
website. 
http://spark-packages.org/package/datastax/spark-cassandra-connector

```bash
./bin/pyspark \
  --packages com.datastax.spark:spark-cassandra-connector_2.11:2.0.0-M3
```

### Loading a DataFrame in Python

A DataFrame can be created which links to Cassandra by using the the `org.apache.spark.sql.cassandra` 
source and by specifying keyword arguments for `keyspace` and `table`.

#### Example Loading a Cassandra Table as a Pyspark DataFrame
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

A DataFrame can be saved to an *existing* Cassandra table by using the the `org.apache.spark.sql.cassandra` source and by specifying keyword arguments for `keyspace` and `table` and saving mode (`append`, `overwrite`, `error` or `ignore`, see [Data Sources API doc](https://spark.apache.org/docs/latest/sql-programming-guide.html#save-modes)).

#### Example Saving to a Cassanra Table as a Pyspark DataFrame
```python
 df.write\
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table="kv", keyspace="test")\
    .save()
```

The options and parameters are identical to the Scala Data Frames Api so
please see [Data Frames](14_data_frames.md) for more information.

[Next - Spark Partitioners](16_partitioning.md)
