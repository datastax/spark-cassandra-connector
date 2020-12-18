# Documentation

## Datasets (Previously DataFrames)

Datasets provide a new API for manipulating data within Spark. These provide a more user
friendly experience than pure Scala for common queries. The Spark Cassandra Connector provides
an integrated Data Source to make creating Cassandra Datasets easy.

[What happened to DataFrames?](#what-happened-to-dataframes)

Spark Docs:
* [Data Sources](https://spark.apache.org/docs/latest/sql-programming-guide.html#data-sources)
* [Datasets and DataFrames](https://spark.apache.org/docs/latest/sql-programming-guide.html#datasets-and-dataframes)

### Datasource Specific Options
DataSources in Spark take a map of Options which define how the source should act. The
Connector provides a CassandraSource which recognizes the following key/value pairs.
Those followed with a default of N/A are required, all others are optional.

| Option Key  | Controls                                              | Values        | Default  |
|-------------|-------------------------------------------------------|---------------|----------|
| table       | The Cassandra table to connect to                     | String        | N/A      |
| keyspace    | The keyspace where table is looked for                | String        | N/A      |
| cluster     | The group of the Cluster Level Settings to inherit    | String        | "default"|
| pushdown    | Enables pushing down predicates to C* when applicable | (true,false)  | true     |
| confirm.truncate | Confirm to truncate table when use Save.overwrite mode | (true,false) | false |

#### General Read, Write and Connection Options
Any normal Spark Connector configuration options for Connecting, Reading or Writing
can be passed through as Dataset options as well. When using the `read` command below these
options should appear exactly the same as when set in the SparkConf. See 
[Config Helpers](#example-using-typesafe-parameter-configuration-options) for
typed helpers for setting these options.

#### Setting Cluster and Keyspace Level Options
The connector also provides a way to describe the options which should be applied to all
Datasets within a cluster or within a keyspace. When a property has been specified at the
table level it will override the default keyspace or cluster property.

To add these properties add keys to your `SparkConf` use the helpers explained 
 in the next section or by manually entering them in the format

    clusterName:keyspaceName/propertyName
    
#### Example Using TypeSafe Parameter Configuration Options
There are also some helper methods which simplify setting Spark Cassandra 
Connector related parameters. This makes it easier to set parameters without
remembering the above syntax:
```scala
import org.apache.spark.sql.cassandra._

import com.datastax.spark.connector.cql.CassandraConnectorConf
import com.datastax.spark.connector.rdd.ReadConf

// set params for all clusters and keyspaces
spark.setCassandraConf(CassandraConnectorConf.KeepAliveMillisParam.option(10000))

// set params for the particular cluster
spark.setCassandraConf("Cluster1", CassandraConnectorConf.ConnectionHostParam.option("127.0.0.1") ++ CassandraConnectorConf.ConnectionPortParam.option(12345))
spark.setCassandraConf("Cluster2", CassandraConnectorConf.ConnectionHostParam.option("127.0.0.2"))

// set params for the particular keyspace 
spark.setCassandraConf("Cluster1", "ks1", ReadConf.SplitSizeInMBParam.option(128))
spark.setCassandraConf("Cluster1", "ks2", ReadConf.SplitSizeInMBParam.option(64))
spark.setCassandraConf("Cluster2", "ks3", ReadConf.SplitSizeInMBParam.option(80))
```

#### Example Changing Cluster/Keyspace Level Properties
```scala
spark.setCassandraConf("ClusterOne", "ks1", ReadConf.SplitSizeInMBParam.option(32))
spark.setCassandraConf("default", "test", ReadConf.SplitSizeInMBParam.option(128))

val df = spark
  .read
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "words", "keyspace" -> "test"))
  .load() // This Dataset will use a spark.cassandra.input.size of 128

val otherdf =  spark
  .read
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "words", "keyspace" -> "test" , "cluster" -> "ClusterOne"))
  .load() // This Dataset will use a spark.cassandra.input.size of 32

val lastdf = spark
  .read
  .format("org.apache.spark.sql.cassandra")
  .options(Map(
    "table" -> "words",
    "keyspace" -> "test" ,
    "cluster" -> "ClusterOne",
    "spark.cassandra.input.split.size_in_mb" -> 48
    )
  ).load() // This Dataset will use a spark.cassandra.input.split.size of 48
```

### Creating Datasets using Read Commands

The most programmatic way to create a Dataset is to invoke a `read` command on the SparkSession. This
will build a `DataFrameReader`. Specify `format` as `org.apache.spark.sql.cassandra`.
You can then use `options` to give a map of `Map[String,String]` of options as described above.
Then finish by calling `load` to actually get a `Dataset`. This code is all lazy
and will not actually load any data until an action is called.
 
As well as specifying all these parameters manually, we offer a set of 
[helper functions](#example-using-format-helper-functions) to make this easier as well.


#### Example Creating a Dataset using a Read Command
```scala
val df = spark
  .read
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "words", "keyspace" -> "test" ))
  .load()
  
df.show
```
```
word count
cat  30
fox  40
```

There are also some helper methods which can make creating Datasets easier. They can 
be accessed after importing `org.apache.spark.sql.cassandra` package. In the following 
example, all the commands used to create the Dataset are equivalent:

#### Example Using Format Helper Functions
```scala
import org.apache.spark.sql.cassandra._

val df = spark
  .read
  .cassandraFormat("words", "test")
  .load()
 
//Loading an Dataset using a format helper and a option helper
val df = spark
  .read
  .cassandraFormat("words", "test")
  .options(ReadConf.SplitSizeInMBParam.option(32))
  .load()
  
```

### Creating Datasets using Spark SQL

Accessing Datasets using Spark SQL involves creating temporary views with the format 
 as `org.apache.spark.sql.cassandra`. The `OPTIONS` passed to this table are used to
establish a relation between the CassandraTable and the Spark catalog reference.

#### Example Creating a Source Using Spark SQL:

Create Relation with the Cassandra table test.words
```scala
val createDDL = """CREATE TEMPORARY VIEW words
     USING org.apache.spark.sql.cassandra
     OPTIONS (
     table "words",
     keyspace "test",
     cluster "Test Cluster",
     pushdown "true")"""
spark.sql(createDDL) // Creates Catalog Entry registering an existing Cassandra Table
spark.sql("SELECT * FROM words").show
spark.sql("SELECT * FROM words WHERE word = 'fox'").show
```

### Persisting a Dataset to Cassandra Using the Save Command
Datasets provide a save function which allows them to persist their data to another
DataSource. The connector supports using this feature to persist a Dataset to a Cassandra
table.

#### Example Copying Between Two Tables Using Datasets
```scala

val df = spark
  .read
  .cassandraFormat("words", "test")
  .load()

df.write
  .cassandraFormat("words_copy", "test")
  .save()
```

Similarly to reading Cassandra tables into Datasets, we have some helper methods for the write path which are 
provided by `org.apache.spark.sql.cassandra` package. In the following example, all the commands are equivalent:

#### Example Using Helper Commands to Write Datasets
```scala
import org.apache.spark.sql.cassandra._

df.write
  .format("org.apache.spark.sql.cassandra")
  .options(Map("table" -> "words_copy", "keyspace" -> "test", "cluster" -> "cluster_B"))
  .save()

df.write
  .cassandraFormat("words_copy", "test", "cluster_B")
  .save()

```

### Setting Connector Specific Options on Datasets
Connector specific options can be set by invoking `options` method on either `DataFrameReader` or `DataFrameWriter`. 
There are several settings you may want to change in `ReadConf`, `WriteConf`, `CassandraConnectorConf`, `AuthConf` and
others. Those settings are identified by instances of `ConfigParameter` case class which offers an easy way to apply 
the option which it represents to a `DataFrameReader` or `DataFrameWriter`. 

Suppose we want to set `spark.cassandra.read.timeout_ms` to 7 seconds on some `DataFrameReader`, we can do this both 
ways:
```scala
option("spark.cassandra.read.timeout_ms", "7000")
```
Since this setting is represented by `CassandraConnectorConf.ReadTimeoutParam` we can simply do:
```scala
options(CassandraConnectorConf.ReadTimeoutParam.sqlOption("7000"))
```

Each parameter, that is, each instance of `ConfigParameter` allows to invoke `apply` method with a single parameter. 
That method returns a `Map[String, String]` (note that you need to use `options` instead of `option`) so setting 
multiple parameters can be chained:
```scala
options(CassandraConnectorConf.ReadTimeoutParam.sqlOption("7000") ++ ReadConf.TaskMetricParam.sqlOption(true))
```

### Creating a New Cassandra Table From a Dataset Schema
Spark Cassandra Connector adds a method to `Dataset` that allows it to create a new Cassandra table from
the `StructType` schema of the Dataset. This is convenient for persisting a Dataset to a new table, especially
when the schema of the Dataset is not known (fully or at all) ahead of time (at compile time of your application).
Once the new table is created, you can persist the Dataset to the new table using the save function described above.

The partition key and clustering key of the newly generated table can be set by passing in a list of 
names of columns which should be used as partition key and clustering key.

#### Example Creating a Cassandra Table from a Dataset
```scala
// Add spark connector specific methods to Dataset
import com.datastax.spark.connector._

val df = spark
  .read
  .cassandraFormat("words", "test")
  .load()

val renamed = df.withColumnRenamed("col1", "newcolumnname")
renamed.createCassandraTable(
    "test", 
    "renamed", 
    partitionKeyColumns = Some(Seq("user")), 
    clusteringKeyColumns = Some(Seq("newcolumnname")))

renamed.write
  .cassandraFormat("renamed", "test")
  .save()
```

### Automatic  Predicate Pushdown and Column Pruning
The Dataset API will automatically pushdown valid "where" clauses to Cassandra as long as the
pushdown option is enabled (default is enabled).

Example Table
```sql
CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };
USE test;
CREATE table words (
    user  TEXT, 
    word  TEXT, 
    count INT, 
    PRIMARY KEY (user, word));

INSERT INTO words (user, word, count ) VALUES ( 'Russ', 'dino', 10 );
INSERT INTO words (user, word, count ) VALUES ( 'Russ', 'fad', 5 );
INSERT INTO words (user, word, count ) VALUES ( 'Sam', 'alpha', 3 );
INSERT INTO words (user, word, count ) VALUES ( 'Zebra', 'zed', 100 );
```

First we can create a Dataset and see that it has no `pushdown filters` set in the log. This
means all requests will go directly to Cassandra and we will require reading all of the data to `show`
this Dataset.

#### Example Catalyst Optimization with Cassandra Server Side Pushdowns
```scala
val df = spark
  .read
  .cassandraFormat("words", "test")
  .load
df.explain
```
```
15/07/06 09:21:21 INFO CassandraSourceRelation: filters:
15/07/06 09:21:21 INFO CassandraSourceRelation: pushdown filters: //ArrayBuffer()
== Physical Plan ==
PhysicalRDD [user#0,word#1,count#2], MapPartitionsRDD[2] at explain //at <console>:22
```
```scala
df.show
```
```
15/07/06 09:26:03 INFO CassandraSourceRelation: filters:
15/07/06 09:26:03 INFO CassandraSourceRelation: pushdown filters: //ArrayBuffer()

+-----+-----+-----+
| user| word|count|
+-----+-----+-----+
|Zebra|  zed|  100|
| Russ| dino|   10|
| Russ|  fad|    5|
|  Sam|alpha|    3|
+-----+-----+-----+
```

The example schema has a clustering key of "word" so we can pushdown filters on that column to Cassandra. We
do this by applying a normal Dataset filter. The connector will automatically determine that the
filter can be pushed down and will add it to `pushdown filters`. All of the elements of
`pushdown filters` will be automatically added to the CQL requests made to Cassandra for the
data from this table. The subsequent call will then only serialize data from Cassandra which passes the filter,
reducing the load on Cassandra.

```scala
val dfWithPushdown = df.filter(df("word") > "ham")
dfWithPushdown.explain
```
```
15/07/06 09:29:10 INFO CassandraSourceRelation: filters: GreaterThan(word,ham)
15/07/06 09:29:10 INFO CassandraSourceRelation: pushdown filters: ArrayBuffer(GreaterThan(word,ham))
== Physical Plan ==
Filter (word#1 > ham)
 PhysicalRDD [user#0,word#1,count#2], MapPartitionsRDD[18] at explain at <console>:24
```
```scala
dfWithPushdown.show
```
```
15/07/06 09:30:48 INFO CassandraSourceRelation: filters: GreaterThan(word,ham)
15/07/06 09:30:48 INFO CassandraSourceRelation: pushdown filters: ArrayBuffer(GreaterThan(word,ham))
+-----+----+-----+
| user|word|count|
+-----+----+-----+
|Zebra| zed|  100|
+-----+----+-----+
```

#### Example Pushdown Filters
Example table
```sql
CREATE KEYSPACE IF NOT EXISTS pushdowns WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };
USE pushdowns;

CREATE TABLE pushdownexample (
    partitionkey1 BIGINT,
    partitionkey2 BIGINT,
    partitionkey3 BIGINT,
    clusterkey1   BIGINT,
    clusterkey2   BIGINT,
    clusterkey3   BIGINT,
    regularcolumn BIGINT,
    PRIMARY KEY ((partitionkey1, partitionkey2, partitionkey3), clusterkey1, clusterkey2, clusterkey3)
);
```
```scala
val df = spark
  .read
  .cassandraFormat("pushdownexample", "pushdowns")
  .load()
```
To push down partition keys, all of them must be included, but not more than one predicate per partition key, otherwise nothing is pushed down.
```scala
df.filter("partitionkey1 = 1 AND partitionkey2 = 1 AND partitionkey3 = 1").show()
```
```
INFO  2015-08-26 00:37:40 org.apache.spark.sql.cassandra.CassandraSourceRelation: filters: EqualTo(partitionkey1,1), EqualTo(partitionkey2,1), EqualTo(partitionkey3,1)
INFO  2015-08-26 00:37:40 org.apache.spark.sql.cassandra.CassandraSourceRelation: pushdown filters: ArrayBuffer(EqualTo(partitionkey1,1), EqualTo(partitionkey2,1), EqualTo(partitionkey3,1))
```
One partition key left out:
```scala
df.filter("partitionkey1 = 1 AND partitionkey2 = 1").show()
```
```
INFO  2015-08-26 00:53:07 org.apache.spark.sql.cassandra.CassandraSourceRelation: filters: EqualTo(partitionkey1,1), EqualTo(partitionkey2,1)
INFO  2015-08-26 00:53:07 org.apache.spark.sql.cassandra.CassandraSourceRelation: pushdown filters: ArrayBuffer()
```
More than one predicate for ```partitionkey3```:
```scala
df.filter("partitionkey1 = 1 AND partitionkey2 = 1 AND partitionkey3 > 0 AND partitionkey3 < 5").show()
```
```
INFO  2015-08-26 00:54:03 org.apache.spark.sql.cassandra.CassandraSourceRelation: filters: EqualTo(partitionkey1,1), EqualTo(partitionkey2,1), GreaterThan(partitionkey3,0), LessThan(partitionkey3,5)
INFO  2015-08-26 00:54:03 org.apache.spark.sql.cassandra.CassandraSourceRelation: pushdown filters: ArrayBuffer()
```
Clustering keys are more relaxed. But only the last predicate can be non-EQ, and if there is more than one predicate for a column, they must not be EQ or IN, otherwise only some predicates may be pushed down.
```scala
df.filter("clusterkey1 = 1 AND clusterkey2 > 0 AND clusterkey2 < 10").show()
```
```
INFO  2015-08-26 01:01:02 org.apache.spark.sql.cassandra.CassandraSourceRelation: filters: EqualTo(clusterkey1,1), GreaterThan(clusterkey2,0), LessThan(clusterkey2,10)
INFO  2015-08-26 01:01:02 org.apache.spark.sql.cassandra.CassandraSourceRelation: pushdown filters: ArrayBuffer(EqualTo(clusterkey1,1), GreaterThan(clusterkey2,0), LessThan(clusterkey2,10))
```
First predicate not EQ:
```scala
df.filter("clusterkey1 > 1 AND clusterkey2 > 1").show()
```
```
INFO  2015-08-26 00:55:01 org.apache.spark.sql.cassandra.CassandraSourceRelation: filters: GreaterThan(clusterkey1,1), GreaterThan(clusterkey2,1)
INFO  2015-08-26 00:55:01 org.apache.spark.sql.cassandra.CassandraSourceRelation: pushdown filters: ArrayBuffer(GreaterThan(clusterkey1,1))
```
```clusterkey2``` EQ predicate:
```scala
df.filter("clusterkey1 = 1 AND clusterkey2 = 1 AND clusterkey2 < 10").show()
```
```
INFO  2015-08-26 00:56:37 org.apache.spark.sql.cassandra.CassandraSourceRelation: filters: EqualTo(clusterkey1,1), EqualTo(clusterkey2,1), LessThan(clusterkey2,10)
INFO  2015-08-26 00:56:37 org.apache.spark.sql.cassandra.CassandraSourceRelation: pushdown filters: ArrayBuffer(EqualTo(clusterkey1,1), EqualTo(clusterkey2,1))
```

#### What Happened to DataFrames?

In Spark 2.0 DataFrames are now just a specific case of the Dataset API. In particular
a DataFrame is just an alias for Dataset\[Row\]. This means everything you know about
DataFrames is also applicable to Datasets. A DataFrame is just a special Dataset that is
made up of Row objects. Many texts and resources still use the two terms interchangeably.

[Next - Python DataFrames](15_python.md)
