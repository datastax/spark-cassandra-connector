# Documentation

## DataFrames

DataFrames provide a new API for manipulating data within Spark. These provide a more user
friendly experience than pure Scala for common queries. The Spark Cassandra Connector provides
an integrated DataSource to make creating Cassandra DataFrames easy.

Spark Docs:
[Data Sources](https://spark.apache.org/docs/latest/sql-programming-guide.html#data-sources)
[Data Frames](https://spark.apache.org/docs/latest/sql-programming-guide.html#dataframes)


### Options
DataSources in Spark take a map of Options which define how the source should act. The
Connector provides a CassandraSource which recognizes the following Key Value pairs.
Those followed with a default of N/A are required, all others are optional.

| Option Key  | Controls                                                     | Values        | Default  |
|-------------|--------------------------------------------------------------|---------------|----------|
| table       | The Cassandra table to connect to                            | String        | N/A      |
| keyspace    | The keyspace where table is looked for                       | String        | N/A      |
| cluster     | The group of the Cluster Level Settings to inherit           | String        | "default"|
| pushdown    | Enables pushing down predicates to Cassandra when applicable | (true,false)  | true     |

#### Read, Writing and CassandraConnector Options
Any normal Spark Connector configuration options for Connecting, Reading or Writing
can be passed through as DataFrame options as well. When using the `read` command below these
options should appear exactly the same as when set in the SparkConf.

#### Setting Cluster and Keyspace Level Options
The connector also provides a way to describe the options which should be applied to all
DataFrames within a cluster or within a keyspace. When a property has been specified at the
table level it will override the default keyspace or cluster property.

To add these properties add keys to your `SparkConf` in the format

    clusterName:keyspaceName/propertyName.

#### Example Changing Cluster/Keyspace Level Properties
```scala
sqlContext.setConf("ClusterOne/spark.cassandra.input.split.size_in_mb", "32")
sqlContext.setConf("default:test/spark.cassandra.input.split.size_in_mb", "128")
...
val df = sqlContext
  .read
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "words", "keyspace" -> "test"))
  .load() // This DataFrame will use a spark.cassandra.input.size of 32

val otherdf =  sqlContext
  .read
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "words", "keyspace" -> "test" , "cluster" -> "ClusterOne"))
  .load() // This DataFrame will use a spark.cassandra.input.size of 128

val lastdf = sqlContext
  .read
  .format("org.apache.spark.sql.cassandra")
  .options(Map(
    "table" -> "words",
    "keyspace" -> "test" ,
    "cluster" -> "ClusterOne",
    "spark.cassandra.input.split.size_in_mb" -> 48
    )
  ).load() // This DataFrame will use a spark.cassandra.input.split.size of 48
```


#### Example Using TypeSafe Parameter Configuration Options
There are also some helper method which simplifies setting Spark Cassandra Connector related parameters. They are a part
of `CassandraSqlContext`:
```scala
// set params for all clusters and keyspaces
sqlContext.setConf(CassandraConnectorConf.KeepAliveMillisParam.option(10000))

// set params for the particular cluster
sqlContext.setConf("Cluster1", CassandraConnectorConf.ConnectionHostParam.option("127.0.0.1") ++ CassandraConnectorConf.ConnectionPortParam.option(12345))
sqlContext.setConf("Cluster2", CassandraConnectorConf.ConnectionHostParam.option("127.0.0.2"))

// set params for the particular keyspace 
sqlContext.setConf("Cluster1", "ks1", ReadConf.SplitSizeInMBParam.option(128))
sqlContext.setConf("Cluster1", "ks2", ReadConf.SplitSizeInMBParam.option(64))
sqlContext.setConf("Cluster2", "ks3", ReadConf.SplitSizeInMBParam.option(80))
```

###Creating DataFrames using Read Commands

The most programmatic way to create a data frame is to invoke a `read` command on the SQLContext. This
 will build a `DataFrameReader`. Specify `format` as `org.apache.spark.sql.cassandra`.
 You can then use `options` to give a map of `Map[String,String]` of options as described above.
 Then finish by calling `load` to actually get a `DataFrame`.

#### Example Creating a DataFrame using a Read Command
```scala
val df = sqlContext
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

There are also some helper methods which can make creating data frames easier. They can be accessed after importing 
`org.apache.spark.sql.cassandra` package. In the following example, all the commands used to create a data frame are 
equivalent:

#### Example Using Format Helper Functions
```scala
import org.apache.spark.sql.cassandra._

val df1 = sqlContext
  .read
  .format("org.apache.spark.sql.cassandra")
  .options(Map("table" -> "words", "keyspace" -> "test", "cluster" -> "cluster_A"))
  .load()

val df2 = sqlContext
  .read
  .cassandraFormat("words", "test", "cluster_A")
  .load()
```

### Creating DataFrames using Spark SQL

Accessing data Frames using Spark SQL involves creating temporary tables and specifying the
source as `org.apache.spark.sql.cassandra`. The `OPTIONS` passed to this table are used to
establish a relation between the CassandraTable and the internally used DataSource.

#### Example Creating a Source Using Spark SQL:

Create Relation with the Cassandra table test.words
```scala
scala> sqlContext.sql(
   """CREATE TEMPORARY TABLE words
     |USING org.apache.spark.sql.cassandra
     |OPTIONS (
     |  table "words",
     |  keyspace "test",
     |  cluster "Test Cluster",
     |  pushdown "true"
     |)""".stripMargin)
scala> val df = sqlContext.sql("SELECT * FROM words")
scala> df.show()
```
```
word count
cat  30
fox  40
```
```scala
scala> df.filter(df("count") > 30).show
```
```
word count
fox  40
```

In addition you can use Spark SQL on the registered tables:
```scala
sqlContext.sql("SELECT * FROM words WHERE word = 'fox'").collect
```
```
Array[org.apache.spark.sql.Row] = Array([fox,40])
```

###Persisting a DataFrame to Cassandra Using the Save Command
DataFrames provide a save function which allows them to persist their data to another
DataSource. The connector supports using this feature to persist a DataFrame a Cassandra
Table.

#### Example Copying Between Two Tables Using DataFrames
```scala
val df = sqlContext
  .read
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "words", "keyspace" -> "test" ))
  .load()

df.write
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "words_copy", "keyspace" -> "test"))
  .save()
```

Similarly to reading Cassandra tables into data frames, we have some helper methods for the write path which are 
provided by `org.apache.spark.sql.cassandra` package. In the following example, all the commands are equivalent:

#### Example Using Helper Commands to Write DataFrames
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

### Setting Connector specific options on DataFrames
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

###Creating a New Cassandra Table From a DataFrame Schema
Spark Cassandra Connector adds a method to `DataFrame` that allows it to create a new Cassandra table from
the `StructType` schema of the DataFrame. This is convenient for persisting a DataFrame to a new table, especially
when the schema of the DataFrame is not known (fully or at all) ahead of time (at compile time of your application).
Once the new table is created, you can persist the DataFrame to the new table using the save function described above.

The partition key and clustering key of the newly generated table can be set by passing in a list of 
names of columns which should be used as partition key and clustering key.

#### Example Creating a Cassandra Table from a DataFrame
```scala
// Add spark connector specific methods to DataFrame
import com.datastax.spark.connector._

val df = sqlContext
  .read
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "words", "keyspace" -> "test" ))
  .load()

val renamed = df.withColumnRenamed("col1", "newcolumnname")
renamed.createCassandraTable(
    "test", 
    "renamed", 
    partitionKeyColumns = Some(Seq("user")), 
    clusteringKeyColumns = Some(Seq("newcolumnname")))

renamed.write
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "renamed", "keyspace" -> "test"))
  .save()
```

### Pushing down clauses to Cassandra
The DataFrame API will automatically pushdown valid where clauses to Cassandra as long as the
pushdown option is enabled (defaults to enabled.)

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

First we can create a DataFrame and see that it has no `pushdown filters` set in the log. This
means all requests will go directly to Cassandra and we will require reading all of the data to `show`
this DataFrame.

#### Example Catalyst Optimization with Cassandra Server Side Pushdowns
```scala
val df = sqlContext
  .read
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "words", "keyspace" -> "test"))
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
do this by applying a normal DataFrame filter. The connector will automatically determine that the
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
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._

val df = sqlContext
  .read
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "pushdownexample", "keyspace" -> "pushdowns" ))
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

[Next - Python DataFrames](15_python.md)
