# Documentation

## DataFrames - Experimental

DataFrames provide a new api for manipulating data within Spark. These provide a more user
friendly experience than pure scala for common queries. The Spark Cassandra Connector provides
an integrated DataSource to make creating Cassandra DataFrames easy. 

Spark Docs:
[Data Sources](https://spark.apache.org/docs/latest/sql-programming-guide.html#data-sources)
[Data Frames](https://spark.apache.org/docs/latest/sql-programming-guide.html#dataframes)


### Options
DataSources in Spark take a map of Options which define how the source should act. The
Connector provides a CassandraSource which recognizes the following Key Value pairs.
Those followed with a default of N/A are required, all others are optional. 

| Option Key  | Controls                                              | Values        | Default  |
|-------------|-------------------------------------------------------|---------------|----------|
| table       | The Cassandra Table to connect to                     | String        | N/A      |
| keyspace    | The Keyspace where table is looked for                | String        | N/A      |
| cluster     | The group of the Cluster Level Settings to inherit    | String        | "default"|
| pushdown    | Enables pushing down predicates to C* when applicable | (true,false)  | true     |

####Read, Writing and CassandraConnector Options
Any normal Spark Connector configuration options for Connecting, Reading or Writing
can be passed through as DataFrame options as well. When using the `read` command below these 
options should appear exactly the same as when set in the SparkConf.

####Setting Cluster and Keyspace Level Options
The connector also provides a way to describe the options which should be applied to all
DataFrames within a cluster or within a keyspace. When a property has been specified at the
table level it will override the default keyspace or cluster property.

To add these properties add keys to your `SparkConf` in the format
    
    clusterName:keyspaceName/propertyName.
    
Example Changing Cluster/Keyspace Level Properties
```scala 
val conf = new SparkConf()
  .set("ClusterOne/spark.cassandra.input.split.size","1000") 
  .set("default:test/spark.cassandra.input.split.size","5000")

...

val df = sqlContext
  .read
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "words", "keyspace" -> "test"))
  .load()// This DataFrame will use a spark.cassandra.input.size of 5000

val otherdf =  sqlContext
  .read
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "words", "keyspace" -> "test" , "cluster" -> "ClusterOne"))
  .load()// This DataFrame will use a spark.cassandra.input.size of 1000

val lastdf = sqlContext
  .read
  .format("org.apache.spark.sql.cassandra")
  .options(Map( 
    "table" -> "words", 
    "keyspace" -> "test" ,
    "cluster" -> "ClusterOne",
    "spark.cassandra.input.split.size" -> 500
    )
  ).load()// This DataFrame will use a spark.cassandra.input.split.size of 500
```

###Creating DataFrames using Read Commands

The most programmatic way to create a data frame is to invoke a `read` command on the SQLContext. This
 will build a `DataFrameReader`. Specify `format` as `org.apache.spark.sql.cassandra`. 
 You can then use `options` to give a map of `Map[String,String]` of options as described above. 
 Then finish by calling `load` to actually get a `DataFrame`.

Example Creating a DataFrame using a Read Command
```scala
val df = sqlContext
  .read
  .source("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "words", "keyspace" -> "test" ))
  .load()
df.show
//word count
//cat  30
//fox  40
```

###Creating DataFrames using Spark SQL

Accessing data Frames using Spark SQL involves creating temporary tables and specifying the
source as `org.apache.spark.sql.cassandra`. The `OPTIONS` passed to this table are used to
establish a relation between the CassandraTable and the internally used DataSource.

Because of a limitation in SparkSQL 1.4.0 DDL parser, SparkSQL `OPTIONS` 
do not accept "." and "_" characters in option names, so options containing these 
characters can be only set in the `SparkConf` or `SQLConf` objects.

Example Creating a Source Using Spark SQL:
```scala
//Create Relation with the cassandra table test.words
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
//word count
//cat  30
//fox  40
scala> df.filter(df("count") > 30).show
//word count
//fox  40
```

In addition you can use Spark SQL on the registered tables:
```scala
sqlContext.sql("SELECT * FROM words WHERE word = 'fox'").collect
//Array[org.apache.spark.sql.Row] = Array([fox,40])
```

###Persisting a DataFrame to Cassandra Using the Save Command
DataFrames provide a save function which allows them to persist their data to another
DataSource. The connector supports using this feature to persist a DataFrame a Cassandra
Table.

Example Copying Between Two Tables Using DataFrames
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

###Creating a New Cassandra Table From a DataFrame Schema
Spark Cassandra Connector adds a method to `DataFrame` that allows it to create a new Cassandra table from
the `StructType` schema of the DataFrame. This is convenient for persisting a DataFrame to a new table, especially
when the schema of the DataFrame is not known (fully or at all) ahead of time (at compile time of your application).
Once the new table is created, you can persist the DataFrame to the new table using the save function described above.

Example Transform DataFrame and Save to New Table
```scala
// Add spark connector specific methods to DataFrame
import com.datastax.spark.connector._

val df = sqlContext
  .read
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "words", "keyspace" -> "test" ))
  .load()

val renamed = df.withColumnRenamed("col1", "newcolumnname")
renamed.createCassandraTable("test", "renamed")

renamed.write
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "renamed", "keyspace" -> "test"))
  .save()
```

###Pushing down clauses to Cassandra
The dataframe api will automatically pushdown valid where clauses to Cassandra as long as the
pushdown option is enabled (defaults to enabled.)

Example Table
```sql
CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };
use test;
create table words ( user text, word text, count int , PRIMARY KEY (user,word))
INSERT INTO words (user, word, count ) VALUES ( 'Russ', 'dino', 10 );
INSERT INTO words (user, word, count ) VALUES ( 'Russ', 'fad', 5 );
INSERT INTO words (user, word, count ) VALUES ( 'Sam', 'alpha', 3 );
INSERT INTO words (user, word, count ) VALUES ( 'Zebra', 'zed', 100 );
```

First we can create a DataFrame and see that it has no `pushdown filters` set in the log. This
means all requests will go directly to C* and we will require reading all of the data to `show`
this DataFrame.

```scala
val df = sqlContext
  .read
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "words", "keyspace" -> "test"))
  .load
df.explain                               
//15/07/06 09:21:21 INFO CassandraSourceRelation: filters:
//15/07/06 09:21:21 INFO CassandraSourceRelation: pushdown filters: //ArrayBuffer()
//== Physical Plan ==
//PhysicalRDD [user#0,word#1,count#2], MapPartitionsRDD[2] at explain //at <console>:22

df.show
//...
//15/07/06 09:26:03 INFO CassandraSourceRelation: filters:
//15/07/06 09:26:03 INFO CassandraSourceRelation: pushdown filters: //ArrayBuffer()
//
//+-----+-----+-----+
//| user| word|count|
//+-----+-----+-----+
//|Zebra|  zed|  100|
//| Russ| dino|   10|
//| Russ|  fad|    5|
//|  Sam|alpha|    3|
//+-----+-----+-----+
```

The example schema has a clustering key of "word" so we can pushdown filters on that column to C*. We
do this by applying a normal DataFrame filter. The connector will automatically determine that the 
filter can be pushed down and will add it to `pushdown filters`. All of the elements of 
`pushdown filters` will be automatically added to the CQL requests made to C* for the 
data from this table. The subsequent call will then only serialize data from C* which passes the filter,
reducing the load on C*. 

```scala
val dfWithPushdown = df.filter(df("word") > "ham")

dfWithPushdown.explain
//15/07/06 09:29:10 INFO CassandraSourceRelation: filters: GreaterThan(word,ham)
//15/07/06 09:29:10 INFO CassandraSourceRelation: pushdown filters: ArrayBuffer(GreaterThan(word,ham))
== Physical Plan ==
Filter (word#1 > ham)
 PhysicalRDD [user#0,word#1,count#2], MapPartitionsRDD[18] at explain at <console>:24

dfWithPushdown.show
15/07/06 09:30:48 INFO CassandraSourceRelation: filters: GreaterThan(word,ham)
15/07/06 09:30:48 INFO CassandraSourceRelation: pushdown filters: ArrayBuffer(GreaterThan(word,ham))
+-----+----+-----+
| user|word|count|
+-----+----+-----+
|Zebra| zed|  100|
+-----+----+-----+

```

[Next - Python DataFrames](15_python.md) 