# Documentation
## Saving datasets to Cassandra

It is possible to save any `RDD` to Cassandra, not just `CassandraRDD`. 
The only requirement is that the object class of `RDD` is a tuple or has property names 
corresponding to Cassandra column names. 

It is possible to save an `RDD` to an existing Cassandra table as well as to let the
connector create appropriate table automatically based on the definition
of the `RDD` item class.
To save an `RDD` to an existing table, import `com.datastax.spark.connector._`
and call the `saveToCassandra` method with the
keyspace name, table name and a list of columns. Make sure to include at least all primary key columns.
To save an `RDD` to a new table, instead of calling `saveToCassandra`, call `saveAsCassandraTable` or
`saveAsCassandraTableEx` with the name of the table you want to create.
 
## Saving a collection of tuples

Assume the following table definition:
```sql
CREATE TABLE test.words (word text PRIMARY KEY, count int);
```

```scala
val collection = sc.parallelize(Seq(("cat", 30), ("fox", 40)))
collection.saveToCassandra("test", "words", SomeColumns("word", "count"))
```
    
    cqlsh:test> select * from words;

     word | count
    ------+-------
      bar |    20
      foo |    10
      cat |    30
      fox |    40

    (4 rows)
   
Using a custom mapper is also supported with tuples

```sql
CREATE TABLE test.words (word text PRIMARY KEY, count int);
```

```scala
val collection = sc.parallelize(Seq((30, "cat"), (40, "fox")))
collection.saveToCassandra("test", "words", SomeColumns("word" as "_2", "count" as "_1"))
```
    
    cqlsh:test> select * from words;

     word | count
    ------+-------
      cat |    30
      fox |    40

    (2 rows)

## Saving a collection of objects
When saving a collection of objects of a user-defined class, the items to be saved
must provide appropriately named public property accessors for getting every column
to be saved. This example provides more information on property-column naming 
conventions as described [here](4_mapper.md).

```scala
case class WordCount(word: String, count: Long)
val collection = sc.parallelize(Seq(WordCount("dog", 50), WordCount("cow", 60)))
collection.saveToCassandra("test", "words", SomeColumns("word", "count"))
```

    cqlsh:test> select * from words;

     word | count
    ------+-------
      bar |    20
      foo |    10
      cat |    30
      fox |    40
      dog |    50
      cow |    60
      
The driver will execute a CQL `INSERT` statement for every object in the `RDD`, 
grouped in unlogged batches. The consistency level for writes is `ONE`. 

It is possible to specify custom column to property mapping with `SomeColumns`. If the property
names in objects to be saved do not correspond to the column names in the destination table, use
the `as` method on the column names you want to override. The parameter order is table column
name first, then object property name.

Example:
Say you want to save `WordCount` objects to the table which has columns `word TEXT` and `num INT`.

```scala
case class WordCount(word: String, count: Long)
val collection = sc.parallelize(Seq(WordCount("dog", 50), WordCount("cow", 60)))
collection.saveToCassandra("test", "words2", SomeColumns("word", "num" as "count"))
```

## Saving objects of Cassandra User Defined Types
To save structures consisting of many fields, use `com.datastax.spark.connector.UDTValue`
class. An instance of this class can be easily obtained from a Scala `Map` by calling `fromMap`
factory method.

Assume the following table definition:
```sql
CREATE TYPE test.address (city text, street text, number int);
CREATE TABLE test.companies (name text PRIMARY KEY, address FROZEN<address>);
```

To create a new row in the `test.companies` table:
```scala
import com.datastax.spark.connector.UDTValue
case class Company(name: String, address: UDTValue)
val address = UDTValue.fromMap(Map("city" -> "Santa Clara", "street" -> "Freedom Circle", "number" -> 3975))
val company = Company("DataStax", address)
sc.parallelize(Seq(company)).saveToCassandra("test", "companies")
```

## Saving RDDs as new tables
Use `saveAsCassandraTable` method to automatically create a new table with given name
and save the `RDD` into it. The keyspace you're saving to must exist.
The following code will create a new table `words_new` in keyspace `test` with
columns `word` and `count`, where `word` becomes a primary key:

```scala
case class WordCount(word: String, count: Long)
val collection = sc.parallelize(Seq(WordCount("dog", 50), WordCount("cow", 60)))
collection.saveAsCassandraTable("test", "words_new", SomeColumns("word", "count"))
```

To customize the table definition, call `saveAsCassandraTableEx`. The following example
demonstrates how to add another column of int type to the table definition, creating new
table `words_new_2`:

```scala
import com.datastax.spark.connector.cql.{ColumnDef, RegularColumn, TableDef}
import com.datastax.spark.connector.types.IntType
case class WordCount(word: String, count: Long)
val table1 = TableDef.fromType[WordCount]("test", "words_new")
val table2 = TableDef("test", "words_new_2", table1.partitionKey, table1.clusteringColumns,
  table1.regularColumns :+ ColumnDef("additional_column", RegularColumn, IntType))
val collection = sc.parallelize(Seq(WordCount("dog", 50), WordCount("cow", 60)))
collection.saveAsCassandraTableEx(table2, SomeColumns("word", "count"))
```


## Tuning
The following properties set in `SparkConf` can be used to fine-tune the saving process, 
These values have been set to achieve stability and not performance. Changing these values may
increase your performance based on your workload:

  - `spark.cassandra.output.batch.size.rows`: number of rows per single batch; default is 'auto' which means the connector 
     will adjust the number of rows based on the amount of data in each row  
  - `spark.cassandra.output.batch.size.bytes`: maximum total size of the batch in bytes; defaults to 1 kB.
  - `spark.cassandra.output.batch.grouping.key`: determines how insert statements are grouped into batches; available values are:
     - `none`: a batch may contain any statements
     - `replica_set`: a batch may contain only statements to be written to the same replica set
     - `partition` (default): a batch may contain only statements for rows sharing the same partition key value
  - `spark.cassandra.output.batch.buffer.size`: how many batches per single Spark task can be stored in memory before sending to Cassandra; default 1000
  - `spark.cassandra.output.concurrent.writes`: maximum number of batches executed in parallel by a single Spark task; defaults to 5
  - `spark.cassandra.output.consistency.level`: consistency level for writing; defaults to LOCAL_ONE.
  - `spark.cassandra.output.throughput_mb_per_sec`: maximum write throughput allowed per single core in MB/s
                                                    limit this on long (+8 hour) runs to 70% of your max 
                                                    throughput as seen on a smaller job for stability

[Next - Customizing the object mapping](6_advanced_mapper.md)
