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
grouped in unlogged batches. The consistency level for writes is `LOCAL_QUORUM`. 

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

## Modifying CQL Collections
The default behavior of the Spark Cassandra Connector is to overwrite collections when inserted into
a cassandra table. To override this behavior you can specify a custom mapper with instructions on
how you would like the collection to be treated.

The following operations are supported

- append/add  (lists, sets, maps)
- prepend (lists)
- remove  (lists, sets)
- overwrite (lists, sets, maps) :: Default

Remove is not supported for Maps.

These are applied by adding the desired behavior to the ColumnSelector

Example Usage

Takes the elements from rddSetField and removes them from corrosponding C* column
"a_set" and takes elements from "rddMapField" and adds them to C* column "a_map" where C*
column key == key in the RDD elements. 
   
    ("key", "a_set" as "rddSetField" remove , "a_map" as "rddMapField" append)


Example Schema

```sql
CREATE TABLE ks.collections_mod (
      key int PRIMARY KEY,
      lcol list<text>,
      mcol map<text, text>,
      scol set<text>
  )
```

Example Appending/Prepending Lists

```scala
val listElements = sc.parallelize(Seq(
  (1,Vector("One")),
  (1,Vector("Two")),
  (1,Vector("Three"))))

val prependElements = sc.parallelize(Seq(
  (1,Vector("PrependOne")),
  (1,Vector("PrependTwo")),
  (1,Vector("PrependThree"))))

listElements.saveToCassandra("ks", "collections_mod", SomeColumns("key", "lcol" append))
prependElements.saveToCassandra("ks", "collections_mod", SomeColumns("key", "lcol" prepend))
```

```sql
cqlsh> Select * from ks.collections_mod where key = 1
   ... ;

 key | lcol                                                                | mcol | scol
-----+---------------------------------------------------------------------+------+------
   1 | ['PrependThree', 'PrependTwo', 'PrependOne', 'One', 'Two', 'Three'] | null | null

(1 rows)
```

## Saving objects of Cassandra User Defined Types
To save structures consisting of many fields, use a Case Class or a 
`com.datastax.spark.connector.UDTValue` class. An instance of this class can be easily obtained 
from a Scala `Map` by calling `fromMap` factory method.

Assume the following table definition:
```sql
CREATE TYPE test.address (city text, street text, number int);
CREATE TABLE test.companies (name text PRIMARY KEY, address FROZEN<address>);
```

You can use a case class to insert into the UDT like
```scala
case class Address(street: String, city: String, zip: Int)
val address = Address(city = "Oakland", zip = 90210, street = "Broadway")
val col = Seq((1, "Joe", address))
sc.parallelize(col).saveToCassandra(ks, "udts", SomeColumns("key", "name", "addr"))
```

Or use `UDTValue`'s `fromMap` to create the UDT before inserting:
```scala
import com.datastax.spark.connector.UDTValue
case class Company(name: String, address: UDTValue)
val address = UDTValue.fromMap(Map("city" -> "Santa Clara", "street" -> "Freedom Circle", "number" -> 3975))
val company = Company("DataStax", address)
sc.parallelize(Seq(company)).saveToCassandra("test", "companies")
```

## Skipping Columns and Avoiding Tombstones on Writes (Connector Version 1.6+ and Cassandra 2.2+)
Prior to Cassandra 2.2 there was no way to execute a prepared statement with unbound elements. 
This meant every executed statement via the Spark Cassandra Connector was required 
to bind `nulls` into for any unspecified columns. As of Cassandra 2.2, the native protocol now allows 
for leaving parameters unbound.

To take advantage of unset parameters, the Spark Cassandra Connector now provides a method for 
taking advantage of this unbound behavior. This is done by with the 
`com.datastax.spark.connector.types.CassandraOption` trait. 

The trait has three implementations
```scala
sealed trait CassandraOption[+A] extends Product with Serializable
  
  object CassandraOption {
    case class Value[+A](value: A) extends CassandraOption[A]
    case object Unset extends CassandraOption[Nothing]
    case object Null extends CassandraOption[Nothing]
```

This can be used when reading and writing from C*. When a column is loaded as a `CassandraOption` 
any missing columns will be represented as `Unset`. On writing, these parameters will remain unbound.
This means a table loaded via `CassandraOption` can be written to a second table without any missing
column values being treated as deletes.

####Example: Copying a table without deletes
```sql
//cqlsh
CREATE TABLE doc_example.tab1 (key INT, col_1 INT, col_2 INT, PRIMARY KEY (key))
INSERT INTO doc_example.tab1 (key, col_1, col_2) VALUES (1, null, 1)
CREATE TABLE doc_example.tab2 (key INT, col_1 INT, col_2 INT, PRIMARY KEY (key))
INSERT INTO doc_example.tab2 (key, col_1, col_2) VALUES (1, 5, null)
```

```scala
//spark-shell
val ks = "doc_example"
//Copy the data from tab1 to tab2 but don't delete when we see a null in tab1
sc.cassandraTable[(Int, CassandraOption[Int], CassandraOption[Int])](ks, "tab1")
  .saveToCassandra(ks, "tab2")
  
sc.cassandraTable[(Int,Int,Int)](ks, "tab2").collect
//(1, 5, 1)
```
 
For more complicated use cases the `CassandraOption` can be set to delete on a per row 
(and per column) basis. This is done by using either the `Unset` or `Null` case objects.

####Example of using different None behaviors
```scala
//Fill tab1 with (1, 1, 1) , (2, 2, 2) ... (6, 6, 6)
sc.parallelize(1 to 6).map(x => (x, x, x)).saveToCassandra(ks, "tab1")
//Delete the second column when x >= 5
//Delete the third column when x <= 2
//For other rows put in the value -1
sc.parallelize(1 to 6).map(x => x match {
  case x if (x >= 5) => (x, CassandraOption.Null, CassandraOption.Unset)
  case x if (x <= 2) => (x, CassandraOption.Unset, CassandraOption.Null)
  case x => (x, CassandraOption(-1), CassandraOption(-1))
}).saveToCassandra(ks, "tab1")

val results = sc.cassandraTable[(Int, Option[Int], Option[Int])](ks, "tab1").collect
results 
/*
  (1, Some(1), None),
  (2, Some(2), None),
  (3, Some(-1), Some(-1)),
  (4, Some(-1), Some(-1)),
  (5, None, Some(5)),
  (6, None, Some(6)))
*/
```
  
CassandraOptions can be converted to Scala Options via an implemented implicit. This means that 
CassandraOptions can be dealt with as if they were normal Scala Options. For the reverse 
transformation, from a Scala Option into a CassandraOption, you need to define the None behavior. 
This is done via `CassandraOption.deleteIfNone` and `CassandraOption.unsetIfNone`
 
####Example of converting Scala Options to Cassandra Options
```scala
import com.datastax.spark.connector.types.CassandraOption
//Setup original data (1, 1, 1) ... (6, 6, 6)
sc.parallelize(1 to 6).map(x => (x,x,x)).saveToCassandra(ks, "tab1")

//Setup options Rdd (1, None, None) (2, None, None) ... (6, None, None)
val optRdd = sc.parallelize(1 to 6)
  .map(x => (x, None, None))
   
//Delete the second column, but ignore the third column
optRdd
  .map{ case (x: Int, y: Option[Int], z: Option[Int]) =>
    (x, CassandraOption.deleteIfNone(y), CassandraOption.unsetIfNone(z))
  }.saveToCassandra(ks, "tab1")

val results = sc.cassandraTable[(Int, Option[Int], Option[Int])](ks, "tab1").collect
results
/*
    (1, None, Some(1)),
    (2, None, Some(2)),
    (3, None, Some(3)),
    (4, None, Some(4)),
    (5, None, Some(5)),
    (6, None, Some(6))
*/
```
     
### Leaving all nulls as Unset
WriteConf also now contains a parameter `ignoreNulls` which can be set via using a `SparkConf` key
`spark.cassandra.output.ignoreNulls`. The default is `false` which will cause `null`s to be treated 
as in previous versions (being inserted into C* as is). When set to `true` all `null`s will be 
treated as `unset`. This can be used with DataFrames to skip null records and avoid tombstones.

####Example of using ignoreNulls
```scala
//Setup original data (1, 1, 1) --> (6, 6, 6)
sc.parallelize(1 to 6).map(x => (x, x, x)).saveToCassandra(ks, "tab1")

val ignoreNullsWriteConf = WriteConf.fromSparkConf(sc.getConf).copy(ignoreNulls = true)
//These writes will not delete because we are ignoring nulls
val optRdd = sc.parallelize(1 to 6)
  .map(x => (x, None, None))
  .saveToCassandra(ks, "tab1", writeConf = ignoreNullsWriteConf)

val results = sc.cassandraTable[(Int, Int, Int)](ks, "tab1").collect

results
/**
  (1, 1, 1),
  (2, 2, 2),
  (3, 3, 3),
  (4, 4, 4),
  (5, 5, 5),
  (6, 6, 6)
**/
```
    
## Specifying TTL and WRITETIME
Spark Cassandra Connector saves the data without explicitly specifying TTL or WRITETIME. If a certain 
values for them have to be used, there are a couple options provided by the API. 

TTL and WRITETIME options are specified as properties of `WriteConf` object, which can be optionally 
passed to `saveToCassandra` method. TTL and WRITETIME options are specified independently from one 
another. 

### Using a constant value for all rows
When the same value should be used for all the rows, one can use the following syntax:

```scala
import com.datastax.spark.connector.writer._
...
rdd.saveToCassandra("test", "tab", writeConf = WriteConf(ttl = TTLOption.constant(100)))
rdd.saveToCassandra("test", "tab", writeConf = WriteConf(timestamp = TimestampOption.constant(ts)))
```

`TTLOption.constant` accepts one of the following values:
  - `Int` / the number of seconds
  - `scala.concurrent.duration.Duration`
  - `org.joda.time.Duration`

`TimestampOption.constant` accepts one of the following values:
  - `Long` / the number of microseconds
  - `java.util.Date`
  - `org.joda.time.DateTime`

### Using a different value for each row
When a different value of TTL or WRITETIME has to be used for each row, one can use the following syntax:

```scala
import com.datastax.spark.connector.writer._
...
rdd.saveToCassandra("test", "tab", writeConf = WriteConf(ttl = TTLOption.perRow("ttl")))
rdd.saveToCassandra("test", "tab", writeConf = WriteConf(timestamp = TimestampOption.perRow("timestamp")))
```

`perRow(String)` method accepts a name of a property in each RDD item, which value will be used as TTL 
or WRITETIME value for the row. 

Say we have an RDD with `KeyValueWithTTL` objects, defined as follows:
```scala
case class KeyValueWithTTL(key: Int, group: Long, value: String, ttl: Int)

val rdd = sc.makeRDD(Seq(
  KeyValueWithTTL(1, 1L, "value1", 100), 
  KeyValueWithTTL(2, 2L, "value2", 200), 
  KeyValueWithTTL(3, 3L, "value3", 300)))
```

and a CQL table:
```sql
CREATE TABLE IF NOT EXISTS test.tab (
    key INT, 
    group BIGINT, 
    value TEXT, 
    PRIMARY KEY (key, group)
)
```

When we run the following command:
```scala
import com.datastax.spark.connector.writer._
...
rdd.saveToCassandra("test", "tab", writeConf = WriteConf(ttl = TTLOption.perRow("ttl")))
```

the TTL for the 1st row will be 100, TTL for the 2nd row will be 200 and TTL for the 3rd row will be 300.

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

To create a table with a custom definition, and define which columns are to be partition and clustering column keys:

```scala
import com.datastax.spark.connector.cql.{ColumnDef, RegularColumn, TableDef, ClusteringColumn, PartitionKeyColumn}
import com.datastax.spark.connector.types._

// Define structure for rdd data
case class outData(col1:UUID, col2:UUID, col3: Double, col4:Int)

// Define columns
val p1Col = new ColumnDef("col1",PartitionKeyColumn,UUIDType)
val c1Col = new ColumnDef("col2",ClusteringColumn(0),UUIDType)
val c2Col = new ColumnDef("col3",ClusteringColumn(1),DoubleType)
val rCol = new ColumnDef("col4",RegularColumn,IntType)

// Create table definition
val table = TableDef("test","words",Seq(p1Col),Seq(c1Col, c2Col),Seq(rCol))

// Map rdd into custom data structure and create table
val rddOut = rdd.map(s => outData(s._1, s._2(0), s._2(1), s._3))
rddOut.saveAsCassandraTableEx(table, SomeColumns("col1", "col2", "col3", "col4"))
```

## Tuning
For a full listing of Write Tuning Parameters see the reference section
[Write Tuning Parameters](reference.md#write-tuning-parameters)

[Next - Customizing the object mapping](6_advanced_mapper.md)
