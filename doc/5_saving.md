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

```
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
