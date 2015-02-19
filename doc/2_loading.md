# Documentation
## Accessing Cassandra data with CassandraRDD

This section describes how to access data from Cassandra table with Spark.   

### Obtaining a Cassandra table as an `RDD`

To get a Spark RDD that represents a Cassandra table, 
call the `cassandraTable` method on the `SparkContext` object. 

```scala
sc.cassandraTable("keyspace name", "table name")
```    

If no explicit type is given to `cassandraTable`, the result of this expression is `CassandraRDD[CassandraRow]`. 

Create this keyspace and table in Cassandra using cqlsh:

```sql
CREATE KEYSPACE test WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 };
CREATE TABLE test.words (word text PRIMARY KEY, count int);
```
    
Load data into the table:

```scala
INSERT INTO test.words (word, count) VALUES ('foo', 20);
INSERT INTO test.words (word, count) VALUES ('bar', 20);
```

Now you can read that table as `RDD`:

```scala
val rdd = sc.cassandraTable("test", "words")
// rdd: com.datastax.spark.connector.rdd.CassandraRDD[com.datastax.spark.connector.rdd.reader.CassandraRow] = CassandraRDD[0] at RDD at CassandraRDD.scala:41

rdd.toArray.foreach(println)
// CassandraRow{word: bar, count: 20}
// CassandraRow{word: foo, count: 20}   
```

### Using emptyCassandraRDD implementation

To create an instance of `CassandraRDD` for a table which does exist use the emptyCassandraRDD method. 
`emptyCassandraRDD`s do not perform validation or create partitions so they can be used to represent absent
tables. To create one, either initialize a `CassandraRDD` as usual and then call `toEmptyCassandraRDD`
method on it or call `emptyCassandraTable` method on Spark context.

Example:

```scala
// validation is deferred, so it is not triggered during rdd creation
val rdd = sc.cassandraTable[SomeType]("ks", "not_existing_table")
val emptyRDD = rdd.toEmptyCassandraRDD

val emptyRDD2 = sc.emptyCassandraTable[SomeType]("ks", "not_existing_table"))
```

### Reading primitive column values

You can read columns in a Cassandra table using the get methods of the `CassandraRow` object. 
The get methods access individual column values by column name or column index.
Type conversions are applied on the fly. Use `getOption` variants when you expect to receive Cassandra null values.

Continuing with the previous example, follow these steps to access individual column values.
Store the first item of the rdd in the firstRow value.

```scala
val firstRow = rdd.first
// firstRow: com.datastax.spark.connector.rdd.reader.CassandraRow = CassandraRow{word: bar, count: 20}
```

Get the number of columns and column names:

```scala
firstRow.columnNames    // Stream(word, count) 
firstRow.size           // 2 
```

Use one of `getXXX` getters to obtain a column value converted to desired type: 
```scala
firstRow.getInt("count")       // 20       
firstRow.getLong("count")      // 20L  
```

Or use a generic get to query the table by passing the return type directly:

```scala
firstRow.get[Int]("count")                   // 20       
firstRow.get[Long]("count")                  // 20L
firstRow.get[BigInt]("count")                // BigInt(20)
firstRow.get[java.math.BigInteger]("count")  // BigInteger(20)
```

### Working with nullable data

When reading potentially `null` data, use the `Option` type on the Scala side to prevent getting a `NullPointerException`.

```scala
firstRow.getIntOption("count")        // Some(20)
firstRow.get[Option[Int]]("count")    // Some(20)    
```

### Reading collections

You can read collection columns in a Cassandra table using the `getList`, `getSet`, `getMap` or generic `get` 
methods of the `CassandraRow` object. The `get` methods access 
the collection column and return a corresponding Scala collection. 
The generic `get` method lets you specify the precise type of the returned collection.

Assuming you set up the test keyspace earlier, follow these steps to access a Cassandra collection.

In the test keyspace, set up a collection set using cqlsh:

```sql
CREATE TABLE test.users (username text PRIMARY KEY, emails SET<text>);
INSERT INTO test.users (username, emails) 
     VALUES ('someone', {'someone@email.com', 's@email.com'});
```

Then in your application, retrieve the first row: 

```scala
val row = sc.cassandraTable("test", "users").first
// row: com.datastax.spark.connector.rdd.reader.CassandraRow = CassandraRow{username: someone, emails: [someone@email.com, s@email.com]}
```

Query the collection set in Cassandra from Spark:

```scala
row.getList[String]("emails")            // Vector(someone@email.com, s@email.com)
row.get[List[String]]("emails")          // List(someone@email.com, s@email.com)    
row.get[Seq[String]]("emails")           // List(someone@email.com, s@email.com)   :Seq[String]
row.get[IndexedSeq[String]]("emails")    // Vector(someone@email.com, s@email.com) :IndexedSeq[String]
row.get[Set[String]]("emails")           // Set(someone@email.com, s@email.com)
```

It is also possible to convert a collection to CQL `String` representation:

```scala
row.get[String]("emails")               // "[someone@email.com, s@email.com]"
```

A `null` collection is equivalent to an empty collection, therefore you don't need to use `get[Option[...]]` 
with collections.

### Reading columns of Cassandra User Defined Types
UDT column values are represented by `com.datastax.spark.connector.UDTValue` type.
The same set of getters is available on `UDTValue` as on `CassandraRow`.

Assume the following table definition:
```sql
CREATE TYPE test.address (city text, street text, number int);
CREATE TABLE test.companies (name text PRIMARY KEY, address FROZEN<address>);
```

You can read the address field of the company in the following way:
```scala
val address: UDTValue = row.getUDTValue("address")
val city = address.getString("city")
val street = address.getString("street")
val number = address.getInt("number")
```

### Data type conversions

The following table shows recommended Scala types corresponding to Cassandra column types. 

| Cassandra type    | Scala types
|-------------------|--------------------------------------------
| `ascii`, `text`   | `String`                                         
| `bigint`          | `Long`                                       
| `blob`            | `ByteBuffer`, `Array[Byte]` 
| `boolean`         | `Boolean`, `Int`              
| `counter`         | `Long`                       
| `decimal`         | `BigDecimal`, `java.math.BigDecimal` 
| `double`          | `Double`    
| `float`           | `Float`    
| `inet`            | `java.net.InetAddress` 
| `int`             | `Int` 
| `list`            | `Vector`, `List`, `Iterable`, `Seq`, `IndexedSeq`, `java.util.List` 
| `map`             | `Map`, `TreeMap`, `java.util.HashMap` 
| `set`             | `Set`, `TreeSet`, `java.util.HashSet` 
| `text`            | `String` 
| `timestamp`       | `Long`, `java.util.Date`, `java.sql.Date`, `org.joda.time.DateTime` 
| `uuid`            | `java.util.UUID` 
| `timeuuid`        | `java.util.UUID` 
| `varchar`         | `String` 
| `varint`          | `BigInt`, `java.math.BigInteger`
| user defined      | `UDTValue`

Other conversions might work, but may cause loss of precision or may not work for all values. 
All types are convertible to strings. Converting strings to numbers, dates, 
addresses or UUIDs is possible as long as the string has proper 
contents, defined by the CQL3 standard. Maps can be implicitly converted to/from sequences of key-value tuples.

## Accessing Cassandra with SparkSQL (since 1.1)
It is possible to query Cassandra using SparkSQL. Configure your `SparkContext` object
to use Cassandra as usual and then wrap it in a `org.apache.spark.sql.cassandra.CassandraSQLContext` object.
To execute an SQL query, call `CassandraSQLContext#sql` method.

```scala
import org.apache.spark.sql.cassandra.CassandraSQLContext
val sc: SparkContext = ...
val cc = new CassandraSQLContext(sc)
val rdd: SchemaRDD = cc.sql("SELECT * from keyspace.table WHERE ...")
```


## Configuration Options for Adjusting Reads

The following options can be specified in the SparkConf object or as a jvm
-Doption to adjust the read parameters of a Cassandra table.

| Environment Variable                    | Controls                                   | Default
|-----------------------------------------|--------------------------------------------|---------
| spark.cassandra.input.split.size        | approx number of rows in a Spark partition | 100000
| spark.cassandra.input.page.row.size     | number of rows fetched per roundtrip       | 1000
| spark.cassandra.input.consistency.level | consistency level to use when reading      | LOCAL_ONE

[Next - Server-side data selection and filtering](3_selection.md)
