# Documentation
## Accessing Cassandra data with CassandraRDD

This section describes how to access data from Cassandra table with Spark.   

### Obtaining a Cassandra table as an `RDD`

To get a Spark RDD that represents a Cassandra table, 
call the `cassandraTable` method on the `SparkContext` object. 

    sc.cassandraTable("keyspace name", "table name")
    
If no explicit type is given to `cassandraTable`, the result of this expression is `CassandraRDD[CassandraRow]`. 

Create this keyspace and table in Cassandra using cqlsh:

    cqlsh> CREATE KEYSPACE test WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 };
    cqlsh> CREATE TABLE test.words (word text PRIMARY KEY, count int);
    
Load data into the table:

    cqlsh> INSERT INTO test.words (word, count) VALUES ('foo', 20);
    cqlsh> INSERT INTO test.words (word, count) VALUES ('bar', 20);

Now you can read that table as `RDD`:

    val rdd = sc.cassandraTable("test", "words")
    // rdd: com.datastax.spark.connector.rdd.CassandraRDD[com.datastax.spark.connector.rdd.reader.CassandraRow] = CassandraRDD[0] at RDD at CassandraRDD.scala:41

    rdd.toArray.foreach(println)
    // CassandraRow{word: bar, count: 20}
    // CassandraRow{word: foo, count: 20}   

### Reading primitive column values

You can read columns in a Cassandra table using the get methods of the `CassandraRow` object. 
The get methods access individual column values by column name or column index.
Type conversions are applied on the fly. Use `getOption` variants when you expect to receive Cassandra null values.

Continuing with the previous example, follow these steps to access individual column values.
Store the first item of the rdd in the firstRow value.
    
    val firstRow = rdd.first
    // firstRow: com.datastax.spark.connector.rdd.reader.CassandraRow = CassandraRow{word: bar, count: 20}
    
Get the number of columns and column names:

    rdd.columnNames    // Stream(word, count) 
    rdd.size           // 2 

Use one of `getXXX` getters to obtain a column value converted to desired type:
 
    firstRow.getInt("count")       // 20       
    firstRow.getLong("count")      // 20L  

Or use a generic get to query the table by passing the return type directly:

    firstRow.get[Int]("count")                   // 20       
    firstRow.get[Long]("count")                  // 20L
    firstRow.get[BigInt]("count")                // BigInt(20)
    firstRow.get[java.math.BigInteger]("count")  // BigInteger(20)

### Working with nullable data

When reading potentially `null` data, use the `Option` type on the Scala side to prevent getting a `NullPointerException`.

    firstRow.getIntOption("count")        // Some(20)
    firstRow.get[Option[Int]]("count")    // Some(20)    

### Reading collections

You can read collection columns in a Cassandra table using the `getList`, `getSet`, `getMap` or generic `get` 
methods of the `CassandraRow` object. The `get` methods access 
the collection column and return a corresponding Scala collection. 
The generic `get` method lets you specify the precise type of the returned collection.

Assuming you set up the test keyspace earlier, follow these steps to access a Cassandra collection.

In the test keyspace, set up a collection set using cqlsh:

    cqlsh> CREATE TABLE test.users (username text PRIMARY KEY, emails SET<text>);
    cqlsh> INSERT INTO test.users (username, emails) 
         VALUES ('someone', {'someone@email.com', 's@email.com'});

Then in your application, retrieve the first row: 
         
    val row = sc.cassandraTable("test", "users").first
    // row: com.datastax.spark.connector.rdd.reader.CassandraRow = CassandraRow{username: someone, emails: [someone@email.com, s@email.com]}

Query the collection set in Cassandra from Spark:

    row.getList[String]("emails")            // Vector(someone@email.com, s@email.com)
    row.get[List[String]]("emails")          // List(someone@email.com, s@email.com)    
    row.get[Seq[String]]("emails")           // List(someone@email.com, s@email.com)   :Seq[String]
    row.get[IndexedSeq[String]]("emails")    // Vector(someone@email.com, s@email.com) :IndexedSeq[String]
    row.get[Set[String]]("emails")           // Set(someone@email.com, s@email.com)

It is also possible to convert a collection to CQL `String` representation:

    row.get[String]("emails")               // "[someone@email.com, s@email.com]"

A `null` collection is equivalent to an empty collection, therefore you don't need to use `get[Option[...]]` 
with collections.

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

Other conversions might work, but may cause loss of precision or may not work for all values. 
All types are convertible to strings. Converting strings to numbers, dates, 
addresses or UUIDs is possible as long as the string has proper 
contents, defined by the CQL3 standard. Maps can be implicitly converted to/from sequences of key-value tuples.
 
[Next - Server-side data selection and filtering](3_selection.md)
