# Documentation
## Working with user-defined case classes and tuples

This section describes how to store Cassandra rows in Scala tuples or objects of your own classes.

### Mapping rows to tuples
Instead of mapping your Cassandra rows to objects of the `CassandraRow` class, you can directly 
unwrap column values into tuples of desired type.
 
```scala
sc.cassandraTable[(String, Int)]("test", "words").select("word", "count").toArray
// Array((bar,20), (foo,10))

sc.cassandraTable[(Int, String)]("test", "words").select("count", "word").toArray
// Array((20,bar), (10,foo))
```    

### Mapping rows to (case) objects
Define a case class with properties named the same as the Cassandra columns. 
For multi-word column identifiers, separate each word by an underscore in Cassandra, 
and use the camel case convention on the Scala side. Then provide the explicit class name
when invoking `cassandraTable`:

#### Example Mapping a Cassandra Row to a Scala Case Class
```scala
case class WordCount(word: String, count: Int)
sc.cassandraTable[WordCount]("test", "words").toArray
// Array(WordCount(bar,20), WordCount(foo,10))
```

The column-property naming convention is:

Cassandra column name	| Scala property name
------------------------|---------------------
`count`	                | `count`
`column_1`	            | `column1`
`user_name`	            | `userName`

Using the same property names as columns also works:

Cassandra column name	| Scala property name
------------------------|---------------------
`COUNT`                 | `COUNT`
`column_1`	            | `column_1`
`user_name`	            | `user_name`

The class doesn't necessarily need to be a case class. The only requirements are:

  - it must be `Serializable`
  - it must have a constructor with parameter names and types matching the columns
  - it must be compiled with debug information, so it is possible to read parameter names at runtime

Property values might be also set by Scala-style setters. The following class is also compatible:

#### Example of Mappable Standard Scala Class
```scala
class WordCount extends Serializable {
  var word: String = ""
  var count: Int = 0    
}
```       

### Using explicitly specified property names
It is possible to specify property names explicitly when rows are mapped 
to objects. In order to do this, you need to use `as` method on a 
selected column name.

#### Example Mapping a Cassandra Column to a Differently Named Scala Class Property
Say, we have a table with columns `word TEXT` and `num INT`. We would like to map rows from this
table to the objects of class with fields `word: String` and `count: Int`:

```scala
case class WordCount(word: String, count: Int)
val result = sc.cassandraTable[WordCount]("test", "words").select("word", "num" as "count").collect()
```

The `as` method can be used for any type of projected value: normal column, TTL or write time:

```scala
sc.cassandraTable[SomeClass]("test", "table").select(
    "no_alias",
    "simple" as "simpleProp",
    "simple".ttl as "simplePropTTL",
    "simple".writeTime as "simpleWriteTime")
```

### Mapping rows to pairs of objects
You can also map rows to pairs of objects or tuples so that it resemble a mapping key to values.
It is convenient to represent data from Cassandra as an RDD of pairs where the first component is
the primary key and the second one includes all the remaining columns.

Suppose we have a table with the following schema:

```sql
CREATE TABLE test.users (
  user_name     TEXT,
  domain        TEXT,
  password_hash TEXT,
  last_visit    TIMESTAMP,
  PRIMARY KEY (domain, user_name)
);

INSERT INTO test.users (user_name, domain, password_hash, last_visit) VALUES ('john', 'datastax.com', '1234', '2014-06-05');
```

We can map each rows of this table into a key-value pair by using the `keyBy` 
method of `CassandraTableScanRDD` class. Using `keyBy` also has performance
implications see (partitioning)[16_partitioning.md]

#### Example using keyBy to Map a Cassandra Table to Pairs of Objects
```scala
import org.joda.time.DateTime
case class UserId(userName: String, domain: String)
case class UserData(passwordHash: String, lastVisit: DateTime)

sc.cassandraTable[UserData]("test", "users").keyBy[UserId]

sc.cassandraTable[UserData]("test", "users").keyBy[(String, String)]("user_name", "domain")

sc.cassandraTable[(String, DateTime)]("test", "users")
  .select("password_hash", "last_visit", "user_name", "domain")   
  .keyBy[(String, String)]("user_name", "domain")

```

### Mapping User Defined Types
User Defined Types (UDTs) can be mapped similarly to rows by making a 
class that has a field for every element in the UDT. For example
``` scala
case class Address(street: String, city: String, zip: Int)
case class ClassWithUDT(key: Int, name: String, addr: Address)
```

ClassWithUDT now can map to row in a CassandraTable with the following 
schema
``` sql
CREATE TYPE ks.address (street text, city text, zip int)
CREATE TABLE $ks.udts(key INT PRIMARY KEY, name text, addr frozen<address>)
```

[Next - Saving data](5_saving.md)
