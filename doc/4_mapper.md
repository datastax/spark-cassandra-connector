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
    
```scala
class WordCount extends Serializable {
  var word: String = ""
  var count: Int = 0    
}
```       
     
[Next - Saving data](5_saving.md)
