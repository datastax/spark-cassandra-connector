# Documentation
## Customizing the mapping between Scala and Cassandra

While mapping rows to tuples and case classes work out-of-the box, 
in some cases you might need more control over Cassandra-Scala mapping. 
For example Java classes are likely to use the JavaBeans naming convention, where accessors are named 
with `get`, `is` or `set` prefixes. 

To customize column-property mappings, 
you need to put an appropriate `ColumnMapper[YourClass]` implicit object in scope. 
Define such an object in a companion object of the class being mapped. 
A `ColumnMapper` affects both loading and saving data. A few special `ColumnMapper` 
implementations are included.

### Working with JavaBeans
To work with Java classes, use `JavaBeanColumnMapper`. 
Make sure your objects are `Serializable`, otherwise Spark won't be able to send them over the network.

    import com.datastax.driver.spark.mapper.JavaBeanColumnMapper
    class WordCount extends Serializable { 
        private var _word: String = ""
        private var _count: Int = 0
        def setWord(word: String) { _word = word }
        def setCount(count: Int) { _count = count }
        override def toString = _word + ":" + _count
    }

    object WordCount {
        implicit object Mapper extends JavaBeanColumnMapper[WordCount] 
    }

    sc.cassandraTable[WordCount]("test", "words").toArray
    // Array(bar:20, foo:10)

To save objects of class `WordCount`, you'll need to define getters.

### Manually specifying the property-name to column-name relationship
If for some reason you wish to associate a column of a different name than the property, 
you may pass a column translation `Map` to a `DefaultColumnMapper` or `JavaBeanColumnMapper`:

    case class WordCount(w: String, c: Int)
    
    object WordCount { 
        implicit object Mapper extends DefaultColumnMapper[WordCount](
            Map("w" -> "word", "c" -> "count")) 
    }

    sc.cassandraTable[WordCount]("test", "words").toArray
    // Array(WordCount(bar,20), WordCount(foo,10))

    sc.parallelize(Seq(WordCount("baz", 30), WordCount("foobar", 40)))
      .saveToCassandra("test", "words", Seq("word", "count"))

### Writing custom `ColumnMapper` implementations
To define column mappings for your classes, create an appropriate implicit object implementing 
`ColumnMapper[YourClass]` trait. This API is subject to future changes, so please refer to the current ScalaDoc.
 
### Low-level control over mapping
The `ColumnMapper` API cannot be used to express every possible mapping, e.g., for classes that do not expose
separate accessors for reading/writing every column. 

For converting a low-level `Row` object obtained from the Cassandra Java driver into an object stored in `RDD`, 
this Spark driver uses a `RowReader` instance. An appropriate `RowReader` is obtained from an implicit 
`RowReaderFactory` resolved based on the target RDD item type. You need to provide a custom implicit 
`RowReaderFactory` and `RowReader` for working with your class, and have it in scope when calling `cassandraTable`.
  
In the same way, when writing an `RDD` back to Cassandra, an appropriate implicit `RowWriterFactory` and 
`RowWriter` are used to extract column values from every RDD item and bind them to an INSERT `PreparedStatement`.
     
Please refer to the ScalaDoc for more details.
