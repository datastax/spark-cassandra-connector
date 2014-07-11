# Documentation
## Saving datasets to Cassandra

It is possible to save any `RDD` to Cassandra, not just `CassandraRDD`. 
The only requirement is that the object class of `RDD` is a tuple or has property names 
corresponding to Cassandra column names. 

To save an `RDD`, import `com.datastax.spark.connector._` and call the `saveToCassandra` method with the
keyspace name, table name and a list of columns. Make sure to include at least all primary key columns.
 
## Saving a collection of tuples

    collection = sc.parallelize(Seq(("cat", 30), ("fox", 40)))
    collection.saveToCassandra("test", "words", Seq("word", "count"))
    
    cqlsh:test> select * from words;

     word | count
    ------+-------
      bar |    20
      foo |    10
      cat |    30
      fox |    40

    (4 rows)
   
## Saving a collection of objects
When saving a collection of objects of a user-defined class, the items to be saved
must provide appropriately named public property accessors for getting every column
to be saved. This example provides more information on property-column naming conventions is described [here](mapper.md).

    case class WordCount(word: String, count: Long)
    collection = sc.parallelize(Seq(WordCount("dog", 50), WordCount("cow", 60)))    
    collection.saveToCassandra("test", "words", Seq("word", "count"))


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

## Tuning
The following properties set in `SparkConf` can be used to fine-tune the saving process:

  - `cassandra.output.batch.size.rows`: number of rows per single batch; default is 'auto' which means the driver 
     will adjust the number of rows based on the amount of data in each row  
  - `cassandra.output.batch.size.bytes`: maximum total size of the batch in bytes; defaults to 64 kB.
  - `cassandra.output.concurrent.writes`: maximum number of batches executed in parallel by a single Spark task; defaults to 5

[Next - Customizing the object mapping](6_advanced_mapper.md)
