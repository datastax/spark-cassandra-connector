# Spark Streaming with Cassandra
Spark Streaming extends the core API to allow high-throughput, fault-tolerant stream processing of live data streams.
Data can be ingested from many sources:  
    Kafka, Flume, Twitter, ZeroMQ,TCP sockets, etc.

Results can be stored in Cassandra.

## The Basic Idea

### Spark Streaming
Here is a basic streaming sample:

Create a StreamingContext with a SparkConf configuration
```scala
    val ssc = new StreamingContext(sparkConf, Seconds(1))
```

Create a DStream that will connect to serverIP:serverPort 
```scala
    val lines = ssc.socketTextStream(serverIP, serverPort)
```

Count each word in each batch
```scala
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
```

Print a few of the counts to the console.
Start the computation.
```scala
    wordCounts.print()
    ssc.start()  
    ssc.awaitTermination() // Wait for the computation to terminate
```

### Spark Streaming With Cassandra
Simply pipe the output to Cassandra vs the console:

```scala
    import org.apache.spark.streaming._
    import org.apache.spark.streaming.StreamingContext._
    wordCounts.saveToCassandra("streaming_test", "words")
```

## Setting up `StreamingContext`
Follow the directions for [creating a `SparkConf`](0_quick_start.md)

Create a `StreamingContext`:

```scala
    val ssc = new StreamingContext(conf, Seconds(n))
```

Enable Cassandra-specific functions on the `StreamingContext`, `DStream` and `RDD`:

```scala
    import com.datastax.spark.connector._
    import com.datastax.spark.connector.streaming._
```

Create any of the available or custom Spark streams, for example an Akka Actor stream:

```scala
    val stream = ssc.actorStream[String](Props[TypedStreamingActor[String]], "stream", StorageLevel.MEMORY_AND_DISK)
```

Writing to Cassandra from a Stream:

```scala
    val wc = stream.flatMap(_.split("\\s+"))
        .map(x => (x, 1))
        .reduceByKey(_ + _)
        .saveToCassandra("streaming_test", "words", SomeColumns("word", "count"))
```

## Find out more
http://spark.apache.org/docs/latest/streaming-programming-guide.html