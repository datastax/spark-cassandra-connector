# Documentation
## Performance monitoring

Spark bases on Codahale metrics system as well as it uses some internal measurements to provide
the amount of data which was read or written.

### Internal Spark metrics
Spark internal metrics are visible in Spark UI. The user is able to browse application stages and
particular tasks along with the amount of data which was read, written and how long did it take.
The amounts of data transferred from or to Cassandra are reported by Spark Cassandra Connector to
Spark. Unfortunately these values are reported as being exchanged with Hadoop because Spark doesn't
allow to specify any different external IO component type. However, it doesn't really matter
because it is just label.

### Codahale metrics
Spark Cassandra Connector also connects to Spark Codahale based metrics in executors and in the
driver. It defines a new metrics source, called `cassandra-connector`, so to collect the statistics
the user needs to configure that source in `metrics.properties` file. In its simplest form, it may
look like as follows:

```
cassandra-connector.sink.csv.class=org.apache.spark.metrics.sink.CsvSink
cassandra-connector.sink.csv.period=5
cassandra-connector.sink.csv.unit=seconds
cassandra-connector.sink.csv.directory=/tmp/spark/sink
```

### Performance impact
Collecting metrics may impact the performance you the tasks. If the user finds them affecting
the performance too much, they can be disabled by setting the following options in Spark
configuration:

- `spark.cassandra.input.metrics` - set to `false` to disable collection of input metrics
- `spark.cassandra.output.metrics` - set to `false` to disable collection of output metrics

### Kinds of metrics
Metric name            | Unit description
-----------------------|---------------------------------------------------------------
write-byte-meter       | Number of bytes which was written to Cassandra
write-row-meter        | Number of rows which was written to Cassandra
write-batch-timer      | Batch writing time
write-batch-wait-timer | Time which the batch needs to wait before it is submitted
write-task-timer       | Total time of the single write task (single partition)
write-success-counter  | Number successfully written batches
write-failure-counter  | Number of failed batches
read-byte-meter        | Number of bytes which was read from Cassandra
read-row-meter         | Number of rows which was read from Cassandra
read-more-rows-timer   | Time of fetching more rows
read-task-timer        | Total time of the single read task (single partition)

