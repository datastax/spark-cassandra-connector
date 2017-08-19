# Documentation
## Performance monitoring

The Spark Cassandra Connector utilizes the Codahale metrics system to expose information on the
latency and throughput of Cassandra operations.

### Internal Spark metrics
Spark internal metrics are visible in Spark UI. The user is able to browse application stages and
particular tasks along with the amount of data which was read, written and how long it took.

Because of a limitation in the Spark metrics system the amount of data that has been read or written
in a task will be marked as a Hadoop operation. Spark doesn't allow custom labels in their metric
system and the Connector is not actually passing data through Hadoop. However, it doesn't really
matter because it is just label.

### Codahale metrics
The Connector metrics are also exposed through Spark's metric system in both the executor and the
driver. To access these metrics add a new source called cassandra-connector in your
`metrics.properties` file. Example:

```
executor.source.cassandra-connector.class=org.apache.spark.metrics.CassandraConnectorSource
driver.source.cassandra-connector.class=org.apache.spark.metrics.CassandraConnectorSource
```

### Performance impact
While there should be a minimal performance effect from collecting metrics, Metric collection can be
disabled. Codahale metrics are not collected if CassandraConnectorSource is not specified in the
metrics configuration file. In order to disable task metrics, use these properties in Spark
configuration:

- `spark.cassandra.input.metrics` - set to `false` to disable collection of input task metrics
- `spark.cassandra.output.metrics` - set to `false` to disable collection of output task metrics

### Available metrics
Metric name                | Unit description
---------------------------|---------------------------------------------------------------
write-byte-meter           | Number of bytes written to Cassandra
write-row-meter            | Number of rows written to Cassandra
write-batch-timer          | Batch write time length
write-batch-wait-timer     | The length of time batches sit in the queue before being submitted to Cassandra
write-batch-size-histogram | The distribution of the rows in batches
write-task-timer           | Timer to measure time of writing a single partition
write-success-counter      | Number successfully written batches
write-failure-counter      | Number of failed batches
read-byte-meter            | Number of bytes read from Cassandra
read-row-meter             | Number of rows read from Cassandra
read-task-timer            | Timer to measure time of reading a single partition

[Next - Building And Artifacts](12_building_and_artifacts.md)