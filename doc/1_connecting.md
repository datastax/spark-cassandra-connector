# Documentation

## Connecting to Cassandra 
This section describes how Spark connects to Cassandra and 
how to execute CQL statements from Spark applications.

### Preparing `SparkContext` to work with Cassandra

To connect your Spark application to Cassandra, set connection options in the 
`SparkConf` object. These are prefixed with `spark.` so that they can be recognized
from the spark-shell and set within the $SPARK_HOME/conf/spark-default.conf.
The following options are available on `SparkConf` object:

Property name                                        | Description                                                       | Default value
-----------------------------------------------------|-------------------------------------------------------------------|--------------------
spark.cassandra.connection.host                      | contact point to connect to the Cassandra cluster                 | address of the Spark master host
spark.cassandra.connection.rpc.port                  | Cassandra thrift port                                             | 9160
spark.cassandra.connection.native.port               | Cassandra native port                                             | 9042
spark.cassandra.connection.conf.factory              | name of a Scala module or class implementing `CassandraConnectionFactory` providing connections to the Cassandra cluster | `com.datastax.spark.connector.cql.DefaultConnectionFactory`
spark.cassandra.connection.keep_alive_ms             | period of time to keep unused connections open                    | 250 ms
spark.cassandra.connection.timeout_ms                | maximum period of time to attempt connecting to a node            | 5000 ms
spark.cassandra.connection.reconnection_delay_ms.min | minimum period of time to wait before reconnecting to a dead node | 1000 ms
spark.cassandra.connection.reconnection_delay_ms.max | maximum period of time to wait before reconnecting to a dead node | 60000 ms
spark.cassandra.connection.compression               | compression to use (LZ4, SNAPPY or NONE)                          | NONE 
spark.cassandra.connection.local_dc                  | the local DC to connect to (other nodes will be ignored)          | None
spark.cassandra.auth.username                        | login name for password authentication                            |
spark.cassandra.auth.password                        | password for password authentication                              |
spark.cassandra.auth.conf.factory                    | name of a Scala module or class implementing `AuthConfFactory` providing custom authentication configuration | `com.datastax.spark.connector.cql.DefaultAuthConfFactory`
spark.cassandra.query.retry.count                    | number of times to retry a timed-out query                        | 10
spark.cassandra.read.timeout_ms                      | maximum period of time to wait for a read to return               | 12000 ms
  
Example:

```scala
val conf = new SparkConf(true)
        .set("spark.cassandra.connection.host", "192.168.123.10")
        .set("spark.cassandra.auth.username", "cassandra")            
        .set("spark.cassandra.auth.password", "cassandra")

val sc = new SparkContext("spark://192.168.123.10:7077", "test", conf)
```

To import Cassandra-specific functions on `SparkContext` and `RDD` objects, call:

```scala
import com.datastax.spark.connector._                                    
```

### Connection management

Whenever you call a method requiring access to Cassandra, the options in the `SparkConf` object will be used
to create a new connection or to borrow one already open from the global connection cache. 
The initial contact node given in
`spark.cassandra.connection.host` can be any node of the cluster. The driver will fetch the cluster topology from 
the contact node and will always try to connect to the closest node in the same data center. If possible, 
connections are established to the same node the task is running on. Consequently, good locality of data can be achieved and the amount 
of data sent across the network is minimized. 

Connections are never made to data centers other than the data center of `spark.cassandra.connection.host`.
If some nodes in the local data center are down and a read or write operation fails, the operation won't be retried on nodes in
a different data center. This technique guarantees proper workload isolation so that a huge analytics job won't disturb
the realtime part of the system.

Connections are cached internally. If you call two methods needing access to the same Cassandra cluster 
quickly, one after another, or in parallel from different threads, they will share the same logical connection 
represented by the underlying Java Driver `Cluster` object.  

Eventually, when all the tasks needing Cassandra connectivity terminate,
the connection to the Cassandra cluster will be closed shortly thereafter. The period of time for keeping unused connections
open is controlled by the global `spark.cassandra.connection.keep_alive_ms` system property, which defaults to 250 ms. 


### Connecting manually to Cassandra

If you ever need to manually connect to Cassandra in order to issue some CQL statements, 
this driver offers a handy `CassandraConnector` class which can be initialized from the `SparkConf` object
and provides access to the `Cluster` and `Session` objects. `CassandraConnector` instances are serializable
and therefore can be safely used in lambdas passed to Spark transformations.

Assuming an appropriately configured `SparkConf` object is stored in the `conf` variable, the following
code creates a keyspace and a table:

```scala
import com.datastax.spark.connector.cql.CassandraConnector

CassandraConnector(conf).withSessionDo { session =>
  session.execute("CREATE KEYSPACE test2 WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
  session.execute("CREATE TABLE test2.words (word text PRIMARY KEY, count int)")
}
```

[Next - Accessing data](2_loading.md)                                        

