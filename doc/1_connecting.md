# Documentation

## Connecting to Cassandra 
This section describes how Spark connects to Cassandra and 
how to execute CQL statements from Spark applications.

### Preparing `SparkContext` to work with Cassandra

To connect your Spark application to Cassandra, set connection options in the 
`SparkConf` object. These are prefixed with `spark.` so that they can be recognized
from `spark-shell` and set in `$SPARK_HOME/conf/spark-default.conf`.

Example:

```scala
val conf = new SparkConf(true)
        .set("spark.cassandra.connection.host", "192.168.123.10")
        .set("spark.cassandra.auth.username", "cassandra")            
        .set("spark.cassandra.auth.password", "cassandra")

val sc = new SparkContext("spark://192.168.123.10:7077", "test", conf)
```

Multiple hosts can be passed in using a comma-separated list in `spark.cassandra.connection.host`
(e.g. `"127.0.0.1,127.0.0.2"`). These are the *initial contact points only*, all nodes in the local DC will be used upon connecting.

See the reference section for [Cassandra Connection Parameters](reference.md#cassandra-connection-parameters).

### Connection management

Whenever you call a method requiring access to Cassandra, the options in the `SparkConf` object will be used
to create a new connection or to borrow one already open from the global connection cache. 

#### Initial Contact

The initial contact node given in`spark.cassandra.connection.host` can 
be any node of the cluster. The driver will fetch the cluster topology 
from the contact node and will always try to connect to the closest node
in the same data center. If possible, connections are established to the 
same node the task is running on. Consequently, good locality of data 
can be achieved and the amount of data sent across the network is minimized. 

#### Inter-DataCenter Communication is forbidden by default

Connections are never made to data centers other than the data center 
of `spark.cassandra.connection.host`. If some nodes in the local data 
center are down and a read or write operation fails, the operation won't
be retried on nodes in a different data center. This technique guarantees 
proper workload isolation so that a huge analytics job won't disturb
the realtime part of the system.


#### Connection Pooling
Connections are cached internally. If you call two methods needing 
access to the same Cassandra cluster quickly, one after another, or in 
parallel from different threads, they will share the same logical connection 
represented by the underlying Java Driver `Cluster` object.  

This means code like
```scala
  val connector = CassandraConnector(sc.getConf)
  connector.withSessionDo(session => ...)
  connector.withSessionDo(session => ...)
```
or 
```scala
val connector = CassandraConnector(sc.getConf)
sc.parallelize(1 to 100).mapPartitions( it => connector.withSessionDo( session => ...))
```
Will not use more than one `Cluster` object or `Session` object per JVM

Eventually, when all the tasks needing Cassandra connectivity terminate,
the connection to the Cassandra cluster will be closed shortly thereafter. 
The period of time for keeping unused connections open is controlled by 
the global `spark.cassandra.connection.keep_alive_ms` system property, 
see [Cassandra Connection Parameters](reference.md#cassandra-connection-parameters).

### Connecting manually to Cassandra

If you ever need to manually connect to Cassandra in order to issue some CQL statements, 
this driver offers a handy `CassandraConnector` class which can be initialized 
from the `SparkConf` object and provides access to the `Cluster` and 
`Session` objects. `CassandraConnector` instances are serializable
and therefore can be safely used in lambdas passed to Spark transformations
as seen in the examples above.

Assuming an appropriately configured `SparkConf` object is stored 
in the `conf` variable, the following code creates a keyspace and a table:

```scala
import com.datastax.spark.connector.cql.CassandraConnector

CassandraConnector(conf).withSessionDo { session =>
  session.execute("CREATE KEYSPACE test2 WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
  session.execute("CREATE TABLE test2.words (word text PRIMARY KEY, count int)")
}
```

### Connecting to multiple Cassandra Clusters

The Spark Cassandra Connector is able to connect to multiple Cassandra 
Clusters for all of its normal operations.
The default `CassandraConnector` object used by calls to `sc.cassandraTable` and `saveToCassandra` is specified by the `SparkConfiguration`. If you would like to use multiple clusters,
an implicit `CassandraConnector` can be used in a code block to specify 
the target cluster for all operations in that block.

#### Example of reading from one cluster and writing to another

```scala
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._

import org.apache.spark.SparkContext

def twoClusterExample ( sc: SparkContext) = {
  val connectorToClusterOne = CassandraConnector(sc.getConf.set("spark.cassandra.connection.host", "127.0.0.1"))
  val connectorToClusterTwo = CassandraConnector(sc.getConf.set("spark.cassandra.connection.host", "127.0.0.2"))

  val rddFromClusterOne = {
    // Sets connectorToClusterOne as default connection for everything in this code block
    implicit val c = connectorToClusterOne
    sc.cassandraTable("ks","tab")
  }

  {
    //Sets connectorToClusterTwo as the default connection for everything in this code block
    implicit val c = connectorToClusterTwo
    rddFromClusterOne.saveToCassandra("ks","tab")
  }

}
```

[Next - Accessing data with RDDs](2_loading.md)
[Jump to - Accessing data with DataFrames](14_data_frames.md)

