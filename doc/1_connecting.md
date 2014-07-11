# Documentation

## Connecting to Cassandra 
This section describes how Spark connects to Cassandra and 
how to execute CQL statements from Spark applications.

### Preparing `SparkContext` to work with Cassandra

To connect your Spark application to Cassandra, set connection options in the 
`SparkConf` object. The following options are available:

Property name                      | Description                                       | Default value
-----------------------------------|---------------------------------------------------|--------------------
cassandra.connection.host          | contact point to connect to the Cassandra cluster | address of the Spark master host
cassandra.connection.rpc.port      | Cassandra thrift port                             | 9160
cassandra.connection.native.port   | Cassandra native port                             | 9042
cassandra.username                 | login name for password authentication            |
cassandra.password                 | password for password authentication              |
cassandra.auth.conf.factory.class  | name of the class implementing `AuthConfFactory` providing custom authentication | `DefaultAuthConfFactory`
  
Example:

    val conf = new SparkConf(true)
            .set("cassandra.connection.host", "192.168.123.10")
            .set("cassandra.username", "cassandra")            
            .set("cassandra.password", "cassandra") 
                         
    val sc = new SparkContext("spark://192.168.123.10:7077", "test", conf)

To import Cassandra-specific functions on `SparkContext` and `RDD` objects, call:

    import com.datastax.spark.connector._                                    


### Connection management

Whenever you call a method requiring access to Cassandra, the options in the `SparkConf` object will be used
to create a new connection or to borrow one already open from the global connection cache. 
The initial contact node given in
`cassandra.connection.host` can be any node of the cluster. The driver will fetch the cluster topology from 
the contact node and will always try to connect to the closest node in the same data center. If possible, 
connections are established to the same node the task is running on. Consequently, good locality of data can be achieved and the amount 
of data sent across the network is minimized. 

Connections are never made to data centers other than the data center of `cassandra.connection.host`.
If some nodes in the local data center are down and a read or write operation fails, the operation won't be retried on nodes in
a different data center. This technique guarantees proper workload isolation so that a huge analytics job won't disturb
the realtime part of the system.

Connections are cached internally. If you call two methods needing access to the same Cassandra cluster 
quickly, one after another, or in parallel from different threads, they will share the same logical connection 
represented by the underlying Java Driver `Cluster` object.  

Eventually, when all the tasks needing Cassandra connectivity terminate,
the connection to the Cassandra cluster will be closed shortly thereafter. The period of time for keeping unused connections
open is controlled by the global `cassandra.connection.keep_alive_ms` system property, which defaults to 250 ms. 


### Connecting manually to Cassandra

If you ever need to manually connect to Cassandra in order to issue some CQL statements, 
this driver offers a handy `CassandraConnector` class which can be initialized from the `SparkConf` object
and provides access to the `Cluster` and `Session` objects. `CassandraConnector` instances are serializable
and therefore can be safely used in lambdas passed to Spark transformations.

Assuming an appropriately configured `SparkConf` object is stored in the `conf` variable, the following
code creates a keyspace and a table:

    import com.datastax.spark.connector.cql.CassandraConnector

    CassandraConnector(conf).withSessionDo { session =>
      session.execute("CREATE KEYSPACE test2 WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
      session.execute("CREATE TABLE test2.words (word text PRIMARY KEY, count int)")
    }


[Next - Accessing data](2_loading.md)                                        
                                        
 