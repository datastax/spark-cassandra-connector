# Configuration Reference


    
## Cassandra Authentication Parameters
**All parameters should be prefixed with <code> spark.cassandra. </code>**

<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Description</th></tr>
<tr>
  <td><code>auth.conf.factory</code></td>
  <td>DefaultAuthConfFactory</td>
  <td>Name of a Scala module or class implementing AuthConfFactory providing custom authentication configuration</td>
</tr>
</table>


## Cassandra Connection Parameters
**All parameters should be prefixed with <code> spark.cassandra. </code>**

<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Description</th></tr>
<tr>
  <td><code>connection.compression</code></td>
  <td></td>
  <td>Compression to use (LZ4, SNAPPY or NONE)</td>
</tr>
<tr>
  <td><code>connection.factory</code></td>
  <td>DefaultConnectionFactory</td>
  <td>Name of a Scala module or class implementing
CassandraConnectionFactory providing connections to the Cassandra cluster</td>
</tr>
<tr>
  <td><code>connection.host</code></td>
  <td>localhost</td>
  <td>Contact point to connect to the Cassandra cluster. A comma seperated list
may also be used. ("127.0.0.1,192.168.0.1")
      </td>
</tr>
<tr>
  <td><code>connection.keep_alive_ms</code></td>
  <td>5000</td>
  <td>Period of time to keep unused connections open</td>
</tr>
<tr>
  <td><code>connection.local_dc</code></td>
  <td>None</td>
  <td>The local DC to connect to (other nodes will be ignored)</td>
</tr>
<tr>
  <td><code>connection.port</code></td>
  <td>9042</td>
  <td>Cassandra native connection port</td>
</tr>
<tr>
  <td><code>connection.reconnection_delay_ms.max</code></td>
  <td>60000</td>
  <td>Maximum period of time to wait before reconnecting to a dead node</td>
</tr>
<tr>
  <td><code>connection.reconnection_delay_ms.min</code></td>
  <td>1000</td>
  <td>Minimum period of time to wait before reconnecting to a dead node</td>
</tr>
<tr>
  <td><code>connection.timeout_ms</code></td>
  <td>5000</td>
  <td>Maximum period of time to attempt connecting to a node</td>
</tr>
<tr>
  <td><code>query.retry.count</code></td>
  <td>60</td>
  <td>Number of times to retry a timed-out query,
Setting this to -1 means unlimited retries</td>
</tr>
<tr>
  <td><code>query.retry.delay</code></td>
  <td>4 * 1.5</td>
  <td>The delay between subsequent retries (can be constant,
 like 1000; linearly increasing, like 1000+100; or exponential, like 1000*2)</td>
</tr>
<tr>
  <td><code>read.timeout_ms</code></td>
  <td>120000</td>
  <td>Maximum period of time to wait for a read to return </td>
</tr>
</table>


## Cassandra DataFrame Source Parameters
**All parameters should be prefixed with <code> spark.cassandra. </code>**

<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Description</th></tr>
<tr>
  <td><code>sql.pushdown.additionalClasses</code></td>
  <td></td>
  <td>A comma seperated list of classes to be used (in order) to apply additional
 pushdown rules for C* Dataframes. Classes must implement CassandraPredicateRules
      </td>
</tr>
<tr>
  <td><code>table.size.in.bytes</code></td>
  <td>None</td>
  <td>Used by DataFrames Internally, will be updated in a future release to
retrieve size from C*. Can be set manually now</td>
</tr>
</table>


## Cassandra SQL Context Options
**All parameters should be prefixed with <code> spark.cassandra. </code>**

<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Description</th></tr>
<tr>
  <td><code>sql.cluster</code></td>
  <td>default</td>
  <td>Sets the default Cluster to inherit configuration from</td>
</tr>
<tr>
  <td><code>sql.keyspace</code></td>
  <td>None</td>
  <td>Sets the default keyspace</td>
</tr>
</table>


## Cassandra SSL Connection Options
**All parameters should be prefixed with <code> spark.cassandra. </code>**

<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Description</th></tr>
<tr>
  <td><code>connection.ssl.clientAuth.enabled</code></td>
  <td>false</td>
  <td>Enable 2-way secure connection to Cassandra cluster</td>
</tr>
<tr>
  <td><code>connection.ssl.enabled</code></td>
  <td>false</td>
  <td>Enable secure connection to Cassandra cluster</td>
</tr>
<tr>
  <td><code>connection.ssl.enabledAlgorithms</code></td>
  <td>Set(TLS_RSA_WITH_AES_128_CBC_SHA, TLS_RSA_WITH_AES_256_CBC_SHA)</td>
  <td>SSL cipher suites</td>
</tr>
<tr>
  <td><code>connection.ssl.keyStore.password</code></td>
  <td>None</td>
  <td>Key store password</td>
</tr>
<tr>
  <td><code>connection.ssl.keyStore.path</code></td>
  <td>None</td>
  <td>Path for the key store being used</td>
</tr>
<tr>
  <td><code>connection.ssl.keyStore.type</code></td>
  <td>JKS</td>
  <td>Key store type</td>
</tr>
<tr>
  <td><code>connection.ssl.protocol</code></td>
  <td>TLS</td>
  <td>SSL protocol</td>
</tr>
<tr>
  <td><code>connection.ssl.trustStore.password</code></td>
  <td>None</td>
  <td>Trust store password</td>
</tr>
<tr>
  <td><code>connection.ssl.trustStore.path</code></td>
  <td>None</td>
  <td>Path for the trust store being used</td>
</tr>
<tr>
  <td><code>connection.ssl.trustStore.type</code></td>
  <td>JKS</td>
  <td>Trust store type</td>
</tr>
</table>


## Custom Cassandra Type Parameters (Expert Use Only)
**All parameters should be prefixed with <code> spark.cassandra. </code>**

<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Description</th></tr>
<tr>
  <td><code>dev.customFromDriver</code></td>
  <td>None</td>
  <td>Provides an additional class implementing CustomDriverConverter for those
clients that need to read non-standard primitive Cassandra types. If your C* implementation
uses a Java Driver which can read DataType.custom() you may need it this. If you are using
OSS Cassandra this should never be used.</td>
</tr>
</table>


## Read Tuning Parameters
**All parameters should be prefixed with <code> spark.cassandra. </code>**

<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Description</th></tr>
<tr>
  <td><code>input.consistency.level</code></td>
  <td>LOCAL_ONE</td>
  <td>Consistency level to use when reading	</td>
</tr>
<tr>
  <td><code>input.fetch.size_in_rows</code></td>
  <td>1000</td>
  <td>Number of CQL rows fetched per driver request</td>
</tr>
<tr>
  <td><code>input.join.throughput_query_per_sec</code></td>
  <td>9223372036854775807</td>
  <td>Maximum read throughput allowed per single core in query/s while joining RDD with C* table</td>
</tr>
<tr>
  <td><code>input.metrics</code></td>
  <td>true</td>
  <td>Sets whether to record connector specific metrics on write</td>
</tr>
<tr>
  <td><code>input.split.size_in_mb</code></td>
  <td>64</td>
  <td>Approx amount of data to be fetched into a Spark partition</td>
</tr>
</table>


## Write Tuning Parameters
**All parameters should be prefixed with <code> spark.cassandra. </code>**

<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Description</th></tr>
<tr>
  <td><code>output.batch.grouping.buffer.size</code></td>
  <td>1000</td>
  <td> How many batches per single Spark task can be stored in
memory before sending to Cassandra</td>
</tr>
<tr>
  <td><code>output.batch.grouping.key</code></td>
  <td>Partition</td>
  <td>Determines how insert statements are grouped into batches. Available values are
<ul>
  <li> <code> none </code> : a batch may contain any statements </li>
  <li> <code> replica_set </code> : a batch may contain only statements to be written to the same replica set </li>
  <li> <code> partition </code> : a batch may contain only statements for rows sharing the same partition key value </li>
</ul>
</td>
</tr>
<tr>
  <td><code>output.batch.size.bytes</code></td>
  <td>1024</td>
  <td>Maximum total size of the batch in bytes. Overridden by
spark.cassandra.output.batch.size.rows
    </td>
</tr>
<tr>
  <td><code>output.batch.size.rows</code></td>
  <td>None</td>
  <td>Number of rows per single batch. The default is 'auto'
which means the connector will adjust the number
of rows based on the amount of data
in each row</td>
</tr>
<tr>
  <td><code>output.concurrent.writes</code></td>
  <td>5</td>
  <td>Maximum number of batches executed in parallel by a
 single Spark task</td>
</tr>
<tr>
  <td><code>output.consistency.level</code></td>
  <td>LOCAL_QUORUM</td>
  <td>Consistency level for writing</td>
</tr>
<tr>
  <td><code>output.ifNotExists</code></td>
  <td>false</td>
  <td>Determines that the INSERT operation is not performed if a row with the same primary
key already exists. Using the feature incurs a performance hit.</td>
</tr>
<tr>
  <td><code>output.ignoreNulls</code></td>
  <td>false</td>
  <td> In Cassandra >= 2.2 null values can be left as unset in bound statements. Setting
this to true will cause all null values to be left as unset rather than bound. For
finer control see the CassandraOption class</td>
</tr>
<tr>
  <td><code>output.metrics</code></td>
  <td>true</td>
  <td>Sets whether to record connector specific metrics on write</td>
</tr>
<tr>
  <td><code>output.throughput_mb_per_sec</code></td>
  <td>2.147483647E9</td>
  <td>*(Floating points allowed)* <br> Maximum write throughput allowed
 per single core in MB/s. <br> Limit this on long (+8 hour) runs to 70% of your max throughput
 as seen on a smaller job for stability</td>
</tr>
<tr>
  <td><code>output.ttl</code></td>
  <td>0</td>
  <td>Time To Live(TTL) assigned to writes to Cassandra. A value of 0 means no TTL</td>
</tr>
</table>
