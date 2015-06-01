# Documentation

## Using the Spark Cassandra Connector with the Spark Shell 

These instructions were last confirmed with C* 2.0.11, Spark 1.2.1 and Connector 1.2.1.

For this guide, we assume an existing Cassandra deployment, running either locally or on a cluster, a local installation of Spark and an optional Spark cluster. For detail setup instructions see [setup spark-shell](13_1_setup_spark_shell.md)   

To use the Spark Cassandra Connector from within the Spark Shell, we need to load the Connector and all its dependencies in the shell context. The easiest way to achieve that is to build an assembly (also known as _"fat jar"_) that packages all dependencies.

### Building the Spark Cassandra Connector assembly 

```bash
git clone git@github.com:datastax/spark-cassandra-connector.git 
cd spark-cassandra-connector
git checkout tags/v1.2.1 ## Replace this with the version of the connector you would like to use
./sbt/sbt assembly
```

Sanity check: 
```bash
ls spark-cassandra-connector/target/scala-2.10/
## Should have a spark-cassandra-connector-assembly-*.jar here, copy full path to use as yourAssemblyJar below)
```

### Starting the Spark Shell 
If you don't include the master address below the spark shell will run in Local mode. 
  
```bash
cd spark/install/dir
#Include the --master if you want to run against a spark cluster and not local mode
./bin/spark-shell [--master sparkMasterAddress] --jars yourAssemblyJar --conf spark.cassandra.connection.host=yourCassandraClusterIp
```

By default spark will log everything to the console and this may be a bit of an overload. To change this copy and modify the `log4j.properties` template file
```bash
cp conf/log4j.properties.template conf/log4j.properties
```

Changing the root logger at the top from INFO to WARN will significantly reduce the verbosity.

## Example

### Import connector classes
```scala    
import com.datastax.spark.connector._ //Imports basic rdd functions
import com.datastax.spark.connector.cql._ //(Optional) Imports java driver helper functions
```
    
### Test it out
``` scala
val c = CassandraConnector(sc.getConf)
c.withSessionDo ( session => session.execute("CREATE KEYSPACE test WITH replication={'class':'SimpleStrategy', 'replication_factor':1}"))
c.withSessionDo ( session => session.execute("CREATE TABLE test.fun (k int PRIMARY KEY, v int)"))


// Your results may differ 
//res1: Array[com.datastax.spark.connector.CassandraRow] = Array(CassandraRow{k: 60, v: 60}, CassandraRow{k: 67, v: 67}, CassandraRow{k: 10, v: 10})
```

## Creating a playground with Docker

We can use Docker to quickly create a working setup without the need to install and configure Cassandra.

For this guide we will use the Cassandra docker image maintained by [Al Tobert on GitHub](https://github.com/tobert/cassandra-docker/blob/master/README.md)

First, let's pull the docker image:
```bash
docker pull tobert/cassandra
```

To instantiate a Cassandra container, we need to specify a host volume where the data will be stored. We store the resulting container id in the variable `CASSANDRA_CONTAINER_ID`. We will need that id afterwards to lookup the IP address of the running instance.
```bash
mkdir /srv/cassandra
CASSANDRA_CONTAINER_ID=`docker run -d -v /srv/cassandra:/data tobert/cassandra`
```

Alternatively, if the data is disposable (e.g. tests), we can omit the volume, resulting in the following step:
```bash
CASSANDRA_CONTAINER_ID=$(docker run -d tobert/cassandra)
```

Now, we obtain the IP Address that docker assigned to the container:
```bash
CASSANDRA_CONTAINER_IP=$(docker inspect -f '{{ .NetworkSettings.IPAddress }}' $CASSANDRA_CONTAINER_ID)
```

And we can start the spark shell using that running container:

```bash
cd spark/install/dir
#Include the --master if you want to run against a spark cluster and not local mode
./bin/spark-shell [--master sparkMasterAddress] --jars yourAssemblyJar --conf spark.cassandra.connection.host=$IP
```

[Next - DataFrames](14_data_frames.md) 
