# Documentation
This documentation covers requirements to run the demos, as well as instructions for different configuration and runtime options.

## About The Demos
The Spark Cassandra Connector includes demos containing basic demos, as samples, in both 
[Scala](https://github.com/datastax/spark-cassandra-connector/tree/master/spark-cassandra-connector-demos/src/main/scala/com/datastax/spark/connector/demo) 
and [Java](https://github.com/datastax/spark-cassandra-connector/tree/master/spark-cassandra-connector-demos/src/main/java/com/datastax/spark/connector/demo).
 
    - Read and write to/from Spark and Cassandra
    - WordCount with Spark and Cassandra
    - Copy a table to Cassandra
    - Integrate Spark SQL with Cassandra
    - Integrate Spark Streaming, Kafka and Cassandra 
    - Integrate Spark Streaming, Twitter and Cassandra
    - Integrate Spark Streaming in Akka, Actor DStreams with Cassandra

Most of the above functionality is covered in the Java API demo samples.

## Requirements

### Start Cassandra
Running a demo requires a local Cassandra instance to be running. This can be one node or a cluster.

If you don't already have it, download the latest Apache Cassandra binaries, un-tar, and start Cassandra by invoking:

    $CASSANDRA_HOME/bin/cassandra -f'

### Cassandra Keyspace and Tables
All Scala demos create the Cassandra keyspaces and tables for you, however the Java demos do not. In order to run the Java Demos, you will need to create the following keyspace, table and secondary index in Cassandra via `cqlsh`:

    CREATE KEYSPACE test WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};     
    CREATE TABLE test.people (id INT, name TEXT, birth_date TIMESTAMP, PRIMARY KEY (id));
    CREATE INDEX people_name_idx ON test.people(name);
 
## Running Demos
    
### Settings
#### Simple Demos
The basic demos (WordCountDemo, BasicReadWriteDemo, SQLDemo, AkkaStreamingDemo, etc) set
`spark.master` as 127.0.0.1 or `local[n]`, and `spark.cassandra.connection.host` as 127.0.0.1. Change this locally if desired.
    
#### Kafka Streaming Demo
The Kafka streaming demo sets `spark.master` as 127.0.0.1 or `local[n]`, and `spark.cassandra.connection.host` as 127.0.0.1. Change this locally if desired.

#### Twitter Streaming Demo
The Twitter streaming demo accepts Java system properties passed in on the command line when invoking `sbt run`.
Default configurations are set in the /resources/application.conf file as fallbacks. 
One could run like this, for instance:

    sbt -Dspark.master="mySparkUri" twitter-streaming/run
    
    -Dspark.master, default is local[*]
    -Dspark.master, default is localhost
    -Dspark.cassandra.connection.host, default is 127.0.0.1
    -Dspark.cores.max, default is 2 
 
### Run Via SBT or an IDE and Spark `local[n]`
To run any demo from an IDE, simply right click on a particular demo and 'run'.
To run from SBT read on.

#### Running Any Of The `simple` Demos
On the command line at the root of `spark-cassandra-connector`:
    
    sbt simple-demos/run

Against Scala 2.11:

    sbt -Dscala-2.11=true simple-demos/run
    
And then select which demo you want:
    
    Multiple main classes detected, select one to run:
    
     [1] com.datastax.spark.connector.demo.AkkaStreamingDemo
     [2] com.datastax.spark.connector.demo.BasicReadWriteDemo
     [3] com.datastax.spark.connector.demo.JavaApiDemo
     [4] com.datastax.spark.connector.demo.SQLDemo
     [5] com.datastax.spark.connector.demo.TableCopyDemo
     [6] com.datastax.spark.connector.demo.WordCountDemo
 
#### Running The Kafka Streaming Demo
Spark does not support Kafka streaming or publish the `spark-streaming-kafka`
artifact in their Scala 2.11 build yet. Until then this is only available against Scala 2.10.
On the command line at the root of `spark-cassandra-connector`:

    sbt kafka-streaming/run

#### Running The Twitter Streaming Demo
First you need to set your Twitter auth credentials. This is required by Twitter.
The Twitter streaming sample expects these values to either already exist in the 
deploy environment, and if not found, falls back to acquire from Java system properties.

##### Twitter Authentication
To set Twitter credentials in your deploy environment:

    export TWITTER_CONSUMER_KEY="value"
    export TWITTER_CONSUMER_SECRET="value"
    export TWITTER_ACCESS_TOKEN="value"
    export TWITTER_ACCESS_TOKEN_SECRET="value"
 
To set Twitter credentials in your runtime environment:

    -Dtwitter4j.oauth.consumerKey="value"
    -Dtwitter4j.oauth.consumerSecret="value"
    -Dtwitter4j.oauth.accessToken="value"
    -Dtwitter4j.oauth.accessTokenSecret="value"

##### To Run
On the command line at the root of `spark-cassandra-connector`:

    sbt twitter-streaming/run
        
Or to run with any config overrides:

    sbt -Dspark.master="value" twitter-streaming/run
  

### With Local Spark Standalone  
Start a standalone master server by executing:

    ./sbin/start-master.sh
   
Once started, the master will print out a spark://HOST:PORT URL for itself, which you can use to connect workers
to it, or pass as the "master" argument to SparkContext. You can also find this URL on the master's web UI,
which is http://localhost:8080 by default.

Start one or more workers and connect them to the master via:
    
    ./bin/spark-class org.apache.spark.deploy.worker.Worker spark://IP:PORT
     
Once you have started a worker, look at the master's web UI (http://localhost:8080 by default).
You should see the new node listed there, along with its number of CPUs and memory (minus one gigabyte left for the OS).
 
[Next - Embedded Connector](10_embedded.md)
