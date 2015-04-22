# Documentation

## The Spark Shell and Spark Cassandra Connector

These instructions were last confirmed with C* 2.0.11, Spark 1.2.1 and Connector 1.2.0-rc3

### Setting up spark

#### Download Spark

Download spark from  https://spark.apache.org/downloads.html
Untar the tar.gz downloaded with 

    tar -xvf spark-*-.tgz


#### Start Spark in Stand Alone Mode (Optional)
If you would like to run against a separate executor JVM you need a running Spark Master and Worker.
By default the spark-shell will run in local mode (driver/master/executor share a jvm.)

Go to the newly created directory and start up Spark in stand-alone mode bound to localhost

    cd spark*
    ./sbin/start-all.sh
    
At this point you should be able to access the Spark UI at localhost:8080. In the display you
should see a single worker. On this website it will give you a URL set for the spark master. Save
the master address (the entire spark://something:7077) if you would like to connect the shell to 
this stand alone spark master.

### Clone and assemble the Spark Cassandra Connector

    git clone git@github.com:datastax/spark-cassandra-connector.git 
    cd spark-cassandra-connector
    git checkout b1.2 ## Replace this with the version of the connector you would like to use
    ./sbt/sbt  assembly
    ls spark-cassandra-connector/target/scala-2.10/*  
    ## Should have a spark-cassandra-connector-assembly-*.jar here
    
### Start the Spark Shell 
If you don't include the master address below the spark shell will run in Local mode. 

    cd spark-*
    #Include the --master if you want to run against a stand alone spark and not local mode
    ./bin/spark-shell [--master sparkmasteraddress] --jars yourAssemblyJar --conf spark.cassandra.connection.host=yourCassandraClusterIp
### Import connector classes
    
    import com.datastax.spark.connector._ //Imports basic rdd functions
    import com.datastax.spark.connector.cql._ //(Optional) Imports java driver helper functions
    
### Test it out
    val c = CassandraConnector(sc.getConf)
    c.withSessionDo ( session => session.execute("CREATE KEYSPACE test WITH replication={'class':'SimpleStrategy', 'replication_factor':1}")
    c.withSessionDo ( session => session.execute("CREATE TABLE test.fun (k int PRIMARY KEY, v int)"))
    sc.parallelize(1 to 100).map( x => (x,x)).saveToCassandra("test","fun")
    sc.cassandraTable("test","fun").take(3)
    // Your results may differ 
    //res1: Array[com.datastax.spark.connector.CassandraRow] = Array(CassandraRow{k: 60, v: 60}, CassandraRow{k: 67, v: 67}, CassandraRow{k: 10, v: 10})
    
