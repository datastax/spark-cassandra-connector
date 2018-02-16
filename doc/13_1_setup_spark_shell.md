# Documentation

## Setting up Cassandra

To install and set up open-source Cassandra, refer to the instructions provided by the
[Apache Cassandra project](https://cassandra.apache.org/doc/latest/).

## Setting up spark

### Download Spark

Download a pre-built Spark from  https://spark.apache.org/downloads.html
Untar the tar.gz downloaded with 

    tar -xvf spark-*-.tgz

### Start Spark in Stand Alone Mode (Optional)

[Official Spark Instructions](https://spark.apache.org/docs/latest/spark-standalone.html)

If you would like to run against a separate executor JVM then you need a running Spark Master and Worker.
By default the spark-shell will run in local mode (driver/master/executor share a jvm.)

Go to the newly created directory and start up Spark in stand-alone mode bound to localhost

    cd spark*
    ./sbin/start-all.sh

At this point you should be able to access the Spark UI at localhost:8080. In the display you
should see a single worker. At the top of this website you should see a URL set for the spark master. Save
the master address (the entire spark://something:7077) if you would like to connect the shell to 
this stand alone spark master (use as sparkMasterAddress below).
