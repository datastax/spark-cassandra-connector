# To run the demos:
Running the demos takes 3 simple steps:

## Start Cassandra
### Using CCM 
For a local cluster that's already configured:
(http://www.datastax.com/dev/blog/ccm-a-development-tool-for-creating-local-cassandra-clusters)  
    
    sudo ccm start
     
To use Apache Cassandra binaries start up Cassandra by invoking
    
    $CASSANDRA_HOME/bin/cassandra -f'
    
## Start Spark
### Start a standalone master server by executing:
    ./sbin/start-master.sh
   
Once started, the master will print out a spark://HOST:PORT URL for itself, which you can use to connect workers
to it, or pass as the “master” argument to SparkContext. You can also find this URL on the master’s web UI,
which is http://localhost:8080 by default.
### Start one or more workers and connect them to the master via:
    
    ./bin/spark-class org.apache.spark.deploy.worker.Worker spark://IP:PORT
     
Once you have started a worker, look at the master’s web UI (http://localhost:8080 by default).
You should see the new node listed there, along with its number of CPUs and memory (minus one gigabyte left for the OS).
 
## Run the demo 
From SBT, run the following on the comman line, then enter the number of the demo you wish to run:
    
    sbt spark-cassandra-connector-demos/run
 
 

Or from an IDE, right click on a particular demo and 'run'.

