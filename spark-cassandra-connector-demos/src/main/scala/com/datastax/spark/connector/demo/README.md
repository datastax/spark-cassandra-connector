# To run the demos:

The new 'embedded' module is leveraged to provide an embedded Cassandra and Spark local instance.
It is also leveraged for an embedded Kafka and Zookeeper local instance for the Kafka demo.

    Make sure you don't already have Cassandra or Spark running locally.

    If you already use SBT, no setup is needed. 

## SBT: Scala Build Tool
The connector is an SBT project. It is very easy to set up:

* [Download SBT](http://www.scala-sbt.org/download.html) 
* [SBT Setup](http://www.scala-sbt.org/0.13/tutorial/Manual-Installation.html) 

## Run the demo 
From SBT, run the following on the comman line, then enter the number of the demo you wish to run:
    
    sbt spark-cassandra-connector-demos/run
 

Or from an IDE, right click on a particular demo and 'run'.

