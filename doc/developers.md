# Documentation

## Developers Tips

### Getting Started

The Spark Cassandra Connector is built using sbt. There is a premade
launching script for sbt so it is unneccessary to download it. To invoke
this script you can run `./sbt/sbt` from a clone of this repository.

For information on setting up your clone please follow the [Github 
Help](https://help.github.com/articles/cloning-a-repository/)

Once in the sbt shell you will be able to build and run tests for the
connector without any Spark or Cassandra nodes running. The most common
commands to use when developing the connector are

1. `test` - Runs the the unit tests for the project.
2. `it:test` - Runs the integeration tests with embedded Cassandra and Spark
3. `assembly` - Builds a fat jar for using with --jars in spark submit or spark-shell

The integration tests located in `spark-cassandra-connector/src/it` should
probably be the first place to look for anyone considering adding code.
There are many examples of executing a feature of the connector with
the embedded Cassandra and Spark nodes and are the core of our test 
coverage.

### Sub-Projects

The connector currently contains several subprojects
#### spark-cassandra-connector
This sub project contains all of the actual connector code and is where
any new features or tests should go. This Scala project also contains the
Java api and related code.

It includes the code for building reference documentation. This 
automatically determines what belongs in the reference file. It should 
mostly be used for regenerating the reference file after new parameters 
have been added or old parameters have been changed. Tests will throw 
errors if the reference file is not up to date. To fix this run 
`spark-cassandra-connector-unshaded/run` to update the file. It is still 
necessary to commit the changed file after running this sub-project.

#### spark-cassandra-connector-embedded
The code used to start the embedded services used in the integration tests. 
This contains methods for starting up Cassandra as a thread within the running
test code.

#### spark-cassandra-connector-perf
Code for performance based tests. Add any performance comparisons needed
to this project.

### Continuous Testing

It's often useful when implementing new features to have the tests run
in a loop on code change. Sbt provides a method for this by using the
`~` operator. With this `~ test` will run the unit tests every time a
change in the source code is detected. This is often useful to use in
conjunction with `testOnly` which runs a single test. So if a new feature
were being added to the integration suite `foo` you may want to run
`~ it:testOnly foo`. Which would only run the suite you are interested in
on a loop while you are modifying the connector code.

### Packaging

The `spark-shell` and `spark-submit` are able to use local caches to load
libraries and this can be taken advantage of by the SCC. For example
if you wanted to test the maven artifacts produced for your current build
you could run `publishM2` which would generate the needed artifacts and
pom in your local cache. You can then reference this from `spark-shell`
or `spark-submit` using the following code 
```bash
./bin/spark-shell --repositories file:/Users/yourUser/.m2/repository --packages com.datastax.spark:spark-cassandra-connector_2.10:1.6.0-14-gcfca49e
```
Where you would change the revision `1.6.0-14-gcfca49e` to match the output
of your publish command. 

This same method should work with `publishLocal`
after the merging of [SPARK-12666](https://issues.apache.org/jira/browse/SPARK-12666)



