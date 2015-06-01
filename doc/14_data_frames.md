# Documentation

## DataFrames - Experimental

DataFrames provide a new api for manipulating data within Spark. These provide a more user
friendly experience than pure scala for common queries. The Spark Cassandra Connector provides
an integrated DataSource to make creating Cassandra DataFrames easy. 

Spark Docs:
[Data Sources](https://spark.apache.org/docs/latest/sql-programming-guide.html#data-sources)
[Data Frames](https://spark.apache.org/docs/latest/sql-programming-guide.html#dataframes)


### Options
DataSources in Spark take a map of Options which define how the source should act. The
Connector provides a CassandraSource which recognizes the following Key Value pairs.
Those followed with a default of N/A are required, all others are optional. 

| Option Key  | Controls                                              | Values        | Default  |
|-------------|-------------------------------------------------------|---------------|----------|
| c_table     | The Cassandra Table to connect to                     | String        | N/A      |
| keyspace    | The Keyspace where c_table is looked for              | String        | N/A      |
| cluster     | The group of the Cluster Level Settings to inherit    | String        | "default"|
| push_down   | Enables pushing down predicates to C* when applicable | (true,false)  | true     |

####Read, Writing and CassandraConnector Options
Any normal Spark Connector configuration options for Connecting, Reading or Writing
can be passed through as DataFrame options as well. When using the `load` command below these 
options should appear exactly the same as when set in the SparkConf.

####Setting Cluster and Keyspace Level Options
The connector also provides a way to describe the options which should be applied to all
DataFrames within a cluster or within a keyspace. When a property has been specified at the
table level it will override the default keyspace or cluster property.

To add these properties add keys to your `SparkConf` in the format
    
    clusterName:keyspaceName/propertyName.
    
Example Changing Cluster/Keyspace Level Properties
```scala 
val conf = new SparkConf()
  .set("ClusterOne/spark.cassandra.input.split.size","1000") 
  .set("default:test/spark.cassandra.input.split.size","5000")

...

val df = sqlContext.load(
  "org.apache.spark.sql.cassandra", 
   options = Map( "c_table" -> "words", "keyspace" -> "test" 
)// This DataFrame will use a spark.cassandra.input.size of 5000

val otherdf =  sqlContext.load(
  "org.apache.spark.sql.cassandra", 
   options = Map( "c_table" -> "words", "keyspace" -> "test" , "cluster" -> "ClusterOne" )
)// This DataFrame will use a spark.cassandra.input.size of 1000

val lastdf = sqlContext.load(
               "org.apache.spark.sql.cassandra", 
                options = Map( 
                  "c_table" -> "words", 
                  "keyspace" -> "test" ,
                  "cluster" -> "ClusterOne",
                  "spark.cassandra.input.split.size" -> 500
                )
)// This DataFrame will use a spark.cassandra.input.split.size of 500
```

###Creating DataFrames using Load Commands

The most programmatic way to create a data frame is to invoke a `load` command on the SQLContext. 
This function takes a `String` (the DataSource Class) as the first argument and a 
`Map[String,String]` of options as described above.

Example Creating a DataFrame using a Load Command
```scala
val df = sqlContext.load(
  "org.apache.spark.sql.cassandra", 
   options = Map( "c_table" -> "words", "keyspace" -> "test" )
   )
df.show
//word count
//cat  30
//fox  40
```

###Creating DataFrames using Spark SQL

Accessing data Frames using Spark SQL involves creating temporary tables and specifying the
source as `org.apache.spark.sql.cassandra`. The `OPTIONS` passed to this table are used to
establish a relation between the CassandraTable and the internally used DataSource.

Because of a limitation in SparkSQL, SparkSQL `OPTIONS` must have their
`.` characters replaced with `_`. This means `spark.cassandra.input.split.size` becomes 
`spark_cassandra_input_split_size`. 

Example Creating a Source Using Spark SQL:
```scala
//Create Relation with the cassandra table test.words
scala> sqlContext.sql(
   """CREATE TEMPORARY TABLE words 
     |USING org.apache.spark.sql.cassandra 
     |OPTIONS ( 
     |  c_table "words",
     |  keyspace "test", 
     |  cluster "Test Cluster", 
     |  push_down "true", 
     |  spark_cassandra_input_page_row_size "10", 
     |  spark_cassandra_output_consistency_level "ONE", 
     |  spark_cassandra_connection_timeout_ms "1000" 
     |  )""".stripMargin)
scala> val df = sqlContext.sql("SELECT * FROM words")
scala> df.show()
//word count
//cat  30
//fox  40
scala> df.filter(df("count") > 30).show
//word count
//fox  40
```

In addition you can use Spark SQL on the registered tables:
```scala
sqlContext.sql("SELECT * FROM words WHERE word = 'fox'").collect
//Array[org.apache.spark.sql.Row] = Array([fox,40])
```

###Persisting a DataFrame to Cassandra Using the Save Command
DataFrames provide a save function which allows them to persist their data to another
DataSource. The connector supports using this feature to persist a DataFrame a Cassandra
Table.

Example Copying Between Two Tables Using DataFrames
```scala
val df = sqlContext.load(
  "org.apache.spark.sql.cassandra", 
  options = Map( "c_table" -> "words", "keyspace" -> "test" )
)

df.save(
  "org.apache.spark.sql.cassandra",
  options = Map( "c_table" -> "words_copy", "keyspace" -> "test")
)
```