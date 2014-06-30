package com.datastax.driver.spark.demo


import org.apache.spark.{SparkConf, SparkContext}

object BasicReadWriteDemo extends App {

  import com.datastax.driver.spark._

  val sparkMasterHost = "127.0.0.1"
  val cassandraHost = "127.0.0.1"
  val keyspace = "test"
  val table = "kv"
  
  // Tell Spark the address of one Cassandra node:
  val conf = new SparkConf(true).set("cassandra.connection.host", cassandraHost)

  // Connect to the Spark cluster:
  val sc = new SparkContext("spark://" + sparkMasterHost + ":7077", "demo-program", conf)

  // Read table test.kv and print its contents:
  val rdd = sc.cassandraTable("test", "kv").select("key", "value")
  rdd.toArray().foreach(println)

  // Write two rows to the test.kv table:
  val col = sc.parallelize(Seq((1, "value 1"), (2, "value 2")))
  col.saveToCassandra("test", "kv", Seq("key", "value"))

  sc.stop()
}
