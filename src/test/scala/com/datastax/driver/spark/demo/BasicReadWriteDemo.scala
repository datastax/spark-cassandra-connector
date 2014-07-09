package com.datastax.driver.spark.demo

object BasicReadWriteDemo extends App with DemoApp {

  import com.datastax.driver.spark._

  // Read table test.kv and print its contents:
  val rdd = sc.cassandraTable("test", "kv").select("key", "value")
  rdd.toArray().foreach(println)

  // Write two rows to the test.kv table:
  val col = sc.parallelize(Seq((1, "value 1"), (2, "value 2")))
  col.saveToCassandra("test", "kv", Seq("key", "value"))

  sc.stop()
}
