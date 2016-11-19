package com.datastax.spark.connector.rdd.typeTests

/**
 * Counters don't allow inserts and updates in the same pattern as teh other types so
 * we need to test them differently.
 */

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.YamlTransformations

import scala.concurrent.Future

class CounterTypeTest  extends SparkCassandraITFlatSpecBase  {
  useCassandraConfig(Seq(YamlTransformations.Default))
  useSparkConf(defaultConf)

  override val conn = CassandraConnector(defaultConf)

  override def beforeAll() {
    //Create Cql3 Keypsace and Data
    counterCreateData()
  }

  val keyspaceName = "counter_ks"

  def counterCreateData() = conn.withSessionDo{ session =>
    createKeyspace(session, keyspaceName)

    session.execute("use counter_ks")

    val create_table = "CREATE TABLE IF NOT EXISTS counter_norm ( key bigint PRIMARY KEY, expectedkeys counter )"
    val create_table_cs = "CREATE TABLE IF NOT EXISTS counter_cs ( key bigint PRIMARY KEY, expectedkeys counter ) WITH COMPACT STORAGE"
    val create_table_write = "CREATE TABLE IF NOT EXISTS counter_write ( name text, key bigint , expectedkeys counter, PRIMARY KEY(name,key))"
    val create_table_multi_write = "CREATE TABLE IF NOT EXISTS counter_multi_write ( name text, key bigint , data1 counter, data2 counter, data3 counter, PRIMARY KEY(name,key))"
    awaitAll(
      Future(session.execute(create_table)),
      Future(session.execute(create_table_cs)),
      Future(session.execute(create_table_write)),
      Future(session.execute(create_table_multi_write))
    )
    (1L to 10L).foreach { x => (1 to 10).foreach(_ => session.execute("UPDATE counter_norm SET expectedkeys = expectedKeys + 1 WHERE key = ?", x: java.lang.Long))}
    (1L to 10L).foreach { x => (1 to 10).foreach(_ => session.execute("UPDATE counter_cs SET expectedkeys = expectedKeys + 1 WHERE key = ?", x: java.lang.Long))}
  }

  "Counters" should "be updatable via the C* Connector" in conn.withSessionDo { session =>
    (1L to 10L).foreach( x => session.execute("SELECT * FROM counter_norm where key = ?",x: java.lang.Long).one().getLong("expectedkeys") should equal (10))
    (1L to 10L).foreach( x => session.execute("SELECT * FROM counter_cs where key = ?",x: java.lang.Long).one().getLong("expectedkeys") should equal (10))
  }

  it should "be readable via cassandraTable" in {
    var rdd = sc.cassandraTable("counter_ks","counter_norm")
    rdd.count() should equal (10)
    rdd.collect().foreach(row => row.getLong("expectedkeys") should equal(10))
    rdd = sc.cassandraTable("counter_ks","counter_cs")
    rdd.count() should equal (10)
    rdd.collect().foreach(row => row.getLong("expectedkeys") should equal(10))
  }

  it should "be writable via saveToCassandra" in {
    sc.parallelize(1L until 3L).map( x => (x.toString,x,1)).saveToCassandra("counter_ks","counter_multi_write",SomeColumns("name", "key", "data1"))
    sc.parallelize(1L until 3L).map( x => (x.toString,x,1)).saveToCassandra("counter_ks","counter_multi_write",SomeColumns("name", "key", "data2"))
    sc.parallelize(1L until 3L).map( x => (x.toString,x,1)).saveToCassandra("counter_ks","counter_multi_write",SomeColumns("name", "key", "data2"))
    val multiRdd = sc.cassandraTable("counter_ks", "counter_multi_write")
    multiRdd.collect().foreach(row => row.getString("data1") should equal("1"))
    multiRdd.collect().foreach(row => row.getString("data2") should equal("2"))
    multiRdd.collect().foreach(row => row.get[Option[String]]("data3") should be (None))
    multiRdd.count() should equal(2)
    sc.parallelize(0L until 10L).map( x => (x.toString,x,x)).saveToCassandra("counter_ks","counter_write")
    val rdd = sc.cassandraTable("counter_ks", "counter_write")
    rdd.collect().foreach(row => row.getLong("key") should equal(row.getLong("expectedkeys")))
    rdd.count() should equal(10)
  }

}


