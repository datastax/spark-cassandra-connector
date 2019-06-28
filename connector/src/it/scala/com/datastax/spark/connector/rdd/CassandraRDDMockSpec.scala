package com.datastax.spark.connector.rdd

import com.datastax.spark.connector.cluster.DefaultCluster
import com.datastax.spark.connector.{CassandraRow, SparkCassandraITFlatSpecBase}
import com.datastax.spark.connector.cql.CassandraConnector

class CassandraRDDMockSpec extends SparkCassandraITFlatSpecBase with DefaultCluster {

  override lazy val conn = CassandraConnector(defaultConf)

  "A CassandraRDDMock" should "behave like a CassandraRDD without needing Cassandra" in {
    val columns = Seq("key", "value")
    //Create a fake CassandraRDD[CassandraRow]
    val rdd = sc
      .parallelize(1 to 10)
      .map(num => CassandraRow.fromMap(columns.zip(Seq(num, num)).toMap))

    val fakeCassandraRDD: CassandraRDD[CassandraRow] = new CassandraRDDMock(rdd)

    fakeCassandraRDD.cassandraCount() should be (10)
  }
}
