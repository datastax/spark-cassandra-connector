package com.datastax.spark.connector.rdd.typeTests

import java.net.InetAddress

import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.spark.connector.cluster.DefaultCluster
import org.apache.spark.sql.SaveMode

import scala.collection.JavaConverters._

case class InetStringRow(pkey: String, ckey1: String, ckey2: String, data1: String)
class InetTypeTest extends AbstractTypeTest[InetAddress, InetAddress] with DefaultCluster {
  override val typeName = "inet"

  override val typeData: Seq[InetAddress] = Seq(InetAddress.getByName("192.168.2.1"), InetAddress.getByName("192.168.2.2"),InetAddress.getByName("192.168.2.3"),InetAddress.getByName("192.168.2.4"),InetAddress.getByName("192.168.2.5"))
  override val addData: Seq[InetAddress] = Seq(InetAddress.getByName("192.168.2.6"), InetAddress.getByName("192.168.2.7"),InetAddress.getByName("192.168.2.8"),InetAddress.getByName("192.168.2.9"),InetAddress.getByName("192.168.2.10"))

  override def getDriverColumn(row: Row, colName: String): InetAddress = {
    row.getInetAddress(colName)
  }

  "A String DataFrame" should "write to C* Inets" in {
    val InetString = "111.111.111.111"
    val stringRDD = sc.parallelize(Seq(InetStringRow(InetString, InetString, InetString, InetString)))
    val stringDf = spark.createDataFrame(stringRDD)

    val normOptions = Map("keyspace" -> keyspaceName, "table" -> typeNormalTable)
    stringDf.write
      .format("org.apache.spark.sql.cassandra")
      .options(normOptions)
      .mode(SaveMode.Append)
      .save()

    val row = conn.withSessionDo(session =>
      session.execute(s"SELECT * FROM $keyspaceName.$typeNormalTable WHERE pkey = '$InetString' and ckey1 = '$InetString'").one)
    row.getInetAddress("data1") should be (InetAddress.getByName(InetString))
  }
}

