package com.datastax.spark.connector.rdd.typeTests

import java.net.InetAddress
import scala.collection.JavaConverters._

class InetTypeTest extends AbstractTypeTest[InetAddress, InetAddress] {
  override val typeName = "inet"
  override val typeData: Seq[InetAddress] = Seq(InetAddress.getByName("192.168.2.1"), InetAddress.getByName("192.168.2.2"),InetAddress.getByName("192.168.2.3"),InetAddress.getByName("192.168.2.4"),InetAddress.getByName("192.168.2.5"))
  override val typeSet: Set[InetAddress] = Set(InetAddress.getByName("4.4.4.4"),InetAddress.getByName("8.8.4.4"),InetAddress.getByName("8.8.8.8"))
  override val typeMap1: Map[String, InetAddress] = Map("key1" -> InetAddress.getByName("127.0.0.1"), "key2" -> InetAddress.getByName("10.200.11.20"), "key3" -> InetAddress.getByName("54.54.54.54"))
  override val typeMap2: Map[InetAddress, String] = Map(InetAddress.getByName("200.1.1.1") -> "val1", InetAddress.getByName("200.1.1.2") -> "val2", InetAddress.getByName("200.1.1.3") -> "val3")

  override val addData: Seq[InetAddress] = Seq(InetAddress.getByName("192.168.2.6"), InetAddress.getByName("192.168.2.7"),InetAddress.getByName("192.168.2.8"),InetAddress.getByName("192.168.2.9"),InetAddress.getByName("192.168.2.10"))
  override val addSet: Set[InetAddress] = Set(InetAddress.getByName("4.4.4.16"),InetAddress.getByName("8.8.4.16"),InetAddress.getByName("8.8.8.16"))
  override val addMap1: Map[String, InetAddress] = Map("key4" -> InetAddress.getByName("32.0.0.1"), "key5" -> InetAddress.getByName("32.0.0.2"), "key3" -> InetAddress.getByName("32.0.0.3"))
  override val addMap2: Map[InetAddress, String] = Map(InetAddress.getByName("100.1.1.1") -> "val4", InetAddress.getByName("100.1.1.2") -> "val5", InetAddress.getByName("100.1.1.2") -> "val6")

  override def getDriverColumn(row: com.datastax.driver.core.Row, colName: String): InetAddress = {
    row.getInet(colName)
  }
}

