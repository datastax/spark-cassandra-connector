package com.datastax.spark.connector.rdd.typeTests

import java.net.InetAddress
import scala.collection.JavaConverters._

class InetTypeTest extends AbstractTypeTest[InetAddress, InetAddress] {
  override val typeName = "inet"

  override val typeData: Seq[InetAddress] = Seq(InetAddress.getByName("192.168.2.1"), InetAddress.getByName("192.168.2.2"),InetAddress.getByName("192.168.2.3"),InetAddress.getByName("192.168.2.4"),InetAddress.getByName("192.168.2.5"))
  override val addData: Seq[InetAddress] = Seq(InetAddress.getByName("192.168.2.6"), InetAddress.getByName("192.168.2.7"),InetAddress.getByName("192.168.2.8"),InetAddress.getByName("192.168.2.9"),InetAddress.getByName("192.168.2.10"))

  override def getDriverColumn(row: com.datastax.driver.core.Row, colName: String): InetAddress = {
    row.getInet(colName)
  }
}

