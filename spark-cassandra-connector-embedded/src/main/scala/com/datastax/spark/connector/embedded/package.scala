package com.datastax.spark.connector

import java.net.InetAddress

package object embedded {

  implicit val ZookeeperConnectionString = s"${InetAddress.getLocalHost.getHostAddress}:2181"

}
