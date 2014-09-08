package com.datastax.spark.connector.demo.streaming

import java.net.InetAddress

package object embedded {

  implicit val ZookeeperConnectionString = s"${InetAddress.getLocalHost.getHostAddress}:2181"

}
