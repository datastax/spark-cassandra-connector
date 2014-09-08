package com.datastax.spark.connector.streaming.kafka

import java.net.InetAddress

package object embedded {

  implicit val ZookeeperConnectionString = s"${InetAddress.getLocalHost.getHostAddress}:2181"

}
