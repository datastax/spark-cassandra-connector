package com.datastax.spark.connector.cql

import java.net.InetAddress

import com.datastax.driver.core.Host

trait DataCenterAware {
  def determineDataCenter(contactPoints: Set[InetAddress], allHosts: Set[Host]):String
}
