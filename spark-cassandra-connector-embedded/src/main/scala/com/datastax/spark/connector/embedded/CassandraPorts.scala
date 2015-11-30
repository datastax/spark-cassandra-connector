package com.datastax.spark.connector.embedded

trait CassandraPorts {
  def getRpcPort(index: Int): Int

  def getJmxPort(index: Int): Int

  def getSslStoragePort(index: Int): Int

  def getStoragePort(index: Int): Int

}

object CassandraPorts {
  def apply(ports: Seq[Int]): CassandraPorts = {
    new CassandraPorts {
      override def getRpcPort(index: Int): Int = ports.lift.apply(index).getOrElse(9042)

      override def getStoragePort(index: Int): Int = 7000

      override def getJmxPort(index: Int): Int = CassandraRunner.DefaultJmxPort

      override def getSslStoragePort(index: Int): Int = 7100
    }
  }
}