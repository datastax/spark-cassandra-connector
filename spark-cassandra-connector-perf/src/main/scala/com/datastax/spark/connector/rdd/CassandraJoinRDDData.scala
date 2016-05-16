package com.datastax.spark.connector.rdd

import java.lang.{Long => JLong}

import com.datastax.driver.core.{ResultSet, ResultSetFuture, Session}
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.SparkTemplate
import com.datastax.spark.connector.rdd.CassandraJoinRDDBenchmark._
import com.datastax.spark.connector.writer.AsyncExecutor

object CassandraJoinRDDData extends App with SparkTemplate {

  val config = SparkTemplate.defaultConf.set("spark.cassandra.connection.port", "9042")

  val someContent =
    """Lorem ipsum dolor sit amet, consectetur adipiscing elit. Etiam quis augue
      tristique, ultricies nisl sit amet, condimentum felis. Ut blandit nisi eget imperdiet pretium. Ut a
      erat in tortor ultrices posuere quis eu dui. Lorem ipsum dolor sit amet, consectetur adipiscing
      elit. Integer vestibulum vitae arcu ac vehicula. Praesent non erat quis ipsum tempor tempus
      vitae quis neque. Aenean eget urna egestas, lobortis velit sed, vestibulum justo. Nam nibh
      risus, bibendum non ex ac, bibendum varius purus. """

  def createKeyspaceCql(name: String) =
    s"""
       |CREATE KEYSPACE IF NOT EXISTS $name
       |WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }
       |AND durable_writes = false
       |""".stripMargin

  def createTableCql(keyspace: String, tableName: String): String =
    s"""
       |CREATE TABLE $keyspace.$tableName (
       |  key INT,
       |  group BIGINT,
       |  value TEXT,
       |  PRIMARY KEY (key, group)
       |)""".stripMargin

  def insertData(session: Session, keyspace: String, table: String, partitions: Int,
    partitionSize: Int): Unit = {
    def executeAsync(data: (Int, Long, String)): ResultSetFuture = {
      session.executeAsync(
        s"INSERT INTO $Keyspace.$table (key, group, value) VALUES (?, ?, ?)",
        data._1: Integer, data._2.toLong: JLong, data._3.toString
      )
    }
    val executor = new AsyncExecutor[(Int, Long, String), ResultSet](executeAsync, 1000, None, None)

    for (
      partition <- 0 until partitions;
      group <- 0 until partitionSize
    ) yield executor.executeAsync((partition, group, s"${partition}_${group}_${someContent}"))
    executor.waitForCurrentlyExecutingTasks()
  }

  val conn = CassandraConnector(config)
  conn.withSessionDo { session =>
    session.execute(s"DROP KEYSPACE IF EXISTS $Keyspace")
    session.execute(createKeyspaceCql(Keyspace))

    session.execute(createTableCql(Keyspace, "one_element_partitions"))
    session.execute(createTableCql(Keyspace, "small_partitions"))
    session.execute(createTableCql(Keyspace, "moderate_partitions"))
    session.execute(createTableCql(Keyspace, "big_partitions"))

    insertData(session, Keyspace, "one_element_partitions", Rows, 1)
    insertData(session, Keyspace, "small_partitions", Rows / SmallPartitionSize, SmallPartitionSize)
    insertData(session, Keyspace, "moderate_partitions", Rows/ ModeratePartitionSize, ModeratePartitionSize)
    insertData(session, Keyspace, "big_partitions", Rows / BigPartitionSize, BigPartitionSize)
  }
}
