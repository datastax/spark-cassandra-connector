package com.datastax.spark.connector.cql

import com.datastax.driver.core.{Row, Session, Statement}
import com.datastax.spark.connector.CassandraRowMetadata
import com.datastax.spark.connector.rdd.ReadConf
import com.datastax.spark.connector.rdd.reader.PrefetchingResultSetIterator

/**
  * Object which will be used in Table Scanning Operations.
  * One Scanner will be created per Spark Partition, it will be
  * created at the beginning of the compute method and Closed at the
  * end of the compute method.
  */
trait Scanner {
  def close(): Unit
  def getSession(): Session
  def scan(statement: Statement): ScanResult
}

case class ScanResult (rows: Iterator[Row], metadata: CassandraRowMetadata)

class DefaultScanner (
    readConf: ReadConf,
    connConf: CassandraConnectorConf,
    columnNames: IndexedSeq[String]) extends Scanner {

  private val session = new CassandraConnector(connConf).openSession()

  override def close(): Unit = {
    session.close()
  }

  override def scan(statement: Statement): ScanResult = {
    val rs = session.execute(statement)
    val columnMetaData = CassandraRowMetadata.fromResultSet(columnNames, rs)
    val iterator = new PrefetchingResultSetIterator(rs, readConf.fetchSizeInRows)
    ScanResult(iterator, columnMetaData)
  }

  override def getSession(): Session = session
}
