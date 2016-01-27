package com.datastax.spark.connector.bulk

import com.datastax.driver.core._
import com.datastax.spark.connector.bulk.BulkConf.BulkServerConf
import org.apache.cassandra.config.CFMetaData
import org.apache.cassandra.db.composites.CellNames
import org.apache.cassandra.db.marshal.TypeParser
import org.apache.cassandra.db.{ColumnFamilyType, SystemKeyspace}
import org.apache.cassandra.dht.{Token, Range}
import org.apache.cassandra.io.sstable.SSTableLoader.Client
import org.apache.cassandra.schema.LegacySchemaTables
import org.apache.cassandra.streaming.StreamConnectionFactory
import org.apache.cassandra.tools.BulkLoadConnectionFactory

import scala.collection.JavaConverters._

class BulkSSTableLoaderClient(session: Session, bulkServerConf: BulkServerConf) extends Client {
  var tables: Map[String, CFMetaData] = Map.empty[String, CFMetaData]

  override def init(keyspace: String): Unit = {
    val cluster = session.getCluster

    val metaData = cluster.getMetadata

    setPartitioner(metaData.getPartitioner)

    val tokenRanges = metaData.getTokenRanges.asScala
    val tokenFactory = getPartitioner.getTokenFactory

    for (tokenRange <- tokenRanges) {
      val endpoints = metaData.getReplicas(Metadata.quote(keyspace), tokenRange).asScala

      val range = new Range[Token](
        tokenFactory.fromString(tokenRange.getStart.getValue.toString),
        tokenFactory.fromString(tokenRange.getEnd.getValue.toString)
      )

      for (endpoint <- endpoints) {
        addRangeForEndpoint(range, endpoint.getAddress)
      }
    }

    tables = fetchTablesMetadata(keyspace)
  }

  override def getTableMetadata(tableName: String): CFMetaData = {
    tables(tableName)
  }

  override def setTableMetadata(cfm: CFMetaData): Unit = {
    tables = tables + (cfm.cfName -> cfm)
  }


  override def getConnectionFactory: StreamConnectionFactory = {
    new BulkLoadConnectionFactory(
      bulkServerConf.storagePort,
      bulkServerConf.sslStoragePort,
      bulkServerConf.getServerEncryptionOptions,
      false
    )
  }

  private def fetchTablesMetadata(keyspace: String): Map[String, CFMetaData] = {
    var tempTables = Map.empty[String, CFMetaData]

    val queryString = s"""SELECT columnfamily_name, cf_id, type, comparator, subcomparator, is_dense FROM ${SystemKeyspace.NAME}.${LegacySchemaTables.COLUMNFAMILIES} WHERE keyspace_name = '$keyspace'"""

    val rowIterator = session.execute(queryString).iterator.asScala
    for (row <- rowIterator) {
      val rowName = row.getString("columnfamily_name")
      val rowId = row.getUUID("cf_id")
      val rowType = ColumnFamilyType.valueOf(row.getString("type"))
      val rowRawComparator = TypeParser.parse(row.getString("comparator"))
      val rowSubComparator = if (row.isNull("subcomparator")) null else TypeParser.parse(row.getString("subcomparator"))
      val rowIsDense = row.getBool("is_dense")
      val rowComparator = CellNames.fromAbstractType(CFMetaData.makeRawAbstractType(rowRawComparator, rowSubComparator), rowIsDense)

      tempTables = tempTables + (rowName -> new CFMetaData(keyspace, rowName, rowType, rowComparator, rowId))
    }

    tempTables
  }
}
