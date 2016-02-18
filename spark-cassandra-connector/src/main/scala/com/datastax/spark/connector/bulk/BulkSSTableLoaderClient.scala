package com.datastax.spark.connector.bulk

import com.datastax.driver.core._
import com.datastax.spark.connector.bulk.BulkConf.BulkServerConf
import org.apache.cassandra.config.{ColumnDefinition, CFMetaData}
import org.apache.cassandra.cql3.ColumnIdentifier
import org.apache.cassandra.db.marshal.ReversedType
import org.apache.cassandra.dht.{IPartitioner, Token, Range}
import org.apache.cassandra.io.sstable.SSTableLoader.Client
import org.apache.cassandra.schema.{CQLTypeParser, Types, SchemaKeyspace}
import org.apache.cassandra.streaming.StreamConnectionFactory
import org.apache.cassandra.tools.BulkLoadConnectionFactory
import org.apache.cassandra.utils.FBUtilities

import scala.collection.JavaConverters._

class BulkSSTableLoaderClient(session: Session, bulkServerConf: BulkServerConf) extends Client {
  var tables: Map[String, CFMetaData] = Map.empty[String, CFMetaData]

  override def init(keyspace: String): Unit = {
    val cluster = session.getCluster

    val metaData = cluster.getMetadata

    val partitioner = FBUtilities.newPartitioner(metaData.getPartitioner)
    val tokenRanges = metaData.getTokenRanges.asScala
    val tokenFactory = partitioner.getTokenFactory

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

    val types = fetchTypes(keyspace)

    tables = tables ++ fetchTables(keyspace, partitioner, types)
    tables = tables ++ fetchViews(keyspace, partitioner, types)
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

  private def fetchTypes(keyspace: String): Types = {
    val queryString = s"""SELECT * FROM ${SchemaKeyspace.NAME}.${SchemaKeyspace.TYPES} WHERE keyspace_name = ?"""

    val typeBuilder = Types.rawBuilder(keyspace)
    val rowIterator = session.execute(queryString, keyspace).iterator.asScala
    for (row <- rowIterator) {
      val rowTypeName = row.getString("type_name")
      val rowFieldNames = row.getList("field_names", classOf[String])
      val rowFieldTypes = row.getList("field_types", classOf[String])

      typeBuilder.add(rowTypeName, rowFieldNames, rowFieldTypes)
    }

    typeBuilder.build()
  }

  private def fetchTables(keyspace: String, partitioner: IPartitioner, types: Types): Map[String, CFMetaData] = {
    var tempTables = Map.empty[String, CFMetaData]

    val queryString = s"""SELECT * FROM ${SchemaKeyspace.NAME}.${SchemaKeyspace.TABLES} WHERE keyspace_name = ?"""

    val rowIterator = session.execute(queryString, keyspace).iterator.asScala
    for (row <- rowIterator) {
      val rowTableName = row.getString("table_name")

      tempTables = tempTables + (rowTableName -> createTableMetadata(keyspace, partitioner, false, row, rowTableName, types))
    }

    tempTables
  }

  private def fetchViews(keyspace: String, partitioner: IPartitioner, types: Types): Map[String, CFMetaData] = {
    var tempTables = Map.empty[String, CFMetaData]

    val queryString = s"""SELECT * FROM ${SchemaKeyspace.NAME}.${SchemaKeyspace.VIEWS} WHERE keyspace_name = ?"""

    val rowIterator = session.execute(queryString, keyspace).iterator.asScala
    for (row <- rowIterator) {
      val rowViewName = row.getString("view_name")

      tempTables = tempTables + (rowViewName -> createTableMetadata(keyspace, partitioner, true, row, rowViewName, types))
    }

    tempTables
  }

  private def createTableMetadata(keyspace: String, partitioner: IPartitioner, tableIsView: Boolean, tableRow: Row, tableName: String, types: Types): CFMetaData = {
    val tableId = tableRow.getUUID("id")
    val tableFlags = CFMetaData.flagsFromStrings(tableRow.getSet("flags", classOf[String]))

    val tableIsSuper = tableFlags.contains(CFMetaData.Flag.SUPER)
    val tableIsCounter = tableFlags.contains(CFMetaData.Flag.COUNTER)
    val tableIsDense  = tableFlags.contains(CFMetaData.Flag.DENSE)
    val tableIsCompound = tableFlags.contains(CFMetaData.Flag.COMPOUND)

    val columnsQueryString = s"""SELECT * FROM ${SchemaKeyspace.NAME}.${SchemaKeyspace.COLUMNS} WHERE keyspace_name = ?"""

    var columnDefinitions = Vector.empty[ColumnDefinition]

    val colRowIterator = session.execute(columnsQueryString, keyspace).iterator.asScala
    for (colRow <- colRowIterator) {
      columnDefinitions = columnDefinitions :+ createDefinitionFromRow(colRow, keyspace, tableName, types)
    }

    CFMetaData.create(
      keyspace,
      tableName,
      tableId,
      tableIsDense,
      tableIsCompound,
      tableIsSuper,
      tableIsCounter,
      tableIsView,
      columnDefinitions.toList.asJava,
      partitioner
    )
  }

  private def createDefinitionFromRow(colRow: Row, keyspace: String, table: String, types: Types): ColumnDefinition = {
    val columnName = ColumnIdentifier.getInterned(colRow.getBytes("column_name_bytes"), colRow.getString("column_name"))
    val columnOrder = ClusteringOrder.valueOf(colRow.getString("clustering_order").toUpperCase())

    var columnType = CQLTypeParser.parse(keyspace, colRow.getString("type"), types)
    if (columnOrder == ClusteringOrder.DESC)
      columnType = ReversedType.getInstance(columnType)

    val columnPositition = colRow.getInt("position")
    val columnKind = ColumnDefinition.Kind.valueOf(colRow.getString("kind"))

    new ColumnDefinition(keyspace, table, columnName, columnType, columnPositition, columnKind)
  }
}
