package com.datastax.spark.connector.rdd

import java.io.IOException

import com.datastax.driver.core.{Metadata, ConsistencyLevel, ProtocolVersion, Session}
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.{TableDef, CassandraConnector, Schema}
import com.datastax.spark.connector.rdd.reader._
import com.datastax.spark.connector.util.NameTools

import scala.reflect.ClassTag

/**
 * Used to get a RowReader of type [R] for transforming the rows of a particular Cassandra table into
 * scala objects. Performs necessary checking of the schema and output class to make sure they are
 * compatible.
 * @see [[CassandraTableScanRDD]]
 * @see [[CassandraJoinRDD]]
 */
trait CassandraTableRowReaderProvider[R] {

  protected def connector: CassandraConnector

  protected def keyspaceName: String

  protected def tableName: String

  protected def columnNames: ColumnSelector

  protected def readConf: ReadConf

  protected def splitCount: Option[Int] = readConf.splitCount

  protected def splitSizeInMB: Int = readConf.splitSizeInMB

  protected def fetchSize: Int = readConf.fetchSizeInRows

  protected def consistencyLevel: ConsistencyLevel = readConf.consistencyLevel

  /** RowReaderFactory and ClassTag should be exit provided from implicit parameters in the constructor
    * of the class implementing this trait
    * @see CassandraTableScanRDD */
  protected val rowReaderFactory: RowReaderFactory[R]

  protected val classTag: ClassTag[R]

  protected lazy val rowReader: RowReader[R] =
    rowReaderFactory.rowReader(tableDef, columnNames.selectFrom(tableDef))

  lazy val tableDef: TableDef = {
    Schema.fromCassandra(connector, Some(keyspaceName), Some(tableName)).tables.headOption match {
      case Some(t) => t
      case None =>
        val metadata: Metadata = connector.withClusterDo(_.getMetadata)
        val suggestions = NameTools.getSuggestions(metadata, keyspaceName, tableName)
        val errorMessage = NameTools.getErrorString(keyspaceName, tableName, suggestions)
        throw new IOException(errorMessage)
    }
  }

  protected def checkColumnsExistence(columns: Seq[ColumnRef]): Seq[ColumnRef] = {
    val allColumnNames = tableDef.columns.map(_.columnName).toSet
    val regularColumnNames = tableDef.regularColumns.map(_.columnName).toSet

    def checkSingleColumn(column: ColumnRef) = {
      column match {
        case ColumnName(columnName, _) =>
          if (!allColumnNames.contains(column.columnName))
            throw new IOException(s"Column $column not found in table $keyspaceName.$tableName")
        case TTL(columnName, _) =>
          if (!regularColumnNames.contains(columnName))
            throw new IOException(s"TTL can be obtained only for regular columns, " +
              s"but column $columnName is not a regular column in table $keyspaceName.$tableName.")
        case WriteTime(columnName, _) =>
          if (!regularColumnNames.contains(columnName))
            throw new IOException(s"TTL can be obtained only for regular columns, " +
              s"but column $columnName is not a regular column in table $keyspaceName.$tableName.")
        case _ =>
      }

      column
    }

    columns.map(checkSingleColumn)
  }

  /** Returns the columns to be selected from the table.*/
  lazy val selectedColumnRefs: Seq[ColumnRef] = {
    val providedColumns =
      columnNames match {
        case AllColumns => tableDef.columns.map(col => col.columnName: ColumnRef)
        case PartitionKeyColumns => tableDef.partitionKey.map(col => col.columnName: ColumnRef)
        case SomeColumns(cs@_*) => checkColumnsExistence(cs)
      }

    // Let's leave only the columns needed by the rowReader.
    // E.g. even if the user selects AllColumns,
    // this will make sure only the columns needed by the RowReader are actually fetched.
    rowReader.neededColumns match {
      case Some(neededColumns) => providedColumns.filter(neededColumns.toSet)
      case None => providedColumns
    }
  }

  /** Filters currently selected set of columns with a new set of columns */
  def narrowColumnSelection(columns: Seq[ColumnRef]): Seq[ColumnRef] = {
    columnNames match {
      case SomeColumns(cs@_*) =>
        checkColumnsAvailable(columns, cs)
      case AllColumns =>
      case PartitionKeyColumns =>
      // we do not check for column existence yet as it would require fetching schema and a call to C*
      // columns existence will be checked by C* once the RDD gets computed.
    }
    columns
  }

  /** Throws IllegalArgumentException if columns sequence contains unavailable columns */
  private def checkColumnsAvailable(
      columns: Seq[ColumnRef],
      availableColumns: Seq[ColumnRef]) {

    val availableColumnsSet = availableColumns.collect {
      case ColumnName(columnName, _) => columnName
    }.toSet

    val notFound = columns.collectFirst {
      case ColumnName(columnName, _) if !availableColumnsSet.contains(columnName) => columnName
    }

    if (notFound.isDefined)
      throw new IllegalArgumentException(
        s"Column not found in selection: ${notFound.get}. " +
          s"Available columns: [${availableColumns.mkString(",")}].")
  }


  protected lazy val cassandraPartitionerClassName =
    connector.withSessionDo {
      session =>
        session.execute("SELECT partitioner FROM system.local").one().getString(0)
    }

  def protocolVersion(session: Session): ProtocolVersion = {
    session.getCluster.getConfiguration.getProtocolOptions.getProtocolVersionEnum
  }

  /** Checks for existence of keyspace and table.*/
  def verify() = {
    tableDef.columns // will throw IOException if table does not exist
    rowReader // will instantiate rowReader which must validate existence of columns
  }

}
