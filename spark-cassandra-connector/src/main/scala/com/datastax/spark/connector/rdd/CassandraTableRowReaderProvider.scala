package com.datastax.spark.connector.rdd

import java.io.IOException

import com.datastax.driver.core.{ProtocolVersion, Session}
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.{CassandraConnector, Schema}
import com.datastax.spark.connector.rdd.reader._

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

  protected def readConf: ReadConf

  protected def columnNames: ColumnSelector

  protected def fetchSize = readConf.fetchSize

  protected def splitSize = readConf.splitSize

  protected def consistencyLevel = readConf.consistencyLevel

  /**
   * rtf:RowReaderFactory and rct should be included as implict parameters in the constructor
   * of the class implementing this trait see [[CassandraTableScanRDD]]
   */
  protected val rtf: RowReaderFactory[R]
  protected val rct: ClassTag[R]
  protected lazy val rowReader: RowReader[R] = rtf.rowReader(tableDef)

  private lazy val aliasToColumnName = columnNames.aliases

  lazy val tableDef = {
    Schema.fromCassandra(connector, Some(keyspaceName), Some(tableName)).tables.headOption match {
      case Some(t) => t
      case None => throw new IOException(s"Table not found: $keyspaceName.$tableName")
    }
  }

  protected def checkColumnsExistence(columns: Seq[SelectableColumnRef]): Seq[SelectableColumnRef] = {
    val allColumnNames = tableDef.allColumns.map(_.columnName).toSet
    val regularColumnNames = tableDef.regularColumns.map(_.columnName).toSet

    def checkSingleColumn(column: NamedColumnRef) = {
      if (!allColumnNames.contains(column.columnName))
        throw new IOException(s"Column $column not found in table $keyspaceName.$tableName")

      column match {
        case ColumnName(_) =>

        case TTL(columnName) =>
          if (!regularColumnNames.contains(columnName))
            throw new IOException(s"TTL can be obtained only for regular columns, " +
              s"but column $columnName is not a regular column in table $keyspaceName.$tableName.")

        case WriteTime(columnName) =>
          if (!regularColumnNames.contains(columnName))
            throw new IOException(s"TTL can be obtained only for regular columns, " +
              s"but column $columnName is not a regular column in table $keyspaceName.$tableName.")
      }

      column
    }

    columns.map {
      case namedColumnRef: NamedColumnRef => checkSingleColumn(namedColumnRef)
      case columnRef => columnRef
    }
  }

  /** Returns the names of columns to be selected from the table.*/
  lazy val selectedColumnRefs: Seq[SelectableColumnRef] = {
    val providedColumnRefs =
      columnNames match {
        case AllColumns => tableDef.allColumns.map(col => col.columnName: NamedColumnRef).toSeq
        case PartitionKeyColumns => tableDef.partitionKey.map(col => col.columnName: NamedColumnRef).toSeq
        case SomeColumns(cs@_*) => checkColumnsExistence(cs)
      }

    (rowReader.columnNames, rowReader.requiredColumns) match {
      case (Some(cs), None) => providedColumnRefs.filter(columnName => cs.toSet(columnName.selectedFromCassandraAs))
      case (_, _) => providedColumnRefs
    }
  }

  /** Filters currently selected set of columns with a new set of columns */
  def narrowColumnSelection(columns: Seq[SelectableColumnRef]): Seq[SelectableColumnRef] = {
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
  private def checkColumnsAvailable(columns: Seq[SelectableColumnRef], availableColumns: Seq[SelectableColumnRef]) {
    val availableColumnsSet = availableColumns.collect {
      case ColumnName(columnName) => columnName
    }.toSet

    val notFound = columns.collectFirst {
      case ColumnName(columnName) if !availableColumnsSet.contains(columnName) => columnName
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

  protected def quote(name: String) = "\"" + name + "\""

  def protocolVersion(session: Session): ProtocolVersion = {
    session.getCluster.getConfiguration.getProtocolOptions.getProtocolVersionEnum
  }

  /** Checks for existence of keyspace, table, columns and whether the number of selected columns corresponds to
    * the number of the columns expected by the target type constructor.
    * If successful, does nothing, otherwise throws appropriate `IOException` or `AssertionError`. */
  def verify() = {
    val targetType = rct

    tableDef.allColumns // will throw IOException if table does not exist

    rowReader.columnNames match {
      case Some(names) =>
        val missingColumns = names.toSet -- selectedColumnRefs.map(_.selectedFromCassandraAs).toSet
        assert(missingColumns.isEmpty, s"Missing columns needed by $targetType: ${missingColumns.mkString(", ")}")
      case None =>
    }

    rowReader.requiredColumns match {
      case Some(count) =>
        assert(selectedColumnRefs.size >= count,
          s"Not enough columns selected for the target row type $targetType: ${selectedColumnRefs.size} < $count")
      case None =>
    }
  }

}
