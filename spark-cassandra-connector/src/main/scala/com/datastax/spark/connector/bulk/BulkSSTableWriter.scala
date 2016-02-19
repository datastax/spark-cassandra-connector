package com.datastax.spark.connector.bulk

import java.io.{File, IOException}
import java.net.InetAddress
import java.util.UUID

import com.datastax.driver.core.{Session, PreparedStatement}
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.types.ColumnType
import com.datastax.spark.connector.util._
import com.datastax.spark.connector.writer._
import com.datastax.spark.connector.util.Quote._
import org.apache.cassandra.config.DatabaseDescriptor
import org.apache.cassandra.io.sstable.{SSTableLoader, CQLSSTableWriter}
import org.apache.commons.io.FileUtils
import org.apache.spark.{TaskContext, Logging}

import scala.collection.JavaConverters._

class BulkSSTableWriter[T]
(
  cassandraConnector: CassandraConnector,
  tableDef: TableDef,
  columnSelector: IndexedSeq[ColumnRef],
  rowWriter: RowWriter[T],
  bulkConf: BulkConf
) extends Serializable with Logging {
  val keyspaceName = tableDef.keyspaceName
  val tableName = tableDef.tableName
  val columnNames = rowWriter.columnNames diff bulkConf.optionPlaceholders
  val columns = columnNames.map(tableDef.columnByName)

  val defaultTTL = bulkConf.ttl match {
    case TTLOption(StaticWriteOptionValue(value)) => Some(value)
    case _ => None
  }

  val defaultTimestamp = bulkConf.timestamp match {
    case TimestampOption(StaticWriteOptionValue(value)) => Some(value)
    case _ => None
  }

  private val schemaTemplate: String = tableDef.cql

  private val insertTemplate: String = {
    val quotedColumnNames: Seq[String] = columnNames.map(quote)
    val columnSpec = quotedColumnNames.mkString(", ")
    val valueSpec = quotedColumnNames.map(":" + _).mkString(", ")

    val ttlSpec = bulkConf.ttl match {
      case TTLOption(PerRowWriteOptionValue(placeholder)) => Some(s"TTL :$placeholder")
      case TTLOption(StaticWriteOptionValue(value)) => Some(s"TTL $value")
      case _ => None
    }

    val timestampSpec = bulkConf.timestamp match {
      case TimestampOption(PerRowWriteOptionValue(placeholder)) => Some(s"TIMESTAMP :$placeholder")
      case TimestampOption(StaticWriteOptionValue(value)) => Some(s"TIMESTAMP $value")
      case _ => None
    }

    val options = List(ttlSpec, timestampSpec).flatten
    val optionsSpec = if (options.nonEmpty) s"USING ${options.mkString(" AND ")}" else ""

    s"INSERT INTO ${quote(keyspaceName)}.${quote(tableName)} ($columnSpec) VALUES ($valueSpec) $optionsSpec".trim
  }

  private def prepareDataStatement(session: Session): PreparedStatement = {
    try {
      session.prepare(insertTemplate)
    }
    catch {
      case t: Throwable =>
        throw new IOException(s"Failed to prepare insert statement $insertTemplate: " + t.getMessage, t)
    }
  }

  private def prepareSSTableDirectory(): File = {
    val temporaryRoot = System.getProperty("java.io.tmpdir")

    val maxAttempts = 10
    var currentAttempts = 0

    var ssTableDirectory: Option[File] = None
    while (ssTableDirectory.isEmpty) {
      currentAttempts += 1
      if (currentAttempts > maxAttempts) {
        throw new IOException(
          s"Failed to create a SSTable directory of $keyspaceName.$tableName after $maxAttempts attempts!"
        )
      }

      try {
        ssTableDirectory = Some {
          val newSSTablePath = "spark" + "-" + UUID.randomUUID.toString +
            File.separator + keyspaceName + File.separator + tableName

          new File(temporaryRoot, newSSTablePath)
        }
        if (ssTableDirectory.get.exists() || !ssTableDirectory.get.mkdirs()) {
          ssTableDirectory = None
        }
      } catch {
        case e: SecurityException => ssTableDirectory = None
      }
    }

    ssTableDirectory.get.getCanonicalFile
  }

  private def writeRowsToSSTables(
    ssTableDirectory: File,
    dataStatement: PreparedStatement,
    data: Iterator[T]
  ): Unit = {
    val ssTableBuilder = CQLSSTableWriter.builder()
      .inDirectory(ssTableDirectory)
      .forTable(schemaTemplate)
      .using(insertTemplate)
      .withPartitioner(bulkConf.getPartitioner)
    val ssTableWriter = ssTableBuilder.build()

    logInfo(s"Writing rows to temporary SSTables in ${ssTableDirectory.getAbsolutePath}.")

    val startTime = System.nanoTime()

    val rowIterator = new CountingIterator(data)
    val rowColumnNames = rowWriter.columnNames.toIndexedSeq
    val rowColumnTypes = rowColumnNames.map(dataStatement.getVariables.getType)
    val rowConverters = rowColumnTypes.map(ColumnType.converterToCassandra(_))
    val rowBuffer = Array.ofDim[Any](columnNames.size)
    for (currentData <- rowIterator) {
      rowWriter.readColumnValues(currentData, rowBuffer)

      val rowValues = for (index <- columnNames.indices) yield {
        val currentConverter = rowConverters(index)
        val currentValue = currentConverter.convert(rowBuffer(index))

        currentValue
      }

      ssTableWriter.addRow(rowValues: _*)
    }

    ssTableWriter.close()

    val endTime = System.nanoTime()

    val duration = (endTime - startTime) / 1000000000d

    logInfo(s"Wrote rows to temporary SSTables in ${ssTableDirectory.getAbsolutePath} in $duration%.3f s.")
  }

  def write(taskContext: TaskContext, data: Iterator[T]): Unit = {
    val tempSSTableDirectory = prepareSSTableDirectory()

    logInfo(s"Created temporary file directory for SSTables at ${tempSSTableDirectory.getAbsolutePath}.")

    try {
      cassandraConnector.withSessionDo { session =>
        val dataStatement = prepareDataStatement(session)

        writeRowsToSSTables(tempSSTableDirectory, dataStatement, data)

        val bulkSSTableLoaderClient = new BulkSSTableLoaderClient(session, bulkConf.serverConf)
        val bulkSSTableOutputHandler = new BulkOutputHandler(log)
        val bulkSSTableLoader = new SSTableLoader(
          tempSSTableDirectory,
          bulkSSTableLoaderClient,
          bulkSSTableOutputHandler
        )

        DatabaseDescriptor.setStreamThroughputOutboundMegabitsPerSec(bulkConf.throughputMiBPS)

        val bulkProgressIndicator = new BulkProgressIndicator(log)
        try {
          val bulkLoadFuture = bulkSSTableLoader.stream(Set.empty[InetAddress].asJava, bulkProgressIndicator)

          bulkLoadFuture.get()
        } catch  {
          case e: Exception =>
            throw new IOException(s"Failed to write statements to $keyspaceName.$tableName.", e)
        }

        logInfo(s"Finished stream of SSTables from ${tempSSTableDirectory.getAbsolutePath}.")
      }
    } finally {
      if (tempSSTableDirectory.exists()) {
        FileUtils.deleteDirectory(tempSSTableDirectory)
      }
    }
  }
}

object BulkSSTableWriter {
  private def checkMissingColumns(table: TableDef, columnNames: Seq[String]) {
    val allColumnNames = table.columns.map(_.columnName)
    val missingColumns = columnNames.toSet -- allColumnNames
    if (missingColumns.nonEmpty)
      throw new IllegalArgumentException(
        s"Column(s) not found: ${missingColumns.mkString(", ")}")
  }

  private def checkMissingPrimaryKeyColumns(table: TableDef, columnNames: Seq[String]) {
    val primaryKeyColumnNames = table.primaryKey.map(_.columnName)
    val missingPrimaryKeyColumns = primaryKeyColumnNames.toSet -- columnNames
    if (missingPrimaryKeyColumns.nonEmpty)
      throw new IllegalArgumentException(
        s"Some primary key columns are missing in RDD or have not been selected: ${missingPrimaryKeyColumns.mkString(", ")}")
  }

  private def checkNoCollectionBehaviors(table: TableDef, columnRefs: IndexedSeq[ColumnRef]) {
    if (columnRefs.exists(_.isInstanceOf[CollectionColumnName]))
      throw new IllegalArgumentException(
        s"Collection behaviors (add/remove/append/prepend) are not allowed on collection columns")
  }

  private def checkColumns(table: TableDef, columnRefs: IndexedSeq[ColumnRef]) = {
    val columnNames = columnRefs.map(_.columnName)
    checkMissingColumns(table, columnNames)
    checkMissingPrimaryKeyColumns(table, columnNames)
    checkNoCollectionBehaviors(table, columnRefs)
  }

  def apply[T : RowWriterFactory](
    connector: CassandraConnector,
    keyspaceName: String,
    tableName: String,
    columnNames: ColumnSelector,
    bulkConf: BulkConf
  ): BulkSSTableWriter[T] = {
    val schema = Schema.fromCassandra(connector, Some(keyspaceName), Some(tableName))
    val tableDef = schema.tables.headOption
      .getOrElse(throw new IOException(s"Table not found: $keyspaceName.$tableName"))
    val selectedColumns = columnNames.selectFrom(tableDef)
    val optionColumns = bulkConf.optionsAsColumns(keyspaceName, tableName)
    val rowWriter = implicitly[RowWriterFactory[T]].rowWriter(
      tableDef.copy(regularColumns = tableDef.regularColumns ++ optionColumns),
      selectedColumns ++ optionColumns.map(_.ref))

    checkColumns(tableDef, selectedColumns)
    new BulkSSTableWriter[T](connector, tableDef, selectedColumns, rowWriter, bulkConf)
  }
}