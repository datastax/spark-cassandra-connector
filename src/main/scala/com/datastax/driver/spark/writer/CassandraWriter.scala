package com.datastax.driver.spark.writer

import java.io.IOException

import com.datastax.driver.core.{BatchStatement, BoundStatement, PreparedStatement}
import com.datastax.driver.spark.connector.{Schema, TableDef, CassandraConnector}
import com.datastax.driver.spark.mapper._
import com.datastax.driver.spark.types.TypeConverter
import org.apache.log4j.Logger
import org.apache.spark.TaskContext

import scala.collection._
import scala.reflect.ClassTag

/** Writes RDD data into given Cassandra table.
  * Individual column values are extracted from RDD objects using reflection.
  * Then, data are inserted into Cassandra with batches of CQL INSERT statements.
  * Each RDD partition is processed by a single thread. */
class CassandraWriter[T : ClassTag] private (
    connector: CassandraConnector,
    tableDef: TableDef,
    selectedColumns: Seq[String],
    columnMap: ColumnMap,
    maxBatchSizeInBytes: Int,
    maxBatchSizeInRows: Option[Int],
    parallelismLevel: Int) extends Serializable {

  import com.datastax.driver.spark.writer.CassandraWriter._

  private val selectedColumnsSet = selectedColumns.toSet
  private val selectedColumnsIndexed = selectedColumns.toIndexedSeq

  private def checkMissingProperties(requestedPropertyNames: Seq[String]) {
    val availablePropertyNames = PropertyExtractor.availablePropertyNames(cls, requestedPropertyNames)
    val missingColumns = requestedPropertyNames.toSet -- availablePropertyNames
    if (missingColumns.nonEmpty)
      throw new IllegalArgumentException(
        s"One or more properties not found in RDD data: ${missingColumns.mkString(", ")}")
  }

  private def checkMissingColumns(columnNames: Seq[String]) {
    val allColumnNames = tableDef.allColumns.map(_.columnName)
    val missingColumns = columnNames.toSet -- allColumnNames
    if (missingColumns.nonEmpty)
      throw new IllegalArgumentException(
        s"Column(s) not found: ${missingColumns.mkString(", ")}")
  }

  private def checkMissingPrimaryKeyColumns(columnNames: Seq[String]) {
    val primaryKeyColumnNames = tableDef.primaryKey.map(_.columnName)
    val missingPrimaryKeyColumns = primaryKeyColumnNames.toSet -- columnNames
    if (missingPrimaryKeyColumns.nonEmpty)
      throw new IllegalArgumentException(
        s"Some primary key columns are missing in RDD or have not been selected: ${missingPrimaryKeyColumns.mkString(", ")}")
  }

  private val cls = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]

  private def columnNameByRef(columnRef: ColumnRef): Option[String] = {
    columnRef match {
      case NamedColumnRef(name) if selectedColumnsSet.contains(name) => Some(name)
      case IndexedColumnRef(index) if index < selectedColumns.size => Some(selectedColumnsIndexed(index))
      case _ => None
    }
  }

  private val columnTypesByName: Map[String, TypeConverter[_]] =
    tableDef.allColumns.map(c => (c.columnName, c.columnType.converterToCassandra)).toMap

  private val (propertyNames, columnNames) = {
    val propertyToColumnName = columnMap.getters.mapValues(columnNameByRef).toSeq
    val selectedPropertyColumnPairs =
      for ((propertyName, Some(columnName)) <- propertyToColumnName if selectedColumnsSet.contains(columnName))
      yield (propertyName, columnName)
    selectedPropertyColumnPairs.unzip
  }

  checkMissingProperties(propertyNames)
  checkMissingColumns(columnNames)
  checkMissingPrimaryKeyColumns(columnNames)

  val propertyTypes = columnNames.map(columnTypesByName)
  val extractor = new ConvertingPropertyExtractor(cls, propertyNames zip propertyTypes)

  val keyspaceName = tableDef.keyspaceName
  val tableName = tableDef.tableName

  val queryStr: String = {
    val columnSpec = columnNames.map(quote).mkString(", ")
    val valueSpec = columnNames.map(_ => "?").mkString(", ")
    s"INSERT INTO ${quote(keyspaceName)}.${quote(tableName)} ($columnSpec) VALUES ($valueSpec)"
  }

  private def quote(name: String) =
    "\"" + name + "\""


  /** Creates insert statement from serialized data
    * and optionally allows to estimate its serialized size.
    * Mutable and reused for all insert statements. */
  class InsertBuffer(stmt: PreparedStatement) {
    
    private val data: Array[AnyRef] =
      Array.ofDim(columnNames.size)

    def loadFrom(row: T) {
      extractor.extract(row, data)
    }

    def estimatedDataSize: Int =
      ObjectSizeEstimator.measureSerializedSize(data)

    def statement: BoundStatement = {
      val boundStmt = new BoundStatement(stmt)
      boundStmt.bind(data: _*)
      boundStmt
    }
  }

  private def createBatch(dataToWrite: Seq[T], insert: InsertBuffer): BatchStatement = {
    val batchStmt = new BatchStatement(BatchStatement.Type.UNLOGGED)
    for (row <- dataToWrite) {
      insert.loadFrom(row)
      batchStmt.add(insert.statement)
    }
    batchStmt
  }

  /** Writes {{{MeasuredInsertsCount}}} rows to Cassandra and returns the maximum size of the row */
  private def measureMaxInsertSize(data: Iterator[T], insert: InsertBuffer, queryExecutor: QueryExecutor): Int = {
    logger.info(s"Writing $MeasuredInsertsCount rows to $keyspaceName.$tableName and measuring maximum serialized row size...")
    var maxInsertSize = 1
    for (row <- data.take(MeasuredInsertsCount)) {
      insert.loadFrom(row)
      queryExecutor.executeAsync(insert.statement)
      val size = insert.estimatedDataSize
      if (size > maxInsertSize)
        maxInsertSize = size
    }
    logger.info(s"Maximum serialized row size: " + maxInsertSize + " B")
    maxInsertSize
  }

  /** Returns either configured batch size or, if not set, determines the optimal batch size by writing a
    * small number of rows and estimating their size. */
  private def optimumBatchSize(data: Iterator[T], insert: InsertBuffer, queryExecutor: QueryExecutor): Int = {
    maxBatchSizeInRows match {
      case Some(size) =>
        size
      case None =>
        val maxInsertSize = measureMaxInsertSize(data, insert, queryExecutor)
        math.max(1, maxBatchSizeInBytes / (maxInsertSize * 2))  // additional margin for data larger than usual
    }
  }

  private def writeBatched(data: Iterator[T], insert: InsertBuffer, queryExecutor: QueryExecutor, batchSize: Int) {
    for (batch <- data.grouped(batchSize)) {
      val batchStmt = createBatch(batch, insert)
      queryExecutor.executeAsync(batchStmt)
    }
  }

  private def writeUnbatched(data: Iterator[T], insert: InsertBuffer, queryExecutor: QueryExecutor) {
    for (row <- data) {
      insert.loadFrom(row)
      queryExecutor.executeAsync(insert.statement)
    }
  }

  /** Main entry point */
  def write(taskContext: TaskContext, data: Iterator[T]) {
    var rowCount = 0
    val countedData = data.map { item => rowCount += 1; item }


    connector.withSessionDo { session =>
      logger.info(s"Connected to Cassandra cluster ${session.getCluster.getClusterName}")
      val startTime = System.currentTimeMillis()
      val stmt = session.prepare(queryStr)
      val insert = new InsertBuffer(stmt)
      val queryExecutor = new QueryExecutor(session, parallelismLevel)
      val batchSize = optimumBatchSize(countedData, insert, queryExecutor)

      logger.info(s"Writing data partition to $keyspaceName.$tableName in batches of $batchSize rows each.")
      batchSize match {
        case 1 => writeUnbatched(countedData, insert, queryExecutor)
        case _ => writeBatched(countedData, insert, queryExecutor, batchSize)
      }

      queryExecutor.waitForCurrentlyExecutingTasks()

      if (queryExecutor.failureCount > 0)
        throw new IOException(s"Failed to write ${queryExecutor.failureCount} batches to $keyspaceName.$tableName.")

      val endTime = System.currentTimeMillis()
      val duration = (endTime - startTime) / 1000.0
      logger.info(f"Successfully wrote $rowCount rows in ${queryExecutor.successCount} batches to $keyspaceName.$tableName in $duration%.3f s.")
    }
  }

}

object CassandraWriter {

  val logger = Logger.getLogger(classOf[CassandraWriter[_]])

  val DefaultParallelismLevel = 5
  val MeasuredInsertsCount = 128
  val DefaultBatchSizeInBytes = 64 * 1024

  def apply[T : ClassTag : ColumnMapper](
      connector: CassandraConnector,
      keyspaceName: String,
      tableName: String,
      columnNames: Option[Seq[String]] = None,
      batchSizeInBytes: Int = DefaultBatchSizeInBytes,
      batchSizeInRows: Option[Int] = None, 
      parallelismLevel: Int = DefaultParallelismLevel) =
  {
    val tableDef = new Schema(connector, Some(keyspaceName), Some(tableName)).tables.head
    val selectedColumns = columnNames.getOrElse(tableDef.allColumns.map(_.columnName).toSeq)
    val columnMap = implicitly[ColumnMapper[T]].columnMap(tableDef)
    new CassandraWriter[T](connector, tableDef, selectedColumns, columnMap,
      batchSizeInBytes, batchSizeInRows, parallelismLevel)
  }
}
