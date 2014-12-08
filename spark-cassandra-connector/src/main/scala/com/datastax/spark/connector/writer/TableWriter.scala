package com.datastax.spark.connector.writer

import java.io.IOException

import com.datastax.driver.core.{Session, BatchStatement, PreparedStatement, ConsistencyLevel}
import com.datastax.spark.connector.{AllColumns, SomeColumns, ColumnSelector}
import com.datastax.spark.connector.cql.{ColumnDef, Schema, TableDef, CassandraConnector}
import com.datastax.spark.connector.util.CountingIterator

import org.apache.spark.{Logging, TaskContext}

import scala.collection._
import scala.reflect.ClassTag

/** Writes RDD data into given Cassandra table.
  * Individual column values are extracted from RDD objects using given [[RowWriter]]
  * Then, data are inserted into Cassandra with batches of CQL INSERT statements.
  * Each RDD partition is processed by a single thread. */
class TableWriter[T] private (
    connector: CassandraConnector,
    tableDef: TableDef,
    rowWriter: RowWriter[T],
    maxBatchSizeInBytes: Int,
    maxBatchSizeInRows: Option[Int],
    parallelismLevel: Int,
    consistencyLevel: ConsistencyLevel) extends Serializable with Logging {

  import com.datastax.spark.connector.writer.TableWriter._

  val keyspaceName = tableDef.keyspaceName
  val tableName = tableDef.tableName
  val columnNames = rowWriter.columnNames
  val columns = columnNames.map(tableDef.columnByName)

  private def quote(name: String): String =
    "\"" + name + "\""

  private lazy val queryTemplateUsingInsert: String = {
    val quotedColumnNames: Seq[String] = columnNames.map(quote)
    val columnSpec = quotedColumnNames.mkString(", ")
    val valueSpec = quotedColumnNames.map(":" + _).mkString(", ")
    s"INSERT INTO ${quote(keyspaceName)}.${quote(tableName)} ($columnSpec) VALUES ($valueSpec)"
  }

  private lazy val queryTemplateUsingUpdate: String = {
    val (primaryKey, regularColumns) = columns.partition(_.isPrimaryKeyColumn)
    val (counterColumns, nonCounterColumns) = regularColumns.partition(_.isCounterColumn)

    def quotedColumnNames(columns: Seq[ColumnDef]) = columns.map(_.columnName).map(quote)
    val setNonCounterColumnsClause = quotedColumnNames(nonCounterColumns).map(c => s"$c = :$c")
    val setCounterColumnsClause = quotedColumnNames(counterColumns).map(c => s"$c = $c + :$c")
    val setClause = (setNonCounterColumnsClause ++ setCounterColumnsClause).mkString(", ")
    val whereClause = quotedColumnNames(primaryKey).map(c => s"$c = :$c").mkString(" AND ")

    s"UPDATE ${quote(keyspaceName)}.${quote(tableName)} SET $setClause WHERE $whereClause"
  }

  private val isCounterUpdate =
    tableDef.allColumns.exists(_.isCounterColumn)

  private val queryTemplate: String = {
    if (isCounterUpdate)
      queryTemplateUsingUpdate
    else
      queryTemplateUsingInsert
  }

  private def prepareStatement(session: Session): PreparedStatement = {
    try {
      session.prepare(queryTemplate)
    }
    catch {
      case t: Throwable =>
        throw new IOException(s"Failed to prepare statement $queryTemplate: " + t.getMessage, t)
    }
  }

  private def createBatch(data: Seq[T], stmt: PreparedStatement): BatchStatement = {
    val batchStmt =
      if (isCounterUpdate)
        new BatchStatement(BatchStatement.Type.COUNTER)
      else
        new BatchStatement(BatchStatement.Type.UNLOGGED)
    for (row <- data)
      batchStmt.add(rowWriter.bind(row, stmt))
    batchStmt
  }

  /** Writes `MeasuredInsertsCount` rows to Cassandra and returns the maximum size of the row */
  private def measureMaxInsertSize(data: Iterator[T], stmt: PreparedStatement, queryExecutor: QueryExecutor): Int = {
    logInfo(s"Writing $MeasuredInsertsCount rows to $keyspaceName.$tableName and measuring maximum serialized row size...")
    var maxInsertSize = 1
    for (row <- data.take(MeasuredInsertsCount)) {
      val insert = rowWriter.bind(row, stmt)
      queryExecutor.executeAsync(insert)
      val size = rowWriter.estimateSizeInBytes(row)
      if (size > maxInsertSize)
        maxInsertSize = size
    }
    logInfo(s"Maximum serialized row size: " + maxInsertSize + " B")
    maxInsertSize
  }

  /** Returns either configured batch size or, if not set, determines the optimal batch size by writing a
    * small number of rows and estimating their size. */
  private def optimumBatchSize(data: Iterator[T], stmt: PreparedStatement, queryExecutor: QueryExecutor): Int = {
    maxBatchSizeInRows match {
      case Some(size) =>
        size
      case None =>
        val maxInsertSize = measureMaxInsertSize(data, stmt, queryExecutor)
        math.max(1, maxBatchSizeInBytes / (maxInsertSize * 2))  // additional margin for data larger than usual
    }
  }

  private def writeBatched(data: Iterator[T], stmt: PreparedStatement, queryExecutor: QueryExecutor, batchSize: Int) {
    for (batch <- data.grouped(batchSize)) {
      val batchStmt = createBatch(batch, stmt)
      batchStmt.setConsistencyLevel(consistencyLevel)
      queryExecutor.executeAsync(batchStmt)
    }
  }

  private def writeUnbatched(data: Iterator[T], stmt: PreparedStatement, queryExecutor: QueryExecutor) {
    for (row <- data)
      queryExecutor.executeAsync(rowWriter.bind(row, stmt))
  }

  /** Main entry point */
  def write(taskContext: TaskContext, data: Iterator[T]) {
    connector.withSessionDo { session =>
      val rowIterator = new CountingIterator(data)
      val startTime = System.currentTimeMillis()
      val stmt = prepareStatement(session)
      stmt.setConsistencyLevel(consistencyLevel)
      val queryExecutor = new QueryExecutor(session, parallelismLevel)
      val batchSize = optimumBatchSize(rowIterator, stmt, queryExecutor)

      logInfo(s"Writing data partition to $keyspaceName.$tableName in batches of $batchSize rows each.")
      batchSize match {
        case 1 => writeUnbatched(rowIterator, stmt, queryExecutor)
        case _ => writeBatched(rowIterator, stmt, queryExecutor, batchSize)
      }

      queryExecutor.waitForCurrentlyExecutingTasks()

      if (queryExecutor.failureCount > 0)
        throw new IOException(s"Failed to write ${queryExecutor.failureCount} batches to $keyspaceName.$tableName.")

      val endTime = System.currentTimeMillis()
      val duration = (endTime - startTime) / 1000.0
      logInfo(f"Wrote ${rowIterator.count} rows in ${queryExecutor.successCount} batches to $keyspaceName.$tableName in $duration%.3f s.")
    }
  }
}

object TableWriter {

  val DefaultParallelismLevel = 5
  val MeasuredInsertsCount = 128
  val DefaultBatchSizeInBytes = 64 * 1024

  def apply[T : ClassTag : RowWriterFactory](
      connector: CassandraConnector,
      keyspaceName: String,
      tableName: String,
      consistencyLevel: ConsistencyLevel,
      columnNames: ColumnSelector,
      batchSizeInBytes: Int = DefaultBatchSizeInBytes,
      batchSizeInRows: Option[Int] = None, 
      parallelismLevel: Int = DefaultParallelismLevel): TableWriter[T] = {

    val schema = Schema.fromCassandra(connector, Some(keyspaceName), Some(tableName))
    val tableDef = schema.tables.headOption
      .getOrElse(throw new IOException(s"Table not found: $keyspaceName.$tableName"))
    val selectedColumns = columnNames match {
      case SomeColumns(names @ _*) => names
      case AllColumns => tableDef.allColumns.map(_.columnName).toSeq
    }
    val rowWriter = implicitly[RowWriterFactory[T]].rowWriter(tableDef, selectedColumns)
    new TableWriter[T](connector, tableDef, rowWriter, batchSizeInBytes, batchSizeInRows, parallelismLevel, consistencyLevel)
  }
}
