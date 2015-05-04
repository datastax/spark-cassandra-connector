package com.datastax.spark.connector.writer

import java.io.IOException

import org.apache.spark.metrics.OutputMetricsUpdater

import com.datastax.driver.core.BatchStatement.Type
import com.datastax.driver.core._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.util.CountingIterator
import com.datastax.spark.connector.util.Quote._
import org.apache.spark.{Logging, TaskContext}

import scala.collection._

/** Writes RDD data into given Cassandra table.
  * Individual column values are extracted from RDD objects using given [[RowWriter]]
  * Then, data are inserted into Cassandra with batches of CQL INSERT statements.
  * Each RDD partition is processed by a single thread. */
class TableWriter[T] private (
    connector: CassandraConnector,
    tableDef: TableDef,
    rowWriter: RowWriter[T],
    writeConf: WriteConf) extends Serializable with Logging {

  val keyspaceName = tableDef.keyspaceName
  val tableName = tableDef.tableName
  val columnNames = rowWriter.columnNames diff writeConf.optionPlaceholders
  val columns = columnNames.map(tableDef.columnByName)
  implicit val protocolVersion = connector.withClusterDo { _.getConfiguration.getProtocolOptions.getProtocolVersionEnum }

  val defaultTTL = writeConf.ttl match {
    case TTLOption(StaticWriteOptionValue(value)) => Some(value)
    case _ => None
  }

  val defaultTimestamp = writeConf.timestamp match {
    case TimestampOption(StaticWriteOptionValue(value)) => Some(value)
    case _ => None
  }

  private[connector] lazy val queryTemplateUsingInsert: String = {
    val quotedColumnNames: Seq[String] = columnNames.map(quote)
    val columnSpec = quotedColumnNames.mkString(", ")
    val valueSpec = quotedColumnNames.map(":" + _).mkString(", ")

    val ttlSpec = writeConf.ttl match {
      case TTLOption(PerRowWriteOptionValue(placeholder)) => Some(s"TTL :$placeholder")
      case TTLOption(StaticWriteOptionValue(value)) => Some(s"TTL $value")
      case _ => None
    }

    val timestampSpec = writeConf.timestamp match {
      case TimestampOption(PerRowWriteOptionValue(placeholder)) => Some(s"TIMESTAMP :$placeholder")
      case TimestampOption(StaticWriteOptionValue(value)) => Some(s"TIMESTAMP $value")
      case _ => None
    }

    val options = List(ttlSpec, timestampSpec).flatten
    val optionsSpec = if (options.nonEmpty) s"USING ${options.mkString(" AND ")}" else ""

    s"INSERT INTO ${quote(keyspaceName)}.${quote(tableName)} ($columnSpec) VALUES ($valueSpec) $optionsSpec".trim
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
    tableDef.columns.exists(_.isCounterColumn)

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

  def batchRoutingKey(session: Session, routingKeyGenerator: RoutingKeyGenerator)(bs: BoundStatement): Any = {
    writeConf.batchGroupingKey match {
      case BatchGroupingKey.None => 0

      case BatchGroupingKey.ReplicaSet =>
        if (bs.getRoutingKey == null)
          bs.setRoutingKey(routingKeyGenerator(bs))
        session.getCluster.getMetadata.getReplicas(keyspaceName, bs.getRoutingKey).hashCode() // hash code is enough

      case BatchGroupingKey.Partition =>
        if (bs.getRoutingKey == null) {
          bs.setRoutingKey(routingKeyGenerator(bs))
        }
        bs.getRoutingKey.duplicate()
    }
  }

  /** Main entry point */
  def write(taskContext: TaskContext, data: Iterator[T]) {
    val updater = OutputMetricsUpdater(taskContext, writeConf)
    connector.withSessionDo { session =>
      val rowIterator = new CountingIterator(data)
      val stmt = prepareStatement(session).setConsistencyLevel(writeConf.consistencyLevel)
      val queryExecutor = new QueryExecutor(session, writeConf.parallelismLevel,
        Some(updater.batchFinished(success = true, _, _, _)), Some(updater.batchFinished(success = false, _, _, _)))
      val routingKeyGenerator = new RoutingKeyGenerator(tableDef, columnNames)
      val batchType = if (isCounterUpdate) Type.COUNTER else Type.UNLOGGED
      val boundStmtBuilder = new BoundStatementBuilder(rowWriter, stmt, protocolVersion)
      val batchStmtBuilder = new BatchStatementBuilder(batchType, routingKeyGenerator, writeConf.consistencyLevel)
      val batchKeyGenerator = batchRoutingKey(session, routingKeyGenerator) _
      val batchBuilder = new GroupingBatchBuilder(boundStmtBuilder, batchStmtBuilder, batchKeyGenerator,
        writeConf.batchSize, writeConf.batchGroupingBufferSize, rowIterator)
      val rateLimiter = new RateLimiter(writeConf.throughputMiBPS * 1024L * 1024L, 1024L * 1024L)

      logDebug(s"Writing data partition to $keyspaceName.$tableName in batches of ${writeConf.batchSize}.")

      for (stmtToWrite <- batchBuilder) {
        queryExecutor.executeAsync(stmtToWrite)
        assert(stmtToWrite.bytesCount > 0)
        rateLimiter.maybeSleep(stmtToWrite.bytesCount)
      }

      queryExecutor.waitForCurrentlyExecutingTasks()

      if (!queryExecutor.successful)
        throw new IOException(s"Failed to write statements to $keyspaceName.$tableName.")

      val duration = updater.finish() / 1000000000d
      logInfo(f"Wrote ${rowIterator.count} rows to $keyspaceName.$tableName in $duration%.3f s.")
    }
  }
}

object TableWriter {

  private def checkColumns(table: TableDef, columnNames: Seq[String]) = {
    checkMissingColumns(table, columnNames)
    checkMissingPrimaryKeyColumns(table, columnNames)
  }

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

  def apply[T : RowWriterFactory](
      connector: CassandraConnector,
      keyspaceName: String,
      tableName: String,
      columnNames: ColumnSelector,
      writeConf: WriteConf): TableWriter[T] = {

    val schema = Schema.fromCassandra(connector, Some(keyspaceName), Some(tableName))
    val tableDef = schema.tables.headOption
      .getOrElse(throw new IOException(s"Table not found: $keyspaceName.$tableName"))
    val selectedColumns = columnNames.selectFrom(tableDef)
    val optionColumns = writeConf.optionsAsColumns(keyspaceName, tableName)
    val rowWriter = implicitly[RowWriterFactory[T]].rowWriter(
      tableDef.copy(regularColumns = tableDef.regularColumns ++ optionColumns),
      selectedColumns ++ optionColumns.map(_.ref))
    
    checkColumns(tableDef, selectedColumns.map(_.columnName))
    new TableWriter[T](connector, tableDef, rowWriter, writeConf)
  }
}
