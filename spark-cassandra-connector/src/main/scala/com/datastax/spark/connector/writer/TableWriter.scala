package com.datastax.spark.connector.writer

import java.io.IOException
import java.util.concurrent.Semaphore

import com.datastax.driver.core.BatchStatement.Type
import com.datastax.driver.core._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.util.{CountingIterator, Logging}
import org.apache.spark.TaskContext

import scala.collection._

import org.apache.spark.executor.{DataWriteMethod, OutputMetrics}

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
    case x: StaticWriteOption[Int] => Some(x.value)
    case _: PerRowWriteOption[Int] => None
    case TTLOption.auto => None
  }

  val defaultTimestamp = writeConf.timestamp match {
    case x: StaticWriteOption[Long] => Some(x.value)
    case _: PerRowWriteOption[Long] => None
    case TimestampOption.auto => None
  }

  private def quote(name: String): String =
    "\"" + name + "\""

  private[connector] lazy val queryTemplateUsingInsert: String = {
    val quotedColumnNames: Seq[String] = columnNames.map(quote)
    val columnSpec = quotedColumnNames.mkString(", ")
    val valueSpec = quotedColumnNames.map(":" + _).mkString(", ")

    val ttlSpec = writeConf.ttl match {
      case x: PerRowWriteOption[Int] => Some(s"TTL :${x.placeholder}")
      case x: StaticWriteOption[Int] => Some(s"TTL ${x.value}")
      case TTLOption.auto => None
    }

    val timestampSpec = writeConf.timestamp match {
      case x: PerRowWriteOption[Long] => Some(s"TIMESTAMP :${x.placeholder}")
      case x: StaticWriteOption[Long] => Some(s"TIMESTAMP ${x.value}")
      case TimestampOption.auto => None
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

  private def prepareUpdateMetricsFunction(taskContext: TaskContext): Option[(RichStatement) => Unit] = {
    val outputMetrics = new OutputMetrics(DataWriteMethod.Hadoop)
    taskContext.taskMetrics().outputMetrics = Some(outputMetrics)
    val mutex = new Semaphore(1)

    Some({ stmt: RichStatement =>
      mutex.acquire()
      outputMetrics.bytesWritten += stmt.bytesCount
      mutex.release()
    })
  }

  /** Main entry point */
  def write(taskContext: TaskContext, data: Iterator[T]) {
    connector.withSessionDo { session =>
      val t0 = System.nanoTime()

      val updateMetricsFunction = prepareUpdateMetricsFunction(taskContext)
      val rowIterator = new CountingIterator(data)
      val stmt = prepareStatement(session).setConsistencyLevel(writeConf.consistencyLevel)
      val queryExecutor: QueryExecutor = new QueryExecutor(session, writeConf.parallelismLevel, updateMetricsFunction, None)
      val routingKeyGenerator = new RoutingKeyGenerator(tableDef, columnNames)
      val batchType = if (isCounterUpdate) Type.COUNTER else Type.UNLOGGED
      val batchStmtBuilder = new BatchStatementBuilder(batchType, rowWriter, stmt, protocolVersion, routingKeyGenerator, writeConf.consistencyLevel)

      val batchKeyGenerator = writeConf.batchLevel match {
        case BatchLevel.All => bs: BoundStatement => 0

        case BatchLevel.ReplicaSet => bs: BoundStatement =>
          if (bs.getRoutingKey == null)
            bs.setRoutingKey(routingKeyGenerator(bs))
          session.getCluster.getMetadata.getReplicas(keyspaceName, bs.getRoutingKey).hashCode() // hash code is enough

        case BatchLevel.Partition => bs: BoundStatement =>
          if (bs.getRoutingKey == null) {
            bs.setRoutingKey(routingKeyGenerator(bs))
          }
          bs.getRoutingKey.duplicate()
      }

      val batchBuilder = new GroupingBatchBuilder(batchStmtBuilder, batchKeyGenerator,
        writeConf.batchSize, writeConf.batchBufferSize, data)

      logDebug(s"Writing data partition to $keyspaceName.$tableName in batches of ${writeConf.batchSize}.")
      val t1 = System.nanoTime()
      for (stmtToWrite <- batchBuilder) {
        queryExecutor.executeAsync(stmtToWrite)
      }

      queryExecutor.waitForCurrentlyExecutingTasks()

      if (queryExecutor.failureCount > 0)
        throw new IOException(s"Failed to write ${queryExecutor.failureCount} statements to $keyspaceName.$tableName.")

      val tEnd = System.nanoTime()
      val duration1 = (t1 - t0) / 1000000000d
      val duration2 = (tEnd - t1) / 1000000000.0
      logInfo(f"Wrote ${rowIterator.count} rows in ${queryExecutor.successCount} statements to $keyspaceName.$tableName in $duration2%.3f s (setup time was $duration1%.3f s).")
    }
  }
}

object TableWriter {

  def apply[T : RowWriterFactory](
      connector: CassandraConnector,
      keyspaceName: String,
      tableName: String,
      columnNames: ColumnSelector,
      writeConf: WriteConf): TableWriter[T] = {

    val schema = Schema.fromCassandra(connector, Some(keyspaceName), Some(tableName))
    val tableDef = schema.tables.headOption
      .getOrElse(throw new IOException(s"Table not found: $keyspaceName.$tableName"))
    val selectedColumns = columnNames match {
      case SomeColumns(names @ _*) => names.map {
        case ColumnName(columnName) => columnName
        case TTL(_) | WriteTime(_) =>
          throw new IllegalArgumentException(
            s"Neither TTL nor WriteTime fields are not supported for writing. " +
            s"Use appropriate write configuration settings to specify TTL or WriteTime.")
      }
      case AllColumns => tableDef.allColumns.map(_.columnName).toSeq
    }

    val rowWriter = implicitly[RowWriterFactory[T]].rowWriter(
      tableDef.copy(regularColumns = tableDef.regularColumns ++ writeConf.optionsAsColumns(keyspaceName, tableName)),
      selectedColumns ++ writeConf.optionPlaceholders)
    new TableWriter[T](connector, tableDef, rowWriter, writeConf)
  }
}
