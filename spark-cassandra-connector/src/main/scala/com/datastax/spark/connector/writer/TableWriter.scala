package com.datastax.spark.connector.writer

import java.io.IOException

import com.datastax.driver.core.BatchStatement.Type
import com.datastax.driver.core._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.types.{CollectionColumnType, ListType, MapType}
import com.datastax.spark.connector.util.Quote._
import com.datastax.spark.connector.util.{CountingIterator, Logging}
import org.apache.spark.TaskContext
import org.apache.spark.metrics.OutputMetricsUpdater

import scala.collection._

/** Writes RDD data into given Cassandra table.
  * Individual column values are extracted from RDD objects using given [[RowWriter]]
  * Then, data are inserted into Cassandra with batches of CQL INSERT statements.
  * Each RDD partition is processed by a single thread. */
class TableWriter[T] private (
    connector: CassandraConnector,
    tableDef: TableDef,
    columnSelector: IndexedSeq[ColumnRef],
    rowWriter: RowWriter[T],
    writeConf: WriteConf) extends Serializable with Logging {

  require(tableDef.isView == false,
    s"${tableDef.name} is a Materialized View and Views are not writable")

  val keyspaceName = tableDef.keyspaceName
  val tableName = tableDef.tableName
  val columnNames = rowWriter.columnNames diff writeConf.optionPlaceholders
  val columns = columnNames.map(tableDef.columnByName)

  private[connector] lazy val queryTemplateUsingInsert: String = {
    val quotedColumnNames: Seq[String] = columnNames.map(quote)
    val columnSpec = quotedColumnNames.mkString(", ")
    val valueSpec = quotedColumnNames.map(":" + _).mkString(", ")

    val ifNotExistsSpec = if (writeConf.ifNotExists) "IF NOT EXISTS " else ""

    s"INSERT INTO ${quote(keyspaceName)}.${quote(tableName)} ($columnSpec) VALUES ($valueSpec) $ifNotExistsSpec$optionsSpec".trim
  }

  private def deleteQueryTemplate(deleteColumns: ColumnSelector): String = {
    val deleteColumnNames: Seq[String] = deleteColumns.selectFrom(tableDef).map(_.columnName)
    val (primaryKey, regularColumns) = columns.partition(_.isPrimaryKeyColumn)
    if (regularColumns.nonEmpty) {
      throw new IllegalArgumentException(
        s"Only primary key columns can be used in delete. Regular columns found: ${regularColumns.mkString(", ")}")
    }
    TableWriter.checkMissingColumns(tableDef, deleteColumnNames)

    def quotedColumnNames(columns: Seq[ColumnDef]) = columns.map(_.columnName).map(quote)
    val deleteColumnsClause = deleteColumnNames.map(quote).mkString(", ")
    val whereClause = quotedColumnNames(primaryKey).map(c => s"$c = :$c").mkString(" AND ")

    val usingTimestampClause = if (timestampSpec.nonEmpty) s"USING ${timestampSpec.get}" else ""

    if (ttlEnabled)
      logWarning(s"${writeConf.ttl} is ignored for DELETE query")

    s"DELETE ${deleteColumnsClause} FROM ${quote(keyspaceName)}.${quote(tableName)} $usingTimestampClause WHERE $whereClause"
  }

  private[connector] lazy val queryTemplateUsingUpdate: String = {
    val (primaryKey, regularColumns) = columns.partition(_.isPrimaryKeyColumn)
    val (counterColumns, nonCounterColumns) = regularColumns.partition(_.isCounterColumn)

    val nameToBehavior = (columnSelector collect {
        case cn:CollectionColumnName => cn.columnName -> cn.collectionBehavior
      }).toMap

    val setNonCounterColumnsClause = for {
      colDef <- nonCounterColumns
      name = colDef.columnName
      collectionBehavior = nameToBehavior.get(name)
      quotedName = quote(name)
    } yield collectionBehavior match {
        case Some(CollectionAppend)           => s"$quotedName = $quotedName + :$quotedName"
        case Some(CollectionPrepend)          => s"$quotedName = :$quotedName + $quotedName"
        case Some(CollectionRemove)           => s"$quotedName = $quotedName - :$quotedName"
        case Some(CollectionOverwrite) | None => s"$quotedName = :$quotedName"
      }

    def quotedColumnNames(columns: Seq[ColumnDef]) = columns.map(_.columnName).map(quote)
    val setCounterColumnsClause = quotedColumnNames(counterColumns).map(c => s"$c = $c + :$c")
    val setClause = (setNonCounterColumnsClause ++ setCounterColumnsClause).mkString(", ")
    val whereClause = quotedColumnNames(primaryKey).map(c => s"$c = :$c").mkString(" AND ")

    s"UPDATE ${quote(keyspaceName)}.${quote(tableName)} $optionsSpec SET $setClause WHERE $whereClause"
  }

  private lazy val timestampSpec: Option[String] = {
    writeConf.timestamp match {
      case TimestampOption(PerRowWriteOptionValue(placeholder)) => Some(s"TIMESTAMP :$placeholder")
      case TimestampOption(StaticWriteOptionValue(value)) => Some(s"TIMESTAMP $value")
      case _ => None
    }
  }

  private lazy val ttlEnabled: Boolean = writeConf.ttl != TTLOption.defaultValue

  private lazy val optionsSpec: String = {
    val ttlSpec = writeConf.ttl match {
      case TTLOption(PerRowWriteOptionValue(placeholder)) => Some(s"TTL :$placeholder")
      case TTLOption(StaticWriteOptionValue(value)) => Some(s"TTL $value")
      case _ => None
    }

    val options = List(ttlSpec, timestampSpec).flatten
    if (options.nonEmpty) s"USING ${options.mkString(" AND ")}" else ""
  }

  private val isCounterUpdate =
    tableDef.columns.exists(_.isCounterColumn)

  private val containsCollectionBehaviors =
    columnSelector.exists(_.isInstanceOf[CollectionColumnName])

  private[connector] val isIdempotent: Boolean = {
    //All counter operations are not Idempotent
    if (columns.filter(_.isCounterColumn).nonEmpty) {
        false
    } else {
      columnSelector.forall {
        //Any appends or prepends to a list are non-idempotent
        case cn: CollectionColumnName =>
          val name = cn.columnName
          val behavior = cn.collectionBehavior
          val isNotList = !tableDef.columnByName(name).columnType.isInstanceOf[ListType[_]]
          behavior match {
            case CollectionPrepend => isNotList
            case CollectionAppend => isNotList
            case _ => true
          }
        //All other operations on regular columns are idempotent
        case regularColumn: ColumnRef => true
      }
    }
  }


  private def prepareStatement(queryTemplate:String, session: Session): PreparedStatement = {
    try {
      session.prepare(queryTemplate).setIdempotent(isIdempotent)
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
        if (bs.getRoutingKey(ProtocolVersion.NEWEST_SUPPORTED, CodecRegistry.DEFAULT_INSTANCE) == null)
          bs.setRoutingKey(routingKeyGenerator(bs))
        session.getCluster.getMetadata.getReplicas(keyspaceName,
          bs.getRoutingKey(ProtocolVersion.NEWEST_SUPPORTED, CodecRegistry.DEFAULT_INSTANCE)).hashCode() // hash code is enough

      case BatchGroupingKey.Partition =>
        if (bs.getRoutingKey(ProtocolVersion.NEWEST_SUPPORTED, CodecRegistry.DEFAULT_INSTANCE) == null) {
          bs.setRoutingKey(routingKeyGenerator(bs))
        }
        bs.getRoutingKey(ProtocolVersion.NEWEST_SUPPORTED, CodecRegistry.DEFAULT_INSTANCE).duplicate()
    }
  }

  /**
    * Main entry point
    * if counter or collection column need to be updated Cql UPDATE command will be used
    * INSERT otherwise
    */
  def write(taskContext: TaskContext, data: Iterator[T]): Unit = {
    if (isCounterUpdate || containsCollectionBehaviors) {
      update(taskContext, data)
    }
    else {
      insert(taskContext, data)
    }
  }

  /**
    * Write data with Cql UPDATE statement
    */
  def update(taskContext: TaskContext, data: Iterator[T]): Unit =
    writeInternal(queryTemplateUsingUpdate, taskContext, data)

  /**
    * Write data with Cql INSERT statement
    */
  def insert(taskContext: TaskContext, data: Iterator[T]):Unit =
    writeInternal(queryTemplateUsingInsert, taskContext, data)

  /**
    * Cql DELETE statement
    * @param columns columns to delete, the row will be deleted completely if the list is empty
    * @param taskContext
    * @param data primary key values to select delete rows
    */
  def delete(columns: ColumnSelector) (taskContext: TaskContext, data: Iterator[T]): Unit =
    writeInternal(deleteQueryTemplate(columns), taskContext, data)

  private def writeInternal(queryTemplate: String, taskContext: TaskContext, data: Iterator[T]) {
    val updater = OutputMetricsUpdater(taskContext, writeConf)
    connector.withSessionDo { session =>
      val protocolVersion = session.getCluster.getConfiguration.getProtocolOptions.getProtocolVersion
      val rowIterator = new CountingIterator(data)
      val stmt = prepareStatement(queryTemplate, session).setConsistencyLevel(writeConf.consistencyLevel)
      val queryExecutor = new QueryExecutor(session, writeConf.parallelismLevel,
        Some(updater.batchFinished(success = true, _, _, _)), Some(updater.batchFinished(success = false, _, _, _)))
      val routingKeyGenerator = new RoutingKeyGenerator(tableDef, columnNames)
      val batchType = if (isCounterUpdate) Type.COUNTER else Type.UNLOGGED

      val boundStmtBuilder = new BoundStatementBuilder(
        rowWriter,
        stmt,
        protocolVersion = protocolVersion,
        ignoreNulls = writeConf.ignoreNulls)

      val batchStmtBuilder = new BatchStatementBuilder(batchType, routingKeyGenerator, writeConf.consistencyLevel)
      val batchKeyGenerator = batchRoutingKey(session, routingKeyGenerator) _
      val batchBuilder = new GroupingBatchBuilder(boundStmtBuilder, batchStmtBuilder, batchKeyGenerator,
        writeConf.batchSize, writeConf.batchGroupingBufferSize, rowIterator)
      val rateLimiter = new RateLimiter((writeConf.throughputMiBPS * 1024 * 1024).toLong, 1024 * 1024)

      logDebug(s"Writing data partition to $keyspaceName.$tableName in batches of ${writeConf.batchSize}.")

      for (stmtToWrite <- batchBuilder) {
        queryExecutor.executeAsync(stmtToWrite)
        assert(stmtToWrite.bytesCount > 0)
        rateLimiter.maybeSleep(stmtToWrite.bytesCount)
      }

      queryExecutor.waitForCurrentlyExecutingTasks()

      queryExecutor.getLatestException().map {
        case exception =>
          throw new IOException(
            s"""Failed to write statements to $keyspaceName.$tableName. The
               |latest exception was
               |  ${exception.getMessage}
               |
               |Please check the executor logs for more exceptions and information
             """.stripMargin)
      }

      val duration = updater.finish() / 1000000000d
      logInfo(f"Wrote ${rowIterator.count} rows to $keyspaceName.$tableName in $duration%.3f s.")
      if (boundStmtBuilder.logUnsetToNullWarning){ logWarning(boundStmtBuilder.UnsetToNullWarning) }
    }
  }
}

object TableWriter {

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

  private def checkMissingPartitionKeyColumns(table: TableDef, columnNames: Seq[String]) {
    val partitionKeyColumnNames = table.partitionKey.map(_.columnName)
    val missingPartitionKeyColumns = partitionKeyColumnNames.toSet -- columnNames
    if (missingPartitionKeyColumns.nonEmpty)
      throw new IllegalArgumentException(
        s"Some partition key columns are missing in RDD or have not been selected: ${missingPartitionKeyColumns.mkString(", ")}")
  }

  private def onlyPartitionKeyAndStatic(table: TableDef, columnNames: Seq[String]): Boolean = {
    val nonPartitionKeyColumnNames = columnNames.toSet -- table.partitionKey.map(_.columnName)
    val nonPartitionKeyColumnRefs = table
      .allColumns
      .filter(columnDef => nonPartitionKeyColumnNames.contains(columnDef.columnName))
    nonPartitionKeyColumnRefs.forall( columnDef => columnDef.columnRole == StaticColumn)
  }

  /**
   * Check whether a collection behavior is being applied to a non collection column
   * Check whether prepend is used on any Sets or Maps
   * Check whether remove is used on Maps
   */
  private def checkCollectionBehaviors(table: TableDef, columnRefs: IndexedSeq[ColumnRef]) {
    val tableCollectionColumns = table.columns.filter(cd => cd.isCollection)
    val tableCollectionColumnNames = tableCollectionColumns.map(_.columnName)
    val tableListColumnNames = tableCollectionColumns
      .map(c => (c.columnName, c.columnType))
      .collect { case (name, x: ListType[_]) => name }

    val tableMapColumnNames = tableCollectionColumns
      .map(c => (c.columnName, c.columnType))
      .collect { case (name, x: MapType[_, _]) => name }

    val refsWithCollectionBehavior = columnRefs collect {
      case columnName: CollectionColumnName => columnName
    }

    val collectionBehaviorColumnNames = refsWithCollectionBehavior.map(_.columnName)

    //Check for non-collection columns with a collection Behavior
    val collectionBehaviorNormalColumn =
      collectionBehaviorColumnNames.toSet -- tableCollectionColumnNames.toSet

    if (collectionBehaviorNormalColumn.nonEmpty)
      throw new IllegalArgumentException(
        s"""Collection behaviors (add/remove/append/prepend) are only allowed on collection columns.
           |Normal Columns with illegal behavior: ${collectionBehaviorNormalColumn.mkString}"""
          .stripMargin
      )

    //Check that prepend is used only on lists
    val prependBehaviorColumnNames = refsWithCollectionBehavior
      .filter(_.collectionBehavior == CollectionPrepend)
      .map(_.columnName)
    val prependOnNonList = prependBehaviorColumnNames.toSet -- tableListColumnNames.toSet

    if (prependOnNonList.nonEmpty)
      throw new IllegalArgumentException(
        s"""The prepend collection behavior only applies to Lists. Prepend used on:
           |${prependOnNonList.mkString}""".stripMargin
      )

    //Check that remove is not used on Maps

    val removeBehaviorColumnNames = refsWithCollectionBehavior
      .filter(_.collectionBehavior == CollectionRemove)
      .map(_.columnName)

    val removeOnMap = removeBehaviorColumnNames.toSet & tableMapColumnNames.toSet

    if (removeOnMap.nonEmpty)
      throw new IllegalArgumentException(
        s"The remove operation is currently not supported for Maps. Remove used on: ${removeOnMap
          .mkString}"
      )
  }

  private def checkColumns(table: TableDef, columnRefs: IndexedSeq[ColumnRef], checkPartitionKey: Boolean) = {
    val columnNames = columnRefs.map(_.columnName)
    checkMissingColumns(table, columnNames)
    if (checkPartitionKey) {
      // For Deletes we only need a partition Key for a valid delete statement
      checkMissingPartitionKeyColumns(table, columnNames)
    }
    else if (onlyPartitionKeyAndStatic(table, columnNames)) {
      // Cassandra only requires a Partition Key Column on insert if all other columns are Static
      checkMissingPartitionKeyColumns(table, columnNames)
    }
    else {
      // For all other normal Cassandra writes we require the full primary key to be present
      checkMissingPrimaryKeyColumns(table, columnNames)
    }
    checkCollectionBehaviors(table, columnRefs)
  }

  def apply[T : RowWriterFactory](
      connector: CassandraConnector,
      keyspaceName: String,
      tableName: String,
      columnNames: ColumnSelector,
      writeConf: WriteConf,
      checkPartitionKey: Boolean = false): TableWriter[T] = {

    val tableDef = Schema.tableFromCassandra(connector, keyspaceName, tableName)
    val selectedColumns = columnNames.selectFrom(tableDef)
    val optionColumns = writeConf.optionsAsColumns(keyspaceName, tableName)
    val rowWriter = implicitly[RowWriterFactory[T]].rowWriter(
      tableDef.copy(regularColumns = tableDef.regularColumns ++ optionColumns),
      selectedColumns ++ optionColumns.map(_.ref))

    checkColumns(tableDef, selectedColumns, checkPartitionKey)
    new TableWriter[T](connector, tableDef, selectedColumns, rowWriter, writeConf)
  }
}
