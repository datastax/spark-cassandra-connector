package com.datastax.spark.connector.rdd

import com.datastax.driver.core.Session
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.metrics.InputMetricsUpdater
import com.datastax.spark.connector.rdd.reader._
import com.datastax.spark.connector.util.CqlWhereParser.{EqPredicate, InListPredicate, InPredicate, RangePredicate}
import com.datastax.spark.connector.util.{CountingIterator, CqlWhereParser}
import com.datastax.spark.connector.writer._
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}

import scala.reflect.ClassTag

/**
 * An `RDD` that will do a selecting join between `left` RDD and the specified Cassandra Table
 * This will perform individual selects to retrieve the rows from Cassandra and will take
 * advantage of RDDs that have been partitioned with the
 * [[com.datastax.spark.connector.rdd.partitioner.ReplicaPartitioner]]
 *
 * @tparam L item type on the left side of the join (any RDD)
 * @tparam R item type on the right side of the join (fetched from Cassandra)
 */
class CassandraJoinRDD[L, R] private[connector](
    left: RDD[L],
    val keyspaceName: String,
    val tableName: String,
    val connector: CassandraConnector,
    val columnNames: ColumnSelector = AllColumns,
    val joinColumns: ColumnSelector = PartitionKeyColumns,
    val where: CqlWhereClause = CqlWhereClause.empty,
    val limit: Option[Long] = None,
    val clusteringOrder: Option[ClusteringOrder] = None,
    val readConf: ReadConf = ReadConf())(
  implicit
    val leftClassTag: ClassTag[L],
    val rightClassTag: ClassTag[R],
    @transient val rowWriterFactory: RowWriterFactory[L],
    @transient val rowReaderFactory: RowReaderFactory[R])
  extends CassandraRDD[(L, R)](left.sparkContext, left.dependencies)
  with CassandraTableRowReaderProvider[R] {

  override type Self = CassandraJoinRDD[L, R]

  override protected val classTag = rightClassTag

  override protected def copy(
    columnNames: ColumnSelector = columnNames,
    where: CqlWhereClause = where,
    limit: Option[Long] = limit,
    clusteringOrder: Option[ClusteringOrder] = None,
    readConf: ReadConf = readConf,
    connector: CassandraConnector = connector): Self = {

    new CassandraJoinRDD[L, R](
      left = left,
      keyspaceName = keyspaceName,
      tableName = tableName,
      connector = connector,
      columnNames = columnNames,
      joinColumns = joinColumns,
      where = where,
      limit = limit,
      clusteringOrder = clusteringOrder,
      readConf = readConf)
  }

  lazy val joinColumnNames: Seq[NamedColumnRef] = joinColumns match {
    case AllColumns => throw new IllegalArgumentException(
      "Unable to join against all columns in a Cassandra Table. Only primary key columns allowed.")
    case PartitionKeyColumns =>
      tableDef.partitionKey.map(col => col.columnName: NamedColumnRef)
    case SomeColumns(cs @ _*) =>
      checkColumnsExistence(cs)
      cs.map {
        case c: NamedColumnRef => c
        case _ => throw new IllegalArgumentException(
          "Unable to join against unnamed columns. No CQL Functions allowed.")
      }
  }

  override def count(): Long = {
    columnNames match {
      case SomeColumns(_) =>
        logWarning("You are about to count rows but an explicit projection has been specified.")
      case _ =>
    }

    val counts =
      new CassandraJoinRDD[L, Long](
        left = left,
        connector = connector,
        keyspaceName = keyspaceName,
        tableName = tableName,
        columnNames = SomeColumns(RowCountRef),
        joinColumns = joinColumns,
        where = where,
        limit = limit,
        clusteringOrder = clusteringOrder,
        readConf= readConf)

    counts.map(_._2).reduce(_ + _)
  }

  /** This method will create the RowWriter required before the RDD is serialized.
    * This is called during getPartitions */
  protected def checkValidJoin(): Seq[NamedColumnRef] = {
    val partitionKeyColumnNames = tableDef.partitionKey.map(_.columnName).toSet
    val primaryKeyColumnNames = tableDef.primaryKey.map(_.columnName).toSet
    val colNames = joinColumnNames.map(_.columnName).toSet

    // Initialize RowWriter and Query to be used for accessing Cassandra
    rowWriter.columnNames
    singleKeyCqlQuery.length

    def checkSingleColumn(column: NamedColumnRef): Unit = {
      require(
        primaryKeyColumnNames.contains(column.columnName),
        s"Can't pushdown join on column $column because it is not part of the PRIMARY KEY")
    }

    // Make sure we have all of the clustering indexes between the 0th position and the max requested
    // in the join:
    val chosenClusteringColumns = tableDef.clusteringColumns
      .filter(cc => colNames.contains(cc.columnName))
    if (!tableDef.clusteringColumns.startsWith(chosenClusteringColumns)) {
      val maxCol = chosenClusteringColumns.last
      val maxIndex = maxCol.componentIndex.get
      val requiredColumns = tableDef.clusteringColumns.takeWhile(_.componentIndex.get <= maxIndex)
      val missingColumns = requiredColumns.toSet -- chosenClusteringColumns.toSet
      throw new IllegalArgumentException(
        s"Can't pushdown join on column $maxCol without also specifying [ $missingColumns ]")
    }
    val missingPartitionKeys = partitionKeyColumnNames -- colNames
    require(
      missingPartitionKeys.size == 0,
      s"Can't join without the full partition key. Missing: [ $missingPartitionKeys ]")

    joinColumnNames.foreach { case c: NamedColumnRef => checkSingleColumn(c) }
    joinColumnNames
  }

  lazy val rowWriter = implicitly[RowWriterFactory[L]].rowWriter(
    tableDef,
    joinColumnNames.map {
      _.columnName
    },
    joinColumns.aliases
  )

  def on(joinColumns: ColumnSelector): CassandraJoinRDD[L, R] = {
    new CassandraJoinRDD[L, R](
      left = left,
      connector = connector,
      keyspaceName = keyspaceName,
      tableName = tableName,
      columnNames = columnNames,
      joinColumns = joinColumns,
      where = where,
      limit = limit,
      clusteringOrder = clusteringOrder,
      readConf = readConf)
  }

  //We need to make sure we get selectedColumnRefs before serialization so that our RowReader is
  //built
  lazy val singleKeyCqlQuery: (String) = {
    val whereClauses = where.predicates.flatMap(CqlWhereParser.parse)
    val partitionKeys = tableDef.partitionKey.map(_.columnName)
    val partitionKeyPredicates = whereClauses.collect {
      case EqPredicate(c, _) if partitionKeys.contains(c) => c
      case InPredicate(c) if partitionKeys.contains(c) => c
      case InListPredicate(c, _) if partitionKeys.contains(c) => c
      case RangePredicate(c, _, _) if partitionKeys.contains(c) => c
    }.toSet

    require(
      partitionKeyPredicates.isEmpty,
      s"No partition keys allowed in where on joins with Cassandra. Found : $partitionKeyPredicates")

    logDebug("Generating Single Key Query Prepared Statement String")
    logDebug(s"SelectedColumns : $selectedColumnRefs -- JoinColumnNames : $joinColumnNames")
    val columns = selectedColumnRefs.map(_.cql).mkString(", ")
    val joinWhere = joinColumnNames.map(_.columnName).map(name => s"${quote(name)} = :$name")
    val limitClause = limit.map(limit => s"LIMIT $limit").getOrElse("")
    val orderBy = clusteringOrder.map(_.toCql(tableDef)).getOrElse("")
    val filter = (where.predicates ++ joinWhere).mkString(" AND ")
    val quotedKeyspaceName = quote(keyspaceName)
    val quotedTableName = quote(tableName)
    val query =
      s"SELECT $columns " +
        s"FROM $quotedKeyspaceName.$quotedTableName " +
        s"WHERE $filter $limitClause $orderBy"
    logDebug(s"Query : $query")
    query
  }

  /**
   * When computing a CassandraPartitionKeyRDD the data is selected via single CQL statements
   * from the specified C* Keyspace and Table. This will be preformed on whatever data is
   * available in the previous RDD in the chain.
   */
  override def compute(split: Partition, context: TaskContext): Iterator[(L, R)] = {
    val session = connector.openSession()
    implicit val pv = protocolVersion(session)
    val stmt = session.prepare(singleKeyCqlQuery).setConsistencyLevel(consistencyLevel)
    val bsb = new BoundStatementBuilder[L](rowWriter, stmt, pv)
    val metricsUpdater = InputMetricsUpdater(context, readConf)
    val rowIterator = fetchIterator(session, bsb, left.iterator(split, context))
    val countingIterator = new CountingIterator(rowIterator, limit)

    context.addTaskCompletionListener { (context) =>
      val duration = metricsUpdater.finish() / 1000000000d
      logDebug(
        f"Fetched ${countingIterator.count} rows " +
          f"from $keyspaceName.$tableName " +
          f"for partition ${split.index} in $duration%.3f s.")
      session.close()
    }
    countingIterator
  }

  private def fetchIterator(
    session: Session,
    bsb: BoundStatementBuilder[L],
    lastIt: Iterator[L]): Iterator[(L, R)] = {

    val columnNamesArray = selectedColumnRefs.map(_.selectedAs).toArray
    implicit val pv = protocolVersion(session)
    for (leftSide <- lastIt;
         rightSide <- {
           val rs = session.execute(bsb.bind(leftSide))
           val iterator = new PrefetchingResultSetIterator(rs, fetchSize)
           iterator.map(rowReader.read(_, columnNamesArray))
         }) yield (leftSide, rightSide)
  }

  override protected def getPartitions: Array[Partition] = {
    verify()
    checkValidJoin()
    left.partitions
  }

  override def getPreferredLocations(split: Partition): Seq[String] = left.preferredLocations(split)

  override def toEmptyCassandraRDD: EmptyCassandraRDD[(L, R)] =
    new EmptyCassandraRDD[(L, R)](
      sc = left.sparkContext,
      keyspaceName = keyspaceName,
      tableName = tableName,
      columnNames = columnNames,
      where = where,
      limit = limit,
      clusteringOrder = clusteringOrder,
      readConf = readConf)
}