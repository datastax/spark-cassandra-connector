package com.datastax.spark.connector.rdd

import com.datastax.driver.core.Session
import com.datastax.spark.connector._
import com.datastax.spark.connector.util.CqlWhereParser.{EqPredicate, InListPredicate, InPredicate, RangePredicate}
import com.datastax.spark.connector.util.Quote._
import com.datastax.spark.connector.util.{CountingIterator, CqlWhereParser}
import com.datastax.spark.connector.writer._
import org.apache.spark.metrics.InputMetricsUpdater
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}

/**
 * This trait contains shared methods from [[com.datastax.spark.connector.rdd.CassandraJoinRDD]] and
 * [[com.datastax.spark.connector.rdd.CassandraLeftJoinRDD]] to avoid code duplication.
 *
 * @tparam L item type on the left side of the join (any RDD)
 * @tparam R item type on the right side of the join (fetched from Cassandra)
 */
private[rdd] trait AbstractCassandraJoin[L, R] {

  self: CassandraRDD[(L, R)] with CassandraTableRowReaderProvider[_] =>

  val left: RDD[L]
  val joinColumns: ColumnSelector
  val manualRowWriter: Option[RowWriter[L]]
  implicit val rowWriterFactory: RowWriterFactory[L]

  private[rdd] def fetchIterator(
    session: Session,
    bsb: BoundStatementBuilder[L],
    lastIt: Iterator[L]
  ): Iterator[(L, R)]

  lazy val rowWriter = manualRowWriter match {
    case Some(_rowWriter) => _rowWriter
    case None => implicitly[RowWriterFactory[L]].rowWriter(tableDef, joinColumnNames.toIndexedSeq)
  }

  lazy val joinColumnNames: Seq[ColumnRef] = joinColumns match {
    case AllColumns => throw new IllegalArgumentException(
      "Unable to join against all columns in a Cassandra Table. Only primary key columns allowed."
    )
    case PartitionKeyColumns =>
      tableDef.partitionKey.map(col => col.columnName: ColumnRef)
    case SomeColumns(cs @ _*) =>
      checkColumnsExistence(cs)
      cs.map {
        case c: ColumnRef => c
        case _ => throw new IllegalArgumentException(
          "Unable to join against unnamed columns. No CQL Functions allowed."
        )
      }
  }

  /**
   * This method will create the RowWriter required before the RDD is serialized.
   * This is called during getPartitions
   */
  protected def checkValidJoin(): Seq[ColumnRef] = {
    val partitionKeyColumnNames = tableDef.partitionKey.map(_.columnName).toSet
    val primaryKeyColumnNames = tableDef.primaryKey.map(_.columnName).toSet
    val colNames = joinColumnNames.map(_.columnName).toSet

    // Initialize RowWriter and Query to be used for accessing Cassandra
    rowWriter.columnNames
    singleKeyCqlQuery.length

    def checkSingleColumn(column: ColumnRef): Unit = {
      require(
        primaryKeyColumnNames.contains(column.columnName),
        s"Can't pushdown join on column $column because it is not part of the PRIMARY KEY"
      )
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
        s"Can't pushdown join on column $maxCol without also specifying [ $missingColumns ]"
      )
    }
    val missingPartitionKeys = partitionKeyColumnNames -- colNames
    require(
      missingPartitionKeys.isEmpty,
      s"Can't join without the full partition key. Missing: [ $missingPartitionKeys ]"
    )

    joinColumnNames.foreach(checkSingleColumn)
    joinColumnNames
  }

  //We need to make sure we get selectedColumnRefs before serialization so that our RowReader is
  //built
  lazy val singleKeyCqlQuery: (String) = {
    val whereClauses = where.predicates.flatMap(CqlWhereParser.parse)
    val joinColumns = joinColumnNames.map(_.columnName)
    val joinColumnPredicates = whereClauses.collect {
      case EqPredicate(c, _) if joinColumns.contains(c) => c
      case InPredicate(c) if joinColumns.contains(c) => c
      case InListPredicate(c, _) if joinColumns.contains(c) => c
      case RangePredicate(c, _, _) if joinColumns.contains(c) => c
    }.toSet

    require(
      joinColumnPredicates.isEmpty,
      s"""Columns specified in both the join on clause and the where clause.
         |Partition key columns are always part of the join clause.
         |Columns in both: ${joinColumnPredicates.mkString(", ")}""".stripMargin
    )

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
        s"WHERE $filter $orderBy $limitClause"
    logDebug(s"Query : $query")
    query
  }

  private[rdd] def boundStatementBuilder(session: Session): BoundStatementBuilder[L] = {
    val protocolVersion = session.getCluster.getConfiguration.getProtocolOptions.getProtocolVersion
    val stmt = session.prepare(singleKeyCqlQuery).setConsistencyLevel(consistencyLevel).setIdempotent(true)
    new BoundStatementBuilder[L](rowWriter, stmt, where.values, protocolVersion = protocolVersion)
  }

  /**
   * When computing a CassandraPartitionKeyRDD the data is selected via single CQL statements
   * from the specified C* Keyspace and Table. This will be preformed on whatever data is
   * available in the previous RDD in the chain.
   */
  override def compute(split: Partition, context: TaskContext): Iterator[(L, R)] = {
    val session = connector.openSession()
    val bsb = boundStatementBuilder(session)
    val metricsUpdater = InputMetricsUpdater(context, readConf)
    val rowIterator = fetchIterator(session, bsb, left.iterator(split, context))
    val countingIterator = new CountingIterator(rowIterator, None)

    context.addTaskCompletionListener { (context) =>
      val duration = metricsUpdater.finish() / 1000000000d
      logDebug(
        f"Fetched ${countingIterator.count} rows " +
          f"from $keyspaceName.$tableName " +
          f"for partition ${split.index} in $duration%.3f s."
      )
      session.close()
    }
    countingIterator
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
      readConf = readConf
    )

}

