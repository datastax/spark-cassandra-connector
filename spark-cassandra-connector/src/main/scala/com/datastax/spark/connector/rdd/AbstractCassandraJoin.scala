package com.datastax.spark.connector.rdd

import org.apache.spark.metrics.InputMetricsUpdater

import com.datastax.spark.connector._
import com.datastax.spark.connector.writer._
import com.datastax.spark.connector.util.CqlWhereParser.{EqPredicate, InListPredicate, InPredicate, RangePredicate}
import com.datastax.spark.connector.util.{CountingIterator, CqlWhereParser}
import com.datastax.spark.connector.util.Quote._

import com.datastax.driver.core.Session

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD

private[rdd] trait AbstractCassandraJoin[L, O]{

  self: CassandraRDD[(L, O)] with (CassandraTableRowReaderProvider[R] forSome {type R}) =>

  val left: RDD[L]
  val joinColumns: ColumnSelector

  implicit val rowWriterFactory: RowWriterFactory[L]

  lazy val rowWriter = implicitly[RowWriterFactory[L]].rowWriter(
    tableDef, joinColumnNames.toIndexedSeq)

  lazy val joinColumnNames: Seq[ColumnRef] = joinColumns match {
    case AllColumns => throw new IllegalArgumentException(
      "Unable to join against all columns in a Cassandra Table. Only primary key columns allowed.")
    case PartitionKeyColumns =>
      tableDef.partitionKey.map(col => col.columnName: ColumnRef)
    case SomeColumns(cs @ _*) =>
      checkColumnsExistence(cs)
      cs.map {
        case c: ColumnRef => c
        case _ => throw new IllegalArgumentException(
          "Unable to join against unnamed columns. No CQL Functions allowed.")
      }
  }

  /** This method will create the RowWriter required before the RDD is serialized.
    * This is called during getPartitions */
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
      missingPartitionKeys.isEmpty,
      s"Can't join without the full partition key. Missing: [ $missingPartitionKeys ]")

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
        s"WHERE $filter $limitClause $orderBy"
    logDebug(s"Query : $query")
    query
  }

  /**
   * When computing a CassandraPartitionKeyRDD the data is selected via single CQL statements
   * from the specified C* Keyspace and Table. This will be preformed on whatever data is
   * available in the previous RDD in the chain.
   */
  override def compute(split: Partition, context: TaskContext): Iterator[(L, O)] = {
    val session = connector.openSession()
    implicit val pv = protocolVersion(session)
    val stmt = session.prepare(singleKeyCqlQuery).setConsistencyLevel(consistencyLevel)
    val bsb = new BoundStatementBuilder[L](rowWriter, stmt, pv, where.values)
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

  override protected def getPartitions: Array[Partition] = {
    verify()
    checkValidJoin()
    left.partitions
  }

  override def getPreferredLocations(split: Partition): Seq[String] = left.preferredLocations(split)

  override def toEmptyCassandraRDD: EmptyCassandraRDD[(L, O)] =
    new EmptyCassandraRDD[(L, O)](
      sc = left.sparkContext,
      keyspaceName = keyspaceName,
      tableName = tableName,
      columnNames = columnNames,
      where = where,
      limit = limit,
      clusteringOrder = clusteringOrder,
      readConf = readConf)

  private[rdd] def fetchIterator(
    session: Session,
    bsb: BoundStatementBuilder[L],
    lastIt: Iterator[L]): Iterator[(L, O)] 

}


