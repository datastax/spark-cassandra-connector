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
 * An RDD that will do a selecting join between `prev:RDD` and the specified Cassandra Table
 * This will perform individual selects to retrieve the rows from Cassandra and will take
 * advantage of RDD's that have been partitioned with the [[com.datastax.spark.connector.rdd.partitioner.ReplicaPartitioner]]
 */
class CassandraJoinRDD[Left, Right] private[connector](prev: RDD[Left],
                                                val keyspaceName: String,
                                                val tableName: String,
                                                val connector: CassandraConnector,
                                                val columnNames: ColumnSelector = AllColumns,
                                                val joinColumns: ColumnSelector = PartitionKeyColumns,
                                                val where: CqlWhereClause = CqlWhereClause.empty,
                                                val limit: Option[Long] = None,
                                                val clusteringOrder: Option[ClusteringOrder] = None,
                                                val readConf: ReadConf = ReadConf())
                                                      (implicit oldTag: ClassTag[Left], val rct: ClassTag[Right],
                                                       @transient val rwf: RowWriterFactory[Left], @transient val rtf: RowReaderFactory[Right])
  extends CassandraRDD[(Left, Right)](prev.sparkContext, prev.dependencies) with CassandraTableRowReader[Right] {

  //Make sure copy operations make new CJRDDs and not CRDDs
  override protected def copy(columnNames: ColumnSelector = columnNames,
                              where: CqlWhereClause = where, limit: Option[Long] = limit, clusteringOrder: Option[ClusteringOrder] = None,
                              readConf: ReadConf = readConf, connector: CassandraConnector = connector) =
    new CassandraJoinRDD[Left, Right](prev, keyspaceName, tableName, connector, columnNames, joinColumns, where, limit, clusteringOrder, readConf).asInstanceOf[this.type]

  lazy val joinColumnNames: Seq[NamedColumnRef] = joinColumns match {
    case AllColumns => throw new IllegalArgumentException("Unable to join against all columns in a Cassandra Table. Only primary key columns allowed.")
    case PartitionKeyColumns => tableDef.partitionKey.map(col => col.columnName: NamedColumnRef).toSeq
    case SomeColumns(cs@_*) => {
      checkColumnsExistence(cs)
      checkValidJoin(cs)
    }
  }

  override def count(): Long = {
    columnNames match {
      case SomeColumns(_) => logWarning("You are about to count rows but an explicit projection has been specified.")
      case _ =>
    }
    new CassandraJoinRDD[Left, Long](prev, keyspaceName, tableName, connector, SomeColumns(RowCountRef), joinColumns, where, limit, clusteringOrder, readConf).map(_._2).reduce(_ + _)
  }

  protected def checkValidJoin(columns: Seq[SelectableColumnRef]): Seq[NamedColumnRef] = {
    val regularColumnNames = tableDef.regularColumns.map(_.columnName).toSet
    val partitionKeyColumnNames = tableDef.partitionKey.map(_.columnName).toSet

    val joinColumns = columns.map {
      case c: NamedColumnRef => c
      case _ => throw new IllegalArgumentException("Unable to join against unnamed columns. No CQL Functions allowed.")
    }

    val joinColumnNames = joinColumns.map(_.columnName).toSet

    def checkSingleColumn(column: NamedColumnRef): Unit = {
      if (regularColumnNames.contains(column.columnName))
        throw new IllegalArgumentException(s"Can't pushdown join on column $column because it is not part of the PRIMARY KEY")
    }

    //Make sure we have all of the clustering indexes between the 0th position and the max requested in the join
    val chosenClusteringColumns = tableDef.clusteringColumns
      .filter(cc => joinColumnNames.contains(cc.columnName))
    if (!tableDef.clusteringColumns.startsWith(chosenClusteringColumns)) {
      val maxCol = chosenClusteringColumns.last
      val maxIndex = maxCol.componentIndex.get
      val requiredColumns = tableDef.clusteringColumns.takeWhile(_.componentIndex.get <= maxIndex)
      val missingColumns = requiredColumns.toSet -- chosenClusteringColumns
      if (!missingColumns.isEmpty)
        throw new IllegalArgumentException(s"Can't pushdown join on column $maxCol without also specifying [ $missingColumns ]")
      }
    val missingPartitionKeys = partitionKeyColumnNames -- joinColumnNames
    if (missingPartitionKeys.size != 0) {
      throw new IllegalArgumentException(s"Can't join without the full partition key. Missing: [ ${missingPartitionKeys} ]")
    }
    joinColumns.foreach { case c: NamedColumnRef => checkSingleColumn(c)}
    joinColumns
  }

  val rowWriter = implicitly[RowWriterFactory[Left]].rowWriter(
    tableDef,
    joinColumnNames.map { case c: NamedColumnRef => c.columnName})

  def on(joinColumns: ColumnSelector): CassandraJoinRDD[Left, Right] = {
    new CassandraJoinRDD[Left, Right](prev, keyspaceName, tableName, connector, columnNames, joinColumns, where, limit, clusteringOrder, readConf).asInstanceOf[this.type]
  }

  //We need to make sure we get selectedColumnRefs before serialization so that our RowReader is
  //built
  private val singleKeyCqlQuery: (String) = {
    val whereClauses = where.predicates.flatMap(CqlWhereParser.parse)
    val partitionKeys = tableDef.partitionKey.map(_.columnName)
    val partitionKeyPredicates = whereClauses.collect {
      case EqPredicate(c, _) if partitionKeys.contains(c) => c
      case InPredicate(c) if partitionKeys.contains(c) => c
      case InListPredicate(c, _) if partitionKeys.contains(c) => c
      case RangePredicate(c, _, _) if partitionKeys.contains(c) => c
    }.toSet
    require(partitionKeyPredicates.isEmpty, s"No partition keys allowed in where on joins with Cassandra. Found : $partitionKeyPredicates")

    logDebug("Generating Single Key Query Prepared Statement String")
    logDebug(s"SelectedColumns : $selectedColumnRefs -- JoinColumnNames : $joinColumnNames")
    val columns = selectedColumnRefs.map(_.cql).mkString(", ")
    val joinWhere = joinColumnNames.map(_.columnName).map(name => s"${quote(name)} = :$name")
    val limitClause = limit.map(limit => s"LIMIT $limit").getOrElse("")
    val orderBy = clusteringOrder.map(_.toCql(tableDef)).getOrElse("")
    val filter = (where.predicates ++ joinWhere).mkString(" AND ")
    val quotedKeyspaceName = quote(keyspaceName)
    val quotedTableName = quote(tableName)
    val query = s"SELECT $columns FROM $quotedKeyspaceName.$quotedTableName WHERE $filter $limitClause $orderBy"
    logDebug(s"Query : $query")
    query
  }

  /**
   * When computing a CassandraPartitionKeyRDD the data is selected via single CQL statements
   * from the specified C* Keyspace and Table. This will be preformed on whatever data is
   * avaliable in the previous RDD in the chain.
   */
  override def compute(split: Partition, context: TaskContext): Iterator[(Left, Right)] = {
    val session = connector.openSession()
    implicit val pv = protocolVersion(session)
    val stmt = session.prepare(singleKeyCqlQuery).setConsistencyLevel(consistencyLevel)
    val bsb = new BoundStatementBuilder[Left](rowWriter, stmt, pv)
    val metricsUpdater = InputMetricsUpdater(context, 20)
    val rowIterator = fetchIterator(session, bsb, prev.iterator(split, context))
    val countingIterator = new CountingIterator(rowIterator, limit)
    context.addTaskCompletionListener { (context) =>
      val duration = metricsUpdater.finish() / 1000000000d
      logDebug(f"Fetched ${countingIterator.count} rows from $keyspaceName.$tableName for partition ${split.index} in $duration%.3f s.")
      session.close()
    }
    countingIterator
  }

  private def fetchIterator(session: Session, bsb: BoundStatementBuilder[Left], lastIt: Iterator[Left]): Iterator[(Left, Right)] = {
    val columnNamesArray = selectedColumnRefs.map(_.selectedAs).toArray
    implicit val pv = protocolVersion(session)
    for (leftSide <- lastIt;
         rightSide <- {
           val rs = session.execute(bsb.bind(leftSide))
           val iterator = new PrefetchingResultSetIterator(rs, fetchSize)
           iterator.map(rowReader.read(_, columnNamesArray))
         }) yield (leftSide, rightSide)
  }

  override protected def getPartitions: Array[Partition] = prev.partitions

  override def getPreferredLocations(split: Partition): Seq[String] = prev.preferredLocations(split)

  override def toEmptyCassandraRDD(): EmptyCassandraRDD[(Left, Right)] = new EmptyCassandraRDD[(Left, Right)](prev.sparkContext, keyspaceName, tableName, columnNames, where, limit, clusteringOrder, readConf)
}