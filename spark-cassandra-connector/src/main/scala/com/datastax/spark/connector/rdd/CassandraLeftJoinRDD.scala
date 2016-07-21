package com.datastax.spark.connector.rdd

import org.apache.spark.metrics.InputMetricsUpdater

import com.datastax.driver.core.Session
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.rdd.reader._
import com.datastax.spark.connector.util.CqlWhereParser.{EqPredicate, InListPredicate, InPredicate, RangePredicate}
import com.datastax.spark.connector.util.{CountingIterator, CqlWhereParser}
import com.datastax.spark.connector.writer._
import com.datastax.spark.connector.util.Quote._
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}

import scala.reflect.ClassTag

/**
 * An [[org.apache.spark.rdd.RDD RDD]] that will do a selecting join between `left` RDD and the specified
 * Cassandra Table This will perform individual selects to retrieve the rows from Cassandra and will take
 * advantage of RDDs that have been partitioned with the
 * [[com.datastax.spark.connector.rdd.partitioner.ReplicaPartitioner]]
 *
 * @tparam L item type on the left side of the join (any RDD)
 * @tparam R item type on the right side of the join (fetched from Cassandra)
 */
class CassandraLeftJoinRDD[L, R] private[connector](
   override val left: RDD[L],
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
 extends CassandraRDD[(L, Option[R])](left.sparkContext, left.dependencies)
 with CassandraTableRowReaderProvider[R]
 with AbstractCassandraJoin[L, Option[R]] {

 override type Self = CassandraLeftJoinRDD[L, R]

 override protected val classTag = rightClassTag

 override protected def copy(
   columnNames: ColumnSelector = columnNames,
   where: CqlWhereClause = where,
   limit: Option[Long] = limit,
   clusteringOrder: Option[ClusteringOrder] = None,
   readConf: ReadConf = readConf,
   connector: CassandraConnector = connector): Self = {

    new CassandraLeftJoinRDD[L, R](
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

  override def cassandraCount(): Long = {
    columnNames match {
      case SomeColumns(_) =>
        logWarning("You are about to count rows but an explicit projection has been specified.")
      case _ =>
    }

    val counts =
      new CassandraLeftJoinRDD[L, Long](
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

    // count only Some(_) rows
    counts.map(_._2.getOrElse(0L)).reduce( _ + _ )
  }

  def on(joinColumns: ColumnSelector): CassandraLeftJoinRDD[L, R] = {
    new CassandraLeftJoinRDD[L, R](
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

  private[rdd] override def fetchIterator(
                             session: Session,
                             bsb: BoundStatementBuilder[L],
                             lastIt: Iterator[L]): Iterator[(L, Option[R])] = {

    val columnNamesArray = selectedColumnRefs.map(_.selectedAs).toArray
    implicit val pv = protocolVersion(session)
    for (leftSide <- lastIt;
         rightSide <- {
           val rs = session.execute(bsb.bind(leftSide))
           val iterator = new PrefetchingResultSetIterator(rs, fetchSize)
           if ( iterator.isEmpty ) Seq(None)
           else iterator.map( r => Some( rowReader.read(r, columnNamesArray) ) )
         }) yield (leftSide, rightSide)
  }

  override protected def getPartitions: Array[Partition] = {
    verify()
    checkValidJoin()
    left.partitions
  }

  override def getPreferredLocations(split: Partition): Seq[String] = left.preferredLocations(split)

  override def toEmptyCassandraRDD: EmptyCassandraRDD[(L, Option[R])] =
    new EmptyCassandraRDD[(L, Option[R])](
      sc = left.sparkContext,
      keyspaceName = keyspaceName,
      tableName = tableName,
      columnNames = columnNames,
      where = where,
      limit = limit,
      clusteringOrder = clusteringOrder,
      readConf = readConf)
}
