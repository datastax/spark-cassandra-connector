package com.datastax.spark.connector.rdd

import com.datastax.driver.core.{ResultSet, Session}
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.rdd.reader._
import com.datastax.spark.connector.writer._
import com.google.common.util.concurrent.{FutureCallback, Futures, SettableFuture}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag


case class FCqlWhereClause[L](predicates: Seq[String], values: L => Seq[Any])  {
  def apply(v1: L): CqlWhereClause = CqlWhereClause(predicates,values(v1))
  def and(other: FCqlWhereClause[L]) = FCqlWhereClause(predicates ++ other.predicates, (l: L) => values(l) ++ other.values(l))
}
object FCqlWhereClause{
  def empty[L] : FCqlWhereClause[L] = FCqlWhereClause[L](Nil,(l: L) => Nil)
}


/**
 * An [[org.apache.spark.rdd.RDD RDD]] that will do a selecting join between `left` RDD and the specified
 * Cassandra Table This will perform individual selects to retrieve the rows from Cassandra and will take
 * advantage of RDDs that have been partitioned with the
 * [[com.datastax.spark.connector.rdd.partitioner.ReplicaPartitioner]]
 *
 * @tparam L item type on the left side of the join (any RDD)
 * @tparam R item type on the right side of the join (fetched from Cassandra)
 */
class CassandraJoinRDD[L, R] private[connector](
    override val left: RDD[L],
    val keyspaceName: String,
    val tableName: String,
    val connector: CassandraConnector,
    val columnNames: ColumnSelector = AllColumns,
    val joinColumns: ColumnSelector = PartitionKeyColumns,
    val where: CqlWhereClause = CqlWhereClause.empty,
    val fwhere : FCqlWhereClause[L] = FCqlWhereClause.empty[L],
    val limit: Option[Long] = None,
    val clusteringOrder: Option[ClusteringOrder] = None,
    val readConf: ReadConf = ReadConf(),
    manualRowReader: Option[RowReader[R]] = None,
    override val manualRowWriter: Option[RowWriter[L]] = None)(
  implicit
    val leftClassTag: ClassTag[L],
    val rightClassTag: ClassTag[R],
    @transient val rowWriterFactory: RowWriterFactory[L],
    @transient val rowReaderFactory: RowReaderFactory[R])
  extends CassandraRDD[(L, R)](left.sparkContext, left.dependencies)
  with CassandraTableRowReaderProvider[R]
  with AbstractCassandraJoin[L, R] {

  override type Self = CassandraJoinRDD[L, R]

  override protected val classTag = rightClassTag

  override lazy val rowReader: RowReader[R] = manualRowReader match {
    case Some(rr) => rr
    case None => rowReaderFactory.rowReader(tableDef, columnNames.selectFrom(tableDef))
  }

  protected def copy(
    columnNames: ColumnSelector = columnNames,
    where: CqlWhereClause = where,
    limit: Option[Long] = limit,
    clusteringOrder: Option[ClusteringOrder] = None,
    readConf: ReadConf = readConf,
    connector: CassandraConnector = connector
  ): Self = {

    new CassandraJoinRDD[L, R](
      left = left,
      keyspaceName = keyspaceName,
      tableName = tableName,
      connector = connector,
      columnNames = columnNames,
      joinColumns = joinColumns,
      where = where,
      fwhere = fwhere,
      limit = limit,
      clusteringOrder = clusteringOrder,
      readConf = readConf
    )
  }

  // I was not able to do a proper copy because of the inheritance.
  def setFWhere(
    fwhere : FCqlWhereClause[L]
  ): Self = {

    new CassandraJoinRDD[L, R](
      left = left,
      keyspaceName = keyspaceName,
      tableName = tableName,
      connector = connector,
      columnNames = columnNames,
      joinColumns = joinColumns,
      where = where,
      fwhere = fwhere,
      limit = limit,
      clusteringOrder = clusteringOrder,
      readConf = readConf
    )
  }

  def where(f : FCqlWhereClause[L]) : Self = setFWhere(fwhere = fwhere and f)
  def where(clause : String, f : L => Seq[Any]) : Self = where(FCqlWhereClause(Seq(clause),f))

  override def cassandraCount(): Long = {
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
        fwhere = fwhere,
        limit = limit,
        clusteringOrder = clusteringOrder,
        readConf = readConf
      )

    counts.map(_._2).reduce(_ + _)
  }

  def on(joinColumns: ColumnSelector): CassandraJoinRDD[L, R] = {
    new CassandraJoinRDD[L, R](
      left = left,
      connector = connector,
      keyspaceName = keyspaceName,
      tableName = tableName,
      columnNames = columnNames,
      joinColumns = joinColumns,
      where = where,
      fwhere = fwhere,
      limit = limit,
      clusteringOrder = clusteringOrder,
      readConf = readConf
    )
  }

  override private[rdd] def fetchIterator(
    session: Session,
    bsb: BoundStatementBuilder[L],
    leftIterator: Iterator[L]
  ): Iterator[(L, R)] = {
    val columnNames = selectedColumnRefs.map(_.selectedAs).toIndexedSeq
    val rateLimiter = new RateLimiter(
      readConf.throughputJoinQueryPerSec, readConf.throughputJoinQueryPerSec
    )
    def pairWithRight(left: L): SettableFuture[Iterator[(L, R)]] = {
      val resultFuture = SettableFuture.create[Iterator[(L, R)]]
      val leftSide = Iterator.continually(left)

      val queryFuture = session.executeAsync(bsb.bind(left))
      Futures.addCallback(queryFuture, new FutureCallback[ResultSet] {
        def onSuccess(rs: ResultSet) {
          val resultSet = new PrefetchingResultSetIterator(rs, fetchSize)
          val columnMetaData = CassandraRowMetadata.fromResultSet(columnNames, rs);
          val rightSide = resultSet.map(rowReader.read(_, columnMetaData))
          resultFuture.set(leftSide.zip(rightSide))
        }
        def onFailure(throwable: Throwable) {
          resultFuture.setException(throwable)
        }
      })
      resultFuture
    }
    val queryFutures = leftIterator.map(left => {

      rateLimiter.maybeSleep(1)
      pairWithRight(left)
    }).toList
    queryFutures.iterator.flatMap(_.get)
  }

  /**
   * Turns this CassandraJoinRDD into a factory for converting other RDD's after being serialized
   * This method is for streaming operations as it allows us to Serialize a template JoinRDD
   * and the use that serializable template in the DStream closure. This gives us a fully serializable
   * joinWithCassandra operation
   */
  private[connector] def applyToRDD(left: RDD[L]): CassandraJoinRDD[L, R] = {
    new CassandraJoinRDD[L, R](
      left,
      keyspaceName,
      tableName,
      connector,
      columnNames,
      joinColumns,
      where,
      fwhere,
      limit,
      clusteringOrder,
      readConf,
      Some(rowReader),
      Some(rowWriter)
    )
  }
}
