package com.datastax.spark.connector.rdd

import java.io.IOException

import scala.collection.JavaConversions._
import scala.language.existentials
import scala.reflect.ClassTag

import org.apache.spark.metrics.InputMetricsUpdater
import org.apache.spark.{Partition, SparkContext, TaskContext}

import com.datastax.driver.core._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.rdd.partitioner.dht.TokenFactory
import com.datastax.spark.connector.rdd.partitioner.{CassandraPartition, CassandraRDDPartitioner, CqlTokenRange}
import com.datastax.spark.connector.rdd.reader._
import com.datastax.spark.connector.types.ColumnType
import com.datastax.spark.connector.util.CountingIterator


/** RDD representing a Table Scan of A Cassandra table.
  *
  * This class is the main entry point for analyzing data in Cassandra database with Spark.
  * Obtain objects of this class by calling
  * [[com.datastax.spark.connector.SparkContextFunctions.cassandraTable]].
  *
  * Configuration properties should be passed in the [[org.apache.spark.SparkConf SparkConf]]
  * configuration of [[org.apache.spark.SparkContext SparkContext]].
  * `CassandraRDD` needs to open connection to Cassandra, therefore it requires appropriate
  * connection property values to be present in [[org.apache.spark.SparkConf SparkConf]].
  * For the list of required and available properties, see
  * [[com.datastax.spark.connector.cql.CassandraConnector CassandraConnector]].
  *
  * `CassandraRDD` divides the data set into smaller partitions, processed locally on every
  * cluster node. A data partition consists of one or more contiguous token ranges.
  * To reduce the number of roundtrips to Cassandra, every partition is fetched in batches.
  *
  * The following properties control the number of partitions and the fetch size:
  * - spark.cassandra.input.split.size: approx number of Cassandra partitions in a Spark partition,
  *   default 100000
  * - spark.cassandra.input.page.row.size:  number of CQL rows fetched per roundtrip,
  *   default 1000
  *
  * A `CassandraRDD` object gets serialized and sent to every Spark Executor, which then
  * calls the `compute` method to fetch the data on every node. The `getPreferredLocations`
  * method tells Spark the preferred nodes to fetch a partition from, so that the data for
  * the partition are at the same node the task was sent to. If Cassandra nodes are collocated
  * with Spark nodes, the queries are always sent to the Cassandra process running on the same
  * node as the Spark Executor process, hence data are not transferred between nodes.
  * If a Cassandra node fails or gets overloaded during read, the queries are retried
  * to a different node.
  *
  * By default, reads are performed at ConsistencyLevel.LOCAL_ONE in order to leverage data-locality
  * and minimize network traffic. This read consistency level is controlled by the
  * spark.cassandra.input.consistency.level property.
  */
class CassandraTableScanRDD[R] private[connector](
    @transient val sc: SparkContext,
    val connector: CassandraConnector,
    val keyspaceName: String,
    val tableName: String,
    val columnNames: ColumnSelector = AllColumns,
    val where: CqlWhereClause = CqlWhereClause.empty,
    val limit: Option[Long] = None,
    val clusteringOrder: Option[ClusteringOrder] = None,
    val readConf: ReadConf = ReadConf())(
  implicit
    val classTag: ClassTag[R],
    @transient val rowReaderFactory: RowReaderFactory[R])
  extends CassandraRDD[R](sc, Seq.empty)
  with CassandraTableRowReaderProvider[R] {

  override type Self = CassandraTableScanRDD[R]

  override protected def copy(
    columnNames: ColumnSelector = columnNames,
    where: CqlWhereClause = where,
    limit: Option[Long] = limit,
    clusteringOrder: Option[ClusteringOrder] = None,
    readConf: ReadConf = readConf,
    connector: CassandraConnector = connector): Self = {

    require(sc != null,
      "RDD transformation requires a non-null SparkContext. " +
        "Unfortunately SparkContext in this CassandraRDD is null. " +
        "This can happen after CassandraRDD has been deserialized. " +
        "SparkContext is not Serializable, therefore it deserializes to null." +
        "RDD transformations are not allowed inside lambdas used in other RDD transformations.")

    new CassandraTableScanRDD[R](
      sc = sc,
      connector = connector,
      keyspaceName = keyspaceName,
      tableName = tableName,
      columnNames = columnNames,
      where = where,
      limit = limit,
      clusteringOrder = clusteringOrder,
      readConf = readConf)
  }

  override protected def convertTo[B : ClassTag : RowReaderFactory]: CassandraTableScanRDD[B] = {
    new CassandraTableScanRDD[B](
      sc = sc,
      connector = connector,
      keyspaceName = keyspaceName,
      tableName = tableName,
      columnNames = columnNames,
      where = where,
      limit = limit,
      clusteringOrder = clusteringOrder,
      readConf = readConf)
  }

  override def getPartitions: Array[Partition] = {
    verify() // let's fail fast
    val tf = TokenFactory.forCassandraPartitioner(cassandraPartitionerClassName)
    val partitions = new CassandraRDDPartitioner(connector, tableDef, splitSize)(tf).partitions(where)
    logDebug(s"Created total ${partitions.length} partitions for $keyspaceName.$tableName.")
    logTrace("Partitions: \n" + partitions.mkString("\n"))
    partitions
  }

  override def getPreferredLocations(split: Partition): Seq[String] =
    split.asInstanceOf[CassandraPartition]
      .endpoints.flatMap(inet => Seq(inet.getHostName, inet.getHostAddress)).toSeq.distinct

  private def tokenRangeToCqlQuery(range: CqlTokenRange): (String, Seq[Any]) = {
    val columns = selectedColumnRefs.map(_.cql).mkString(", ")
    val filter = (range.cql +: where.predicates).filter(_.nonEmpty).mkString(" AND ")
    val limitClause = limit.map(limit => s"LIMIT $limit").getOrElse("")
    val orderBy = clusteringOrder.map(_.toCql(tableDef)).getOrElse("")
    val quotedKeyspaceName = quote(keyspaceName)
    val quotedTableName = quote(tableName)
    val queryTemplate =
      s"SELECT $columns " +
        s"FROM $quotedKeyspaceName.$quotedTableName " +
        s"WHERE $filter $orderBy $limitClause ALLOW FILTERING"
    val queryParamValues = range.values ++ where.values
    (queryTemplate, queryParamValues)
  }

  private def createStatement(session: Session, cql: String, values: Any*): Statement = {
    try {
      implicit val pv = protocolVersion(session)
      val stmt = session.prepare(cql)
      stmt.setConsistencyLevel(consistencyLevel)
      val converters = stmt.getVariables
        .map(v => ColumnType.converterToCassandra(v.getType))
        .toArray
      val convertedValues =
        for ((value, converter) <- values zip converters)
        yield converter.convert(value)
      val bstm = stmt.bind(convertedValues: _*)
      bstm.setFetchSize(fetchSize)
      bstm
    }
    catch {
      case t: Throwable =>
        throw new IOException(s"Exception during preparation of $cql: ${t.getMessage}", t)
    }
  }

  private def fetchTokenRange(
    session: Session,
    range: CqlTokenRange,
    inputMetricsUpdater: InputMetricsUpdater): Iterator[R] = {

    val (cql, values) = tokenRangeToCqlQuery(range)
    logDebug(
      s"Fetching data for range ${range.cql} " +
        s"with $cql " +
        s"with params ${values.mkString("[", ",", "]")}")
    val stmt = createStatement(session, cql, values: _*)
    val columnNamesArray = selectedColumnRefs.map(_.selectedAs).toArray

    try {
      implicit val pv = protocolVersion(session)
      val rs = session.execute(stmt)
      val iterator = new PrefetchingResultSetIterator(rs, fetchSize)
      val iteratorWithMetrics = iterator.map(inputMetricsUpdater.updateMetrics)
      val result = iteratorWithMetrics.map(rowReader.read(_, columnNamesArray))
      logDebug(s"Row iterator for range ${range.cql} obtained successfully.")
      result
    } catch {
      case t: Throwable =>
        throw new IOException(s"Exception during execution of $cql: ${t.getMessage}", t)
    }
  }

  override def compute(split: Partition, context: TaskContext): Iterator[R] = {
    val session = connector.openSession()
    val partition = split.asInstanceOf[CassandraPartition]
    val tokenRanges = partition.tokenRanges
    val metricsUpdater = InputMetricsUpdater(context, readConf)

    // Iterator flatMap trick flattens the iterator-of-iterator structure into a single iterator.
    // flatMap on iterator is lazy, therefore a query for the next token range is executed not earlier
    // than all of the rows returned by the previous query have been consumed
    val rowIterator = tokenRanges.iterator.flatMap(
      fetchTokenRange(session, _, metricsUpdater))
    val countingIterator = new CountingIterator(rowIterator, limit)

    context.addTaskCompletionListener { (context) =>
      val duration = metricsUpdater.finish() / 1000000000d
      logDebug(f"Fetched ${countingIterator.count} rows from $keyspaceName.$tableName " +
        f"for partition ${partition.index} in $duration%.3f s.")
      session.close()
    }
    countingIterator
  }

  override def toEmptyCassandraRDD: EmptyCassandraRDD[R] = {
    new EmptyCassandraRDD[R](
      sc = sc,
      keyspaceName = keyspaceName,
      tableName = tableName,
      columnNames = columnNames,
      where = where,
      limit = limit,
      clusteringOrder = clusteringOrder,
      readConf = readConf)
  }

  override def count(): Long = {
    columnNames match {
      case SomeColumns(_) =>
        logWarning("You are about to count rows but an explicit projection has been specified.")
      case _ =>
    }

    val counts =
      new CassandraTableScanRDD[Long](
        sc = sc,
        connector = connector,
        keyspaceName = keyspaceName,
        tableName = tableName,
        columnNames = SomeColumns(RowCountRef),
        where = where,
        limit = limit,
        clusteringOrder = clusteringOrder,
        readConf = readConf)

    counts.reduce(_ + _)
  }
}

object CassandraTableScanRDD {

  def apply[T : ClassTag : RowReaderFactory](
    sc: SparkContext,
    keyspaceName: String,
    tableName: String): CassandraTableScanRDD[T] = {

    new CassandraTableScanRDD[T](
      sc = sc,
      connector = CassandraConnector(sc.getConf),
      keyspaceName = keyspaceName,
      tableName = tableName,
      readConf = ReadConf.fromSparkConf(sc.getConf),
      columnNames = AllColumns,
      where = CqlWhereClause.empty)
  }

  def apply[K, V](
      sc: SparkContext,
      keyspaceName: String,
      tableName: String)(
    implicit
      keyCT: ClassTag[K],
      valueCT: ClassTag[V],
      rrf: RowReaderFactory[(K, V)]): CassandraTableScanRDD[(K, V)] = {

    new CassandraTableScanRDD[(K, V)](
      sc = sc,
      connector = CassandraConnector(sc.getConf),
      keyspaceName = keyspaceName,
      tableName = tableName,
      readConf = ReadConf.fromSparkConf(sc.getConf),
      columnNames = AllColumns,
      where = CqlWhereClause.empty)
  }
}
