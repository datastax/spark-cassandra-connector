package com.datastax.spark.connector.rdd

import java.io.IOException

import com.datastax.driver.core._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.rdd.CassandraLimit._
import com.datastax.spark.connector.rdd.partitioner.dht.{Token => ConnectorToken}
import com.datastax.spark.connector.rdd.partitioner.{CassandraPartition, CassandraPartitionGenerator, CqlTokenRange, NodeAddresses, _}
import com.datastax.spark.connector.rdd.reader._
import com.datastax.spark.connector.types.ColumnType
import com.datastax.spark.connector.util.CqlWhereParser.{EqPredicate, InListPredicate, InPredicate, Predicate, RangePredicate}
import com.datastax.spark.connector.util.Quote._
import com.datastax.spark.connector.util.{CountingIterator, CqlWhereParser, ReflectionUtil}
import com.datastax.spark.connector.writer.RowWriterFactory
import org.apache.spark.metrics.InputMetricsUpdater
import org.apache.spark.rdd.{PartitionCoalescer, RDD}
import org.apache.spark.{Partition, Partitioner, SparkContext, TaskContext}

import scala.collection.JavaConversions._
import scala.language.existentials
import scala.reflect.ClassTag


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
  * - spark.cassandra.input.split.size_in_mb: approx amount of data to be fetched into a single Spark
  *   partition, default 64 MB
  * - spark.cassandra.input.fetch.size_in_rows:  number of CQL rows fetched per roundtrip,
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
    val limit: Option[CassandraLimit] = None,
    val clusteringOrder: Option[ClusteringOrder] = None,
    val readConf: ReadConf = ReadConf(),
    overridePartitioner: Option[Partitioner] = None)(
  implicit
    val classTag: ClassTag[R],
    @transient val rowReaderFactory: RowReaderFactory[R])
  extends CassandraRDD[R](sc, Seq.empty)
  with CassandraTableRowReaderProvider[R]
  with SplitSizeEstimator[R] {

  override type Self = CassandraTableScanRDD[R]

  override protected def copy(
    columnNames: ColumnSelector = columnNames,
    where: CqlWhereClause = where,
    limit: Option[CassandraLimit] = limit,
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
      readConf = readConf,
      overridePartitioner = overridePartitioner)
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
      readConf = readConf,
      overridePartitioner = overridePartitioner)
  }

  /**
    * Internal method for assigning a partitioner to this RDD, this lacks type safety checks for
    * the Partitioner of type [K]. End users will use the implicit provided in
    * [[CassandraTableScanPairRDDFunctions]]
    */
  private[connector] def withPartitioner[K, V, T <: ConnectorToken[V]](
    partitioner: Option[Partitioner]): CassandraTableScanRDD[R] = {

    val cassPart = partitioner match {
      case Some(newPartitioner: CassandraPartitioner[K, V, T]) => {
        this.partitioner match {
          case Some(currentPartitioner: CassandraPartitioner[K, V, T]) =>
            /** Preserve the mapping set by the current partitioner **/
            logDebug(
              s"""Preserving Partitioner: $currentPartitioner with mapping
                 |${currentPartitioner.keyMapping}""".stripMargin)
            Some(
              newPartitioner
                .withTableDef(tableDef)
                .withKeyMapping(currentPartitioner.keyMapping))
          case _ =>
            logDebug(s"Assigning new Partitioner $newPartitioner")
            Some(newPartitioner.withTableDef(tableDef))
        }
      }
      case Some(other: Partitioner) => throw new IllegalArgumentException(
        s"""Unable to assign
          |non-CassandraPartitioner $other to CassandraTableScanRDD """.stripMargin)
      case None => None
    }

    new CassandraTableScanRDD[R](
      sc = sc,
      connector = connector,
      keyspaceName = keyspaceName,
      tableName = tableName,
      columnNames = columnNames,
      where = where,
      limit = limit,
      clusteringOrder = clusteringOrder,
      readConf = readConf,
      overridePartitioner = cassPart)
  }

  /** Selects a subset of columns mapped to the key and returns an RDD of pairs.
    * Similar to the builtin Spark keyBy method, but this one uses implicit
    * RowReaderFactory to construct the key objects.
    * The selected columns must be available in the CassandraRDD.
    *
    * If the selected columns contain the complete partition key a
    * `CassandraPartitioner` will also be created.
    *
    * @param columns column selector passed to the rrf to create the row reader,
    *                useful when the key is mapped to a tuple or a single value
    */
  def keyBy[K](columns: ColumnSelector)(implicit
    classtag: ClassTag[K],
    rrf: RowReaderFactory[K],
    rwf: RowWriterFactory[K]): CassandraTableScanRDD[(K, R)] = {

    val kRRF = implicitly[RowReaderFactory[K]]
    val vRRF = rowReaderFactory
    implicit val kvRRF = new KeyValueRowReaderFactory[K, R](columns, kRRF, vRRF)

    val selectedColumnNames = columns.selectFrom(tableDef).map(_.columnName).toSet
    val partitionKeyColumnNames = PartitionKeyColumns.selectFrom(tableDef).map(_.columnName).toSet

    if (selectedColumnNames.containsAll(partitionKeyColumnNames)) {
      val partitioner = partitionGenerator.partitioner[K](columns)
      logDebug(
        s"""Made partitioner ${partitioner} for $this""".stripMargin)
      convertTo[(K, R)].withPartitioner(partitioner)

    } else {
      convertTo[(K, R)]
    }
  }

  /** Extracts a key of the given class from the given columns.
    *
    *  @see `keyBy(ColumnSelector)` */
  def keyBy[K](columns: ColumnRef*)(implicit
    classtag: ClassTag[K],
    rrf: RowReaderFactory[K],
    rwf: RowWriterFactory[K]): CassandraTableScanRDD[(K, R)] =
    keyBy(SomeColumns(columns: _*))

  /** Extracts a key of the given class from all the available columns.
    *
    * @see `keyBy(ColumnSelector)` */
  def keyBy[K]()(implicit
    classtag: ClassTag[K],
    rrf: RowReaderFactory[K],
    rwf: RowWriterFactory[K]): CassandraTableScanRDD[(K, R)] =
    keyBy(AllColumns)

  @transient lazy val partitionGenerator = {
    if (containsPartitionKey(where)) {
      CassandraPartitionGenerator(connector, tableDef, 1)
    } else {
      val reevaluatedSplitCount = splitCount.getOrElse(estimateSplitCount(splitSize))
      CassandraPartitionGenerator(connector, tableDef, reevaluatedSplitCount)
    }
  }

  /**
    * This method overrides the default spark behavior and will not create a CoalesceRDD. Instead it will reduce
    * the number of partitions by adjusting the partitioning of C* data on read. Using this method will override
    * spark.cassandra.input.split.size.
    * The method is useful with where() method call, when actual size of data is smaller then the table size.
    * It has no effect if a partition key is used in where clause.
    *
    * @param numPartitions      number of partitions
    * @param shuffle            whether to call shuffle after
    * @param partitionCoalescer is ignored if no shuffle, or just passed to shuffled CoalesceRDD
    * @param ord
    * @return new CassandraTableScanRDD with predefined number of partitions
    */

  override def coalesce(numPartitions: Int, shuffle: Boolean = false, partitionCoalescer: Option[PartitionCoalescer])(implicit ord: Ordering[R] = null): RDD[R]
  = {
    val rdd = copy(readConf = readConf.copy(splitCount = Some(numPartitions)))
    if (shuffle) {
      rdd.superCoalesce(numPartitions, shuffle, partitionCoalescer)
    } else {
      rdd
    }
  }

  private def superCoalesce(numPartitions: Int, shuffle: Boolean = false, partitionCoalescer: Option[PartitionCoalescer])(implicit ord: Ordering[R] = null) =
    super.coalesce(numPartitions, shuffle, partitionCoalescer);


  @transient override val partitioner = overridePartitioner

  override def getPartitions: Array[Partition] = {
    verify() // let's fail fast
    val partitions: Array[Partition] = partitioner match {
      case Some(cassPartitioner: CassandraPartitioner[_, _, _]) => {
        cassPartitioner.verify()
        cassPartitioner.partitions.toArray[Partition]
      }
      case Some(other: Partitioner) =>
        throw new IllegalArgumentException(s"Invalid partitioner $other")

      case None => partitionGenerator.partitions.toArray[Partition]
    }

    logDebug(s"Created total ${partitions.length} partitions for $keyspaceName.$tableName.")
    logTrace("Partitions: \n" + partitions.mkString("\n"))
    partitions
  }

  private lazy val nodeAddresses = new NodeAddresses(connector)

  private lazy val partitionKeyStr =
    tableDef.partitionKey.map(_.columnName).map(quote).mkString(", ")

  override def getPreferredLocations(split: Partition): Seq[String] =
    split.asInstanceOf[CassandraPartition[_, _]].endpoints.flatMap(nodeAddresses.hostNames).toSeq

  private def tokenRangeToCqlQuery(range: CqlTokenRange[_, _]): (String, Seq[Any]) = {
    val columns = selectedColumnRefs.map(_.cql).mkString(", ")
    val (cql, values) = if (containsPartitionKey(where)) {
      ("", Seq.empty)
    } else {
      range.cql(partitionKeyStr)
    }
    val filter = (cql +: where.predicates).filter(_.nonEmpty).mkString(" AND ")
    val limitClause = limitToClause(limit)
    val orderBy = clusteringOrder.map(_.toCql(tableDef)).getOrElse("")
    val quotedKeyspaceName = quote(keyspaceName)
    val quotedTableName = quote(tableName)
    val queryTemplate =
      s"SELECT $columns " +
        s"FROM $quotedKeyspaceName.$quotedTableName " +
        s"WHERE $filter $orderBy $limitClause ALLOW FILTERING"
    val queryParamValues = values ++ where.values
    (queryTemplate, queryParamValues)
  }

  private def createStatement(session: Session, cql: String, values: Any*): Statement = {
    try {
      val stmt = session.prepare(cql).setIdempotent(true)
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
    scanner: Scanner,
    range: CqlTokenRange[_, _],
    inputMetricsUpdater: InputMetricsUpdater): Iterator[R] = {

    val session = scanner.getSession()

    val (cql, values) = tokenRangeToCqlQuery(range)
    logDebug(
      s"Fetching data for range ${range.cql(partitionKeyStr)} " +
        s"with $cql " +
        s"with params ${values.mkString("[", ",", "]")}")
    val stmt = createStatement(session, cql, values: _*)

    try {
      val scanResult = scanner.scan(stmt)

      val iteratorWithMetrics = scanResult.rows.map(inputMetricsUpdater.updateMetrics)
      val result = iteratorWithMetrics.map(rowReader.read(_, scanResult.metadata))
      logDebug(s"Row iterator for range ${range.cql(partitionKeyStr)} obtained successfully.")
      result
    } catch {
      case t: Throwable =>
        throw new IOException(s"Exception during execution of $cql: ${t.getMessage}", t)
    }
  }

  override def compute(split: Partition, context: TaskContext): Iterator[R] = {
    val partition = split.asInstanceOf[CassandraPartition[_, _]]
    val tokenRanges = partition.tokenRanges
    val metricsUpdater = InputMetricsUpdater(context, readConf)

    val columnNames = selectedColumnRefs.map(_.selectedAs).toIndexedSeq

    val scanner = connector.connectionFactory.getScanner(readConf, connector.conf, columnNames)

    // Iterator flatMap trick flattens the iterator-of-iterator structure into a single iterator.
    // flatMap on iterator is lazy, therefore a query for the next token range is executed not earlier
    // than all of the rows returned by the previous query have been consumed
    val rowIterator = tokenRanges.iterator.flatMap(
      fetchTokenRange(scanner, _: CqlTokenRange[_, _], metricsUpdater))
    val countingIterator = new CountingIterator(rowIterator, limitForIterator(limit))

    context.addTaskCompletionListener { (context) =>
      val duration = metricsUpdater.finish() / 1000000000d
      logDebug(f"Fetched ${countingIterator.count} rows from $keyspaceName.$tableName " +
        f"for partition ${partition.index} in $duration%.3f s.")
      scanner.close()
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

  override def cassandraCount(): Long = {
    columnNames match {
      case SomeColumns(_) =>
        logWarning("You are about to count rows but an explicit projection has been specified.")
      case _ =>
    }

    val counts = CassandraTableScanRDD.countRDD(this)
    counts.reduce(_ + _)
  }


  private def containsPartitionKey(clause: CqlWhereClause): Boolean = {
    val pk = tableDef.partitionKey.map(_.columnName).toSet
    val wherePredicates: Seq[Predicate] = clause.predicates.flatMap(CqlWhereParser.parse)

    val whereColumns: Set[String] = wherePredicates.collect {
      case EqPredicate(c, _) if pk.contains(c) => c
      case InPredicate(c) if pk.contains(c) => c
      case InListPredicate(c, _) if pk.contains(c) => c
      case RangePredicate(c, _, _) if pk.contains(c) =>
        throw new UnsupportedOperationException(
          s"Range predicates on partition key columns (here: $c) are " +
            s"not supported in where. Use filter instead.")
    }.toSet

    val primaryKeyComplete = whereColumns.nonEmpty && whereColumns.size == pk.size
    val whereColumnsAllIndexed = whereColumns.forall(tableDef.isIndexed)

    if (!primaryKeyComplete && !whereColumnsAllIndexed) {
      val missing = pk -- whereColumns
      throw new UnsupportedOperationException(
        s"Partition key predicate must include all partition key columns or partition key columns need" +
          s" to be indexed. Missing columns: ${missing.mkString(",")}"
      )
    }
    primaryKeyComplete
  }
}

object CassandraTableScanRDD {

  def apply[T : ClassTag : RowReaderFactory](
    sc: SparkContext,
    keyspaceName: String,
    tableName: String): CassandraTableScanRDD[T] = {

    new CassandraTableScanRDD[T](
      sc = sc,
      connector = CassandraConnector(sc),
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
      rrf: RowReaderFactory[(K, V)],
      rwf: RowWriterFactory[K]): CassandraTableScanRDD[(K, V)] = {

    val rdd = new CassandraTableScanRDD[(K, V)](
      sc = sc,
      connector = CassandraConnector(sc),
      keyspaceName = keyspaceName,
      tableName = tableName,
      readConf = ReadConf.fromSparkConf(sc.getConf),
      columnNames = AllColumns,
      where = CqlWhereClause.empty)
    rdd.withPartitioner(rdd.partitionGenerator.partitioner[K](PartitionKeyColumns))
  }

  /**
    * It is used by cassandraCount() and spark sql cassandra source to push down counts to cassandra
    * @param rdd
    * @tparam R
    * @return rdd, each partitions will have only one long value: number of rows in the partition
    */
  def countRDD[R] (rdd: CassandraTableScanRDD[R]): CassandraTableScanRDD[Long] = {
    new CassandraTableScanRDD[Long](
      sc = rdd.sc,
      connector = rdd.connector,
      keyspaceName = rdd.keyspaceName,
      tableName = rdd.tableName,
      columnNames = SomeColumns(RowCountRef),
      where = rdd.where,
      limit = rdd.limit,
      clusteringOrder = rdd.clusteringOrder,
      readConf = rdd.readConf)
  }
}
