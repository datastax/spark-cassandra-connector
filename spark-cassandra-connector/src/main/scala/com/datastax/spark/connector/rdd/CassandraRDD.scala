package com.datastax.spark.connector.rdd

import java.io.IOException

import scala.reflect.ClassTag
import scala.collection.JavaConversions._
import scala.language.existentials

import com.datastax.driver.core.{Session, Statement}
import com.datastax.spark.connector.mapper.{WriteTime, TTL, IndexedByNameColumnRef, NamedColumnRef}
import com.datastax.spark.connector.{SomeColumns, AllColumns, ColumnSelector}
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.rdd.partitioner.{CassandraRDDPartitioner, CassandraPartition, CqlTokenRange}
import com.datastax.spark.connector.rdd.partitioner.dht.TokenFactory
import com.datastax.spark.connector.rdd.reader._
import com.datastax.spark.connector.types.{ColumnType, TypeConverter}
import com.datastax.spark.connector.util.{Logging, CountingIterator}

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}
import com.datastax.spark.connector._


/** RDD representing a Cassandra table.
  * This class is the main entry point for analyzing data in Cassandra database with Spark.
  * Obtain objects of this class by calling [[com.datastax.spark.connector.SparkContextFunctions#cassandraTable cassandraTable]].
  *
  * Configuration properties should be passed in the `SparkConf` configuration of `SparkContext`.
  * `CassandraRDD` needs to open connection to Cassandra, therefore it requires appropriate connection property values
  * to be present in `SparkConf`. For the list of required and available properties, see
  * [[com.datastax.spark.connector.cql.CassandraConnector CassandraConnector]].
  *
  * `CassandraRDD` divides the dataset into smaller partitions, processed locally on every cluster node.
  * A data partition consists of one or more contiguous token ranges.
  * To reduce the number of roundtrips to Cassandra, every partition is fetched in batches. The following
  * properties control the number of partitions and the fetch size:
  *
  *   - spark.cassandra.input.split.size:        approx number of Cassandra partitions in a Spark partition, default 100000
  *   - spark.cassandra.input.page.row.size:     number of CQL rows fetched per roundtrip, default 1000
  *
  * A `CassandraRDD` object gets serialized and sent to every Spark executor.
  *
  * By default, reads are performed at ConsistencyLevel.LOCAL_ONE in order to leverage data-locality and minimize network traffic.
  * This read consistency level is controlled by the following property:
  *
  *   - spark.cassandra.input.consistency.level: consistency level for RDD reads, string matching the ConsistencyLevel enum name.
  *
  * If a Cassandra node fails or gets overloaded during read, queries are retried to a different node.
  */
class CassandraRDD[R] private[connector] (
    @transient sc: SparkContext,
    val connector: CassandraConnector,
    val keyspaceName: String,
    val tableName: String,
    val columnNames: ColumnSelector = AllColumns,
    val where: CqlWhereClause = CqlWhereClause.empty,
    val readConf: ReadConf = ReadConf())(
  implicit
    ct : ClassTag[R], @transient rtf: RowReaderFactory[R])
  extends RDD[R](sc, Seq.empty) with Logging {

  /* Logging classes inheritance conflict fix. */
  override def log = super[Logging].log
  override def logName = super[Logging].logName
  override def logInfo(msg: => String) = super[Logging].logInfo(msg)
  override def logDebug(msg: => String) = super[Logging].logDebug(msg)
  override def logTrace(msg: => String) = super[Logging].logTrace(msg)
  override def logWarning(msg: => String) = super[Logging].logWarning(msg)
  override def logError(msg: => String) = super[Logging].logError(msg)
  override def logInfo(msg: => String, throwable: Throwable) = super[Logging].logInfo(msg, throwable)
  override def logDebug(msg: => String, throwable: Throwable) = super[Logging].logDebug(msg, throwable)
  override def logTrace(msg: => String, throwable: Throwable) = super[Logging].logTrace(msg, throwable)
  override def logWarning(msg: => String, throwable: Throwable) = super[Logging].logWarning(msg, throwable)
  override def logError(msg: => String, throwable: Throwable) = super[Logging].logError(msg, throwable)
  override def isTraceEnabled() = super[Logging].isTraceEnabled()

  private def fetchSize = readConf.fetchSize
  private def splitSize = readConf.splitSize
  private def consistencyLevel = readConf.consistencyLevel

  private def copy(columnNames: ColumnSelector = columnNames,
                   where: CqlWhereClause = where,
                   readConf: ReadConf = readConf, connector: CassandraConnector = connector): CassandraRDD[R] = {
    require(sc != null,
      "RDD transformation requires a non-null SparkContext. Unfortunately SparkContext in this CassandraRDD is null. " +
      "This can happen after CassandraRDD has been deserialized. SparkContext is not Serializable, therefore it deserializes to null." +
      "RDD transformations are not allowed inside lambdas used in other RDD transformations.")
    new CassandraRDD(sc, connector, keyspaceName, tableName, columnNames, where, readConf)
  }

  /** Returns a copy of this Cassandra RDD with specified connector */
  def withConnector(connector: CassandraConnector): CassandraRDD[R] =
    copy(connector = connector)

  /** Adds a CQL `WHERE` predicate(s) to the query.
    * Useful for leveraging secondary indexes in Cassandra.
    * Implicitly adds an `ALLOW FILTERING` clause to the WHERE clause, however beware that some predicates
    * might be rejected by Cassandra, particularly in cases when they filter on an unindexed, non-clustering column.*/
  def where(cql: String, values: Any*): CassandraRDD[R] = {
    copy(where = where and CqlWhereClause(Seq(cql), values))
  }

  /** Allows to set custom read configuration, e.g. consistency level or fetch size. */
  def withReadConf(readConf: ReadConf) = {
    copy(readConf = readConf)
  }

  /** Throws IllegalArgumentException if columns sequence contains unavailable columns */
  private def checkColumnsAvailable(columns: Seq[IndexedByNameColumnRef], availableColumns: Seq[IndexedByNameColumnRef]) {
    val availableColumnsSet = availableColumns.collect {
      case NamedColumnRef(columnName) => columnName
    }.toSet

    val notFound = columns.collectFirst {
      case NamedColumnRef(columnName) if !availableColumnsSet.contains(columnName) => columnName
    }

    if (notFound.isDefined)
      throw new IllegalArgumentException(
        s"Column not found in selection: ${notFound.get}. " +
          s"Available columns: [${availableColumns.mkString(",")}].")
  }

  /** Filters currently selected set of columns with a new set of columns */
  private def narrowColumnSelection(columns: Seq[IndexedByNameColumnRef]): Seq[IndexedByNameColumnRef] = {
    columnNames match {
      case SomeColumns(cs) =>
        checkColumnsAvailable(columns, cs)
      case AllColumns =>
        // we do not check for column existence yet as it would require fetching schema and a call to C*
        // columns existence will be checked by C* once the RDD gets computed.
    }
    columns
  }

  /** Narrows down the selected set of columns.
    * Use this for better performance, when you don't need all the columns in the result RDD.
    * When called multiple times, it selects the subset of the already selected columns, so
    * after a column was removed by the previous `select` call, it is not possible to
    * add it back.
    *
    * The selected columns are [[IndexedByNameColumnRef]] instances. This type allows to specify columns for
    * straightforward retrieval and to read TTL or write time of regular columns as well. Implicit
    * conversions included in [[com.datastax.spark.connector]] package make it possible to provide
    * just column names (which is also backward compatible) and optional add `.ttl` or `.writeTime`
    * suffix in order to create an appropriate [[IndexedByNameColumnRef]] instance.
    */
  def select(columns: IndexedByNameColumnRef*): CassandraRDD[R] = {
    copy(columnNames = SomeColumns(narrowColumnSelection(columns): _*))
  }

  /** Maps each row into object of a different type using provided function taking column value(s) as argument(s).
    * Can be used to convert each row to a tuple or a case class object:
    * {{{
    * sc.cassandraTable("ks", "table").select("column1").as((s: String) => s)                 // yields CassandraRDD[String]
    * sc.cassandraTable("ks", "table").select("column1", "column2").as((_: String, _: Long))  // yields CassandraRDD[(String, Long)]
    *
    * case class MyRow(key: String, value: Long)
    * sc.cassandraTable("ks", "table").select("column1", "column2").as(MyRow)                 // yields CassandraRDD[MyRow]
    * }}}*/
  def as[B : ClassTag, A0 : TypeConverter](f: A0 => B): CassandraRDD[B] = {
    implicit val ft = new FunctionBasedRowReader1(f)
    new CassandraRDD[B](sc, connector, keyspaceName, tableName, columnNames, where, readConf)
  }

  def as[B : ClassTag, A0 : TypeConverter, A1 : TypeConverter](f: (A0, A1) => B) = {
    implicit val ft = new FunctionBasedRowReader2(f)
    new CassandraRDD[B](sc, connector, keyspaceName, tableName, columnNames, where, readConf)
  }

  def as[B : ClassTag, A0 : TypeConverter, A1 : TypeConverter, A2 : TypeConverter](f: (A0, A1, A2) => B) = {
    implicit val ft = new FunctionBasedRowReader3(f)
    new CassandraRDD[B](sc, connector, keyspaceName, tableName, columnNames, where, readConf)
  }

  def as[B : ClassTag, A0 : TypeConverter, A1 : TypeConverter, A2 : TypeConverter,
  A3 : TypeConverter](f: (A0, A1, A2, A3) => B) = {
    implicit val ft = new FunctionBasedRowReader4(f)
    new CassandraRDD[B](sc, connector, keyspaceName, tableName, columnNames, where, readConf)
  }

  def as[B : ClassTag, A0 : TypeConverter, A1 : TypeConverter, A2 : TypeConverter, A3 : TypeConverter,
  A4 : TypeConverter](f: (A0, A1, A2, A3, A4) => B) = {
    implicit val ft = new FunctionBasedRowReader5(f)
    new CassandraRDD[B](sc, connector, keyspaceName, tableName, columnNames, where, readConf)
  }

  def as[B : ClassTag, A0 : TypeConverter, A1 : TypeConverter, A2 : TypeConverter, A3 : TypeConverter,
  A4 : TypeConverter, A5 : TypeConverter](f: (A0, A1, A2, A3, A4, A5) => B) = {
    implicit val ft = new FunctionBasedRowReader6(f)
    new CassandraRDD[B](sc, connector, keyspaceName, tableName, columnNames, where, readConf)
  }

  def as[B : ClassTag, A0 : TypeConverter, A1 : TypeConverter, A2 : TypeConverter, A3 : TypeConverter,
  A4 : TypeConverter, A5 : TypeConverter, A6 : TypeConverter](f: (A0, A1, A2, A3, A4, A5, A6) => B) = {
    implicit val ft = new FunctionBasedRowReader7(f)
    new CassandraRDD[B](sc, connector, keyspaceName, tableName, columnNames, where, readConf)
  }

  def as[B : ClassTag, A0 : TypeConverter, A1 : TypeConverter, A2 : TypeConverter, A3 : TypeConverter,
  A4 : TypeConverter, A5 : TypeConverter, A6 : TypeConverter,
  A7 : TypeConverter](f: (A0, A1, A2, A3, A4, A5, A6, A7) => B) = {
    implicit val ft = new FunctionBasedRowReader8(f)
    new CassandraRDD[B](sc, connector, keyspaceName, tableName, columnNames, where, readConf)
  }

  def as[B : ClassTag, A0 : TypeConverter, A1 : TypeConverter, A2 : TypeConverter, A3 : TypeConverter,
  A4 : TypeConverter, A5 : TypeConverter, A6 : TypeConverter, A7: TypeConverter,
  A8 : TypeConverter](f: (A0, A1, A2, A3, A4, A5, A6, A7, A8) => B) = {
    implicit val ft = new FunctionBasedRowReader9(f)
    new CassandraRDD[B](sc, connector, keyspaceName, tableName, columnNames, where, readConf)
  }

  def as[B : ClassTag, A0 : TypeConverter, A1 : TypeConverter, A2 : TypeConverter, A3 : TypeConverter,
  A4 : TypeConverter, A5 : TypeConverter, A6 : TypeConverter, A7: TypeConverter,
  A8 : TypeConverter, A9 : TypeConverter](f: (A0, A1, A2, A3, A4, A5, A6, A7, A8, A9) => B) = {
    implicit val ft = new FunctionBasedRowReader10(f)
    new CassandraRDD[B](sc, connector, keyspaceName, tableName, columnNames, where, readConf)
  }

  def as[B : ClassTag, A0 : TypeConverter, A1 : TypeConverter, A2 : TypeConverter, A3 : TypeConverter,
  A4 : TypeConverter, A5 : TypeConverter, A6 : TypeConverter, A7: TypeConverter, A8: TypeConverter,
  A9 : TypeConverter, A10 : TypeConverter](f: (A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10) => B) = {
    implicit val ft = new FunctionBasedRowReader11(f)
    new CassandraRDD[B](sc, connector, keyspaceName, tableName, columnNames, where, readConf)
  }

  def as[B : ClassTag, A0 : TypeConverter, A1 : TypeConverter, A2 : TypeConverter, A3 : TypeConverter,
  A4 : TypeConverter, A5 : TypeConverter, A6 : TypeConverter, A7: TypeConverter, A8: TypeConverter,
  A9 : TypeConverter, A10: TypeConverter, A11 : TypeConverter](
  f: (A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11) => B) = {
    implicit val ft = new FunctionBasedRowReader12(f)
    new CassandraRDD[B](sc, connector, keyspaceName, tableName, columnNames, where, readConf)
  }

  // ===================================================================
  // no references to sc allowed below this line
  // ===================================================================

  // Lazy fields allow to recover non-serializable objects after deserialization of RDD
  // Cassandra connection is not serializable, but will be recreated on-demand basing on serialized host and port.

  // Table metadata should be fetched only once to guarantee all tasks use the same metadata.
  // TableDef is serializable, so there is no problem:
  lazy val tableDef = {
    Schema.fromCassandra(connector, Some(keyspaceName), Some(tableName)).tables.headOption match {
      case Some(t) => t
      case None => throw new IOException(s"Table not found: $keyspaceName.$tableName")
    }
  }

  private lazy val rowTransformer = implicitly[RowReaderFactory[R]].rowReader(tableDef)

  private def checkColumnsExistence(columns: Seq[IndexedByNameColumnRef]): Seq[IndexedByNameColumnRef] = {
    val allColumnNames = tableDef.allColumns.map(_.columnName).toSet
    val regularColumnNames = tableDef.regularColumns.map(_.columnName).toSet

    def checkSingleColumn(column: IndexedByNameColumnRef) = {
      if (!allColumnNames.contains(column.name))
        throw new IOException(s"Column $column not found in table $keyspaceName.$tableName")

      column match {
        case NamedColumnRef(_) =>

        case TTL(columnName) =>
          if (!regularColumnNames.contains(columnName))
            throw new IOException(s"TTL can be obtained only for regular columns, " +
              s"but column $columnName is not a regular column in table $keyspaceName.$tableName.")

        case WriteTime(columnName) =>
          if (!regularColumnNames.contains(columnName))
            throw new IOException(s"TTL can be obtained only for regular columns, " +
              s"but column $columnName is not a regular column in table $keyspaceName.$tableName.")
      }

      column
    }

    columns.map(checkSingleColumn)
  }

  lazy val selectedColumnNames: Seq[String] = {
    selectedColumnRefs.map(_.name)
  }

  /** Returns the names of columns to be selected from the table.*/
  lazy val selectedColumnRefs: Seq[IndexedByNameColumnRef] = {
    val providedColumnNames =
      columnNames match {
        case AllColumns => tableDef.allColumns.map(col => col.columnName: IndexedByNameColumnRef).toSeq
        case SomeColumns(cs) => checkColumnsExistence(cs)
      }

    (rowTransformer.columnNames, rowTransformer.requiredColumns) match {
      case (Some(cs), None) => providedColumnNames.filter(columnName => cs.toSet(columnName.selectedAs))
      case (_, _) => providedColumnNames
    }
  }


  /** Checks for existence of keyspace, table, columns and whether the number of selected columns corresponds to
    * the number of the columns expected by the target type constructor.
    * If successful, does nothing, otherwise throws appropriate `IOException` or `AssertionError`.*/
  lazy val verify = {
    val targetType = implicitly[ClassTag[R]]

    tableDef.allColumns  // will throw IOException if table does not exist

    rowTransformer.columnNames match {
      case Some(names) =>
        val missingColumns = names.toSet -- selectedColumnRefs.map(_.selectedAs).toSet
        assert(missingColumns.isEmpty, s"Missing columns needed by $targetType: ${missingColumns.mkString(", ")}")
      case None =>
    }

    rowTransformer.requiredColumns match {
      case Some(count) =>
        assert(selectedColumnRefs.size >= count,
        s"Not enough columns selected for the target row type $targetType: ${selectedColumnRefs.size} < $count")
      case None =>
    }
  }

  private lazy val cassandraPartitionerClassName =
    connector.withSessionDo {
      session =>
        session.execute("SELECT partitioner FROM system.local").one().getString(0)
    }

  private def quote(name: String) = "\"" + name + "\""

  override def getPartitions: Array[Partition] = {
    verify // let's fail fast
    val tf = TokenFactory.forCassandraPartitioner(cassandraPartitionerClassName)
    val partitions = new CassandraRDDPartitioner(connector, tableDef, splitSize)(tf).partitions(where)
    logDebug(s"Created total ${partitions.size} partitions for $keyspaceName.$tableName.")
    logTrace("Partitions: \n" + partitions.mkString("\n"))
    partitions
  }

  override def getPreferredLocations(split: Partition) =
    split.asInstanceOf[CassandraPartition]
    .endpoints.flatMap(inet => Seq(inet.getHostName, inet.getHostAddress)).toSeq.distinct

  private def tokenRangeToCqlQuery(range: CqlTokenRange): (String, Seq[Any]) = {
    val columns = selectedColumnRefs.map(_.cql).mkString(", ")
    val filter = (range.cql +: where.predicates ).filter(_.nonEmpty).mkString(" AND ") + " ALLOW FILTERING"
    val quotedKeyspaceName = quote(keyspaceName)
    val quotedTableName = quote(tableName)
    (s"SELECT $columns FROM $quotedKeyspaceName.$quotedTableName WHERE $filter", range.values ++ where.values)
  }

  private def createStatement(session: Session, cql: String, values: Any*): Statement = {
    try {
      val stmt = session.prepare(cql)
      stmt.setConsistencyLevel(consistencyLevel)
      val converters = stmt.getVariables
        .view
        .map(_.getType)
        .map(ColumnType.fromDriverType)
        .map(_.converterToCassandra)
        .toArray
      val convertedValues =
        for ((value, converter) <- values zip converters)
        yield converter.convert(value).asInstanceOf[AnyRef]
      val bstm = stmt.bind(convertedValues: _*)
      bstm.setFetchSize(fetchSize)
      bstm
    }
    catch {
      case t: Throwable =>
        throw new IOException(s"Exception during preparation of $cql: ${t.getMessage}", t)
    }
  }

  private def fetchTokenRange(session: Session, range: CqlTokenRange): Iterator[R] = {
    val (cql, values) = tokenRangeToCqlQuery(range)
    logDebug(s"Fetching data for range ${range.cql} with $cql with params ${values.mkString("[", ",", "]")}")
    val stmt = createStatement(session, cql, values: _*)
    val columnNamesArray = selectedColumnRefs.map(_.selectedAs).toArray
    try {
      val rs = session.execute(stmt)
      val protocolVersion = session.getCluster.getConfiguration.getProtocolOptions.getProtocolVersionEnum
      val iterator = new PrefetchingResultSetIterator(rs, fetchSize)
      val result = iterator.map(rowTransformer.read(_, columnNamesArray, protocolVersion))
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
    val startTime = System.currentTimeMillis()

    // Iterator flatMap trick flattens the iterator-of-iterator structure into a single iterator.
    // flatMap on iterator is lazy, therefore a query for the next token range is executed not earlier
    // than all of the rows returned by the previous query have been consumed
    val rowIterator = tokenRanges.iterator.flatMap(fetchTokenRange(session, _))
    val countingIterator = new CountingIterator(rowIterator)

    context.addTaskCompletionListener { (context) =>
      val endTime = System.currentTimeMillis()
      val duration = (endTime - startTime) / 1000.0
      logDebug(f"Fetched ${countingIterator.count} rows from $keyspaceName.$tableName for partition ${partition.index} in $duration%.3f s.")
      session.close()
    }
    countingIterator
  }

}

object CassandraRDD {
  def apply[T](sc: SparkContext, keyspaceName: String, tableName: String)
              (implicit ct: ClassTag[T], rrf: RowReaderFactory[T]): CassandraRDD[T] =

    new CassandraRDD[T](
      sc,
      CassandraConnector(sc.getConf),
      keyspaceName,
      tableName,
      AllColumns,
      CqlWhereClause.empty,
      ReadConf.fromSparkConf(sc.getConf)
    )

  def apply[K, V](sc: SparkContext, keyspaceName: String, tableName: String)
                 (implicit keyCT: ClassTag[K], valueCT: ClassTag[V], rrf: RowReaderFactory[(K, V)]): CassandraRDD[(K, V)] =

    new CassandraRDD[(K, V)](
      sc,
      CassandraConnector(sc.getConf),
      keyspaceName,
      tableName,
      AllColumns,
      CqlWhereClause.empty,
      ReadConf.fromSparkConf(sc.getConf)
    )
}
