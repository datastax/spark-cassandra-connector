package com.datastax.spark.connector.rdd

import java.io.IOException

import com.datastax.driver.core.{ConsistencyLevel, PreparedStatement, Session, SimpleStatement, Statement}
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.rdd.partitioner.{CassandraRDDPartitioner, CassandraPartition, CqlTokenRange}
import com.datastax.spark.connector.rdd.partitioner.dht.TokenFactory
import com.datastax.spark.connector.rdd.reader._
import com.datastax.spark.connector.types.TypeConverter
import com.datastax.spark.connector.util.CountingIterator

import org.apache.spark.rdd.RDD
import org.apache.spark.{Logging, Partition, SparkContext, TaskContext}

import scala.collection.JavaConversions._
import scala.language.existentials
import scala.reflect._

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
  *   - cassandra.input.split.size:        approx number of rows in a Spark partition, default 100000
  *   - cassandra.input.page.row.size:     number of rows fetched per roundtrip, default 1000
  *
  * A `CassandraRDD` object gets serialized and sent to every Spark executor.
  * Reads are performed at ConsistencyLevel.ONE in order to leverage data-locality and minimize network traffic.
  * If a Cassandra node fails or gets overloaded during read, queries are retried to a different node.
  */
class CassandraRDD[R] private[connector] (
    @transient sc: SparkContext,
    val keyspaceName: String,
    val tableName: String,
    val columnNames: ColumnSelector = AllColumns,
    val where: CqlWhereClause = CqlWhereClause.empty)(
  implicit
    ct : ClassTag[R], @transient rtf: RowReaderFactory[R])
  extends RDD[R](sc, Seq.empty) with Logging {

  /** How many rows are fetched at once from server */
  val fetchSize = sc.getConf.getInt("cassandra.input.page.row.size", 1000)

  /** How many rows to fetch in a single Spark Task. */
  val splitSize = sc.getConf.getInt("cassandra.input.split.size", 100000)

  private val connector = CassandraConnector(sc.getConf)

  private def copy(columnNames: ColumnSelector = columnNames, where: CqlWhereClause = where): CassandraRDD[R] =
    new CassandraRDD(sc, keyspaceName, tableName, columnNames, where)

  /** Adds a CQL `WHERE` predicate(s) to the query.
    * Useful for leveraging secondary indexes in Cassandra.
    * Implicitly adds an `ALLOW FILTERING` clause to the WHERE clause, however beware that some predicates
    * might be rejected by Cassandra, particularly in cases when they filter on an unindexed, non-clustering column.*/
  def where(cql: String, values: Any*): CassandraRDD[R] = {
    copy(where = where and CqlWhereClause(Seq(cql), values))
  }

  /** Throws IllegalArgumentException if columns sequence contains unavailable columns */
  private def checkColumnsAvailable(columns: Seq[String], availableColumns: Seq[String]) {
    val availableColumnsSet = availableColumns.toSet
    val notFound = columns.find(c => !availableColumnsSet.contains(c))
    if (notFound.isDefined)
      throw new IllegalArgumentException(
        s"Column not found in selection: ${notFound.get}. " +
          s"Available columns: [${availableColumns.mkString(",")}].")
  }

  /** Filters currently selected set of columns with a new set of columns */
  private def narrowColumnSelection(columns: Seq[String]): Seq[String] = {
    columnNames match {
      case SomeColumns(cs @ _*) =>
        checkColumnsAvailable(columns, cs)
        cs.filter(columns.toSet.contains)
      case AllColumns =>
        // we do not check for column existence yet as it would require fetching schema and a call to C*
        // columns existence will be checked by C* once the RDD gets computed.
        columns
    }
  }

  /** Narrows down the selected set of columns.
    * Use this for better performance, when you don't need all the columns in the result RDD.
    * When called multiple times, it selects the subset of the already selected columns, so
    * after a column was removed by the previous `select` call, it is not possible to
    * add it back.  */
  def select(columns: String*): CassandraRDD[R] = {
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
    new CassandraRDD[B](sc, keyspaceName, tableName, columnNames)
  }

  def as[B : ClassTag, A0 : TypeConverter, A1 : TypeConverter](f: (A0, A1) => B) = {
    implicit val ft = new FunctionBasedRowReader2(f)
    new CassandraRDD[B](sc, keyspaceName, tableName, columnNames)
  }

  def as[B : ClassTag, A0 : TypeConverter, A1 : TypeConverter, A2 : TypeConverter](f: (A0, A1, A2) => B) = {
    implicit val ft = new FunctionBasedRowReader3(f)
    new CassandraRDD[B](sc, keyspaceName, tableName, columnNames)
  }

  def as[B : ClassTag, A0 : TypeConverter, A1 : TypeConverter, A2 : TypeConverter,
  A3 : TypeConverter](f: (A0, A1, A2, A3) => B) = {
    implicit val ft = new FunctionBasedRowReader4(f)
    new CassandraRDD[B](sc, keyspaceName, tableName, columnNames)
  }

  def as[B : ClassTag, A0 : TypeConverter, A1 : TypeConverter, A2 : TypeConverter, A3 : TypeConverter,
  A4 : TypeConverter](f: (A0, A1, A2, A3, A4) => B) = {
    implicit val ft = new FunctionBasedRowReader5(f)
    new CassandraRDD[B](sc, keyspaceName, tableName, columnNames)
  }

  def as[B : ClassTag, A0 : TypeConverter, A1 : TypeConverter, A2 : TypeConverter, A3 : TypeConverter,
  A4 : TypeConverter, A5 : TypeConverter](f: (A0, A1, A2, A3, A4, A5) => B) = {
    implicit val ft = new FunctionBasedRowReader6(f)
    new CassandraRDD[B](sc, keyspaceName, tableName, columnNames)
  }

  def as[B : ClassTag, A0 : TypeConverter, A1 : TypeConverter, A2 : TypeConverter, A3 : TypeConverter,
  A4 : TypeConverter, A5 : TypeConverter, A6 : TypeConverter](f: (A0, A1, A2, A3, A4, A5, A6) => B) = {
    implicit val ft = new FunctionBasedRowReader7(f)
    new CassandraRDD[B](sc, keyspaceName, tableName, columnNames)
  }

  def as[B : ClassTag, A0 : TypeConverter, A1 : TypeConverter, A2 : TypeConverter, A3 : TypeConverter,
  A4 : TypeConverter, A5 : TypeConverter, A6 : TypeConverter,
  A7 : TypeConverter](f: (A0, A1, A2, A3, A4, A5, A6, A7) => B) = {
    implicit val ft = new FunctionBasedRowReader8(f)
    new CassandraRDD[B](sc, keyspaceName, tableName, columnNames)
  }

  def as[B : ClassTag, A0 : TypeConverter, A1 : TypeConverter, A2 : TypeConverter, A3 : TypeConverter,
  A4 : TypeConverter, A5 : TypeConverter, A6 : TypeConverter, A7: TypeConverter,
  A8 : TypeConverter](f: (A0, A1, A2, A3, A4, A5, A6, A7, A8) => B) = {
    implicit val ft = new FunctionBasedRowReader9(f)
    new CassandraRDD[B](sc, keyspaceName, tableName, columnNames)
  }

  def as[B : ClassTag, A0 : TypeConverter, A1 : TypeConverter, A2 : TypeConverter, A3 : TypeConverter,
  A4 : TypeConverter, A5 : TypeConverter, A6 : TypeConverter, A7: TypeConverter,
  A8 : TypeConverter, A9 : TypeConverter](f: (A0, A1, A2, A3, A4, A5, A6, A7, A8, A9) => B) = {
    implicit val ft = new FunctionBasedRowReader10(f)
    new CassandraRDD[B](sc, keyspaceName, tableName, columnNames)
  }

  def as[B : ClassTag, A0 : TypeConverter, A1 : TypeConverter, A2 : TypeConverter, A3 : TypeConverter,
  A4 : TypeConverter, A5 : TypeConverter, A6 : TypeConverter, A7: TypeConverter, A8: TypeConverter,
  A9 : TypeConverter, A10 : TypeConverter](f: (A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10) => B) = {
    implicit val ft = new FunctionBasedRowReader11(f)
    new CassandraRDD[B](sc, keyspaceName, tableName, columnNames)
  }

  def as[B : ClassTag, A0 : TypeConverter, A1 : TypeConverter, A2 : TypeConverter, A3 : TypeConverter,
  A4 : TypeConverter, A5 : TypeConverter, A6 : TypeConverter, A7: TypeConverter, A8: TypeConverter,
  A9 : TypeConverter, A10: TypeConverter, A11 : TypeConverter](
  f: (A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11) => B) = {
    implicit val ft = new FunctionBasedRowReader12(f)
    new CassandraRDD[B](sc, keyspaceName, tableName, columnNames)
  }

  // ===================================================================
  // no references to sc allowed below this line
  // ===================================================================

  // Lazy fields allow to recover non-serializable objects after deserialization of RDD
  // Cassandra connection is not serializable, but will be recreated on-demand basing on serialized host and port.

  // Table metadata should be fetched only once to guarantee all tasks use the same metadata.
  // TableDef is serializable, so there is no problem:
  private lazy val tableDef = {
    new Schema(connector, Some(keyspaceName), Some(tableName)).tables.headOption match {
      case Some(t) => t
      case None => throw new IOException(s"Table not found: $keyspaceName.$tableName")
    }
  }

  private val rowTransformer = implicitly[RowReaderFactory[R]].rowReader(tableDef)

  private def checkColumnsExistence(columnNames: Seq[String]): Seq[String] = {
    val allColumnNames = tableDef.allColumns.map(_.columnName).toSet
    def columnNotFound(c: String) = !allColumnNames.contains(c)
    columnNames.find(columnNotFound) match {
      case Some(c) => throw new IOException(s"Column $c not found in table $keyspaceName.$tableName")
      case None => columnNames
    }
  }


  /** Returns the names of columns to be selected from the table.*/
  lazy val selectedColumnNames: Seq[String] = {
    val providedColumnNames =
      columnNames match {
        case AllColumns => tableDef.allColumns.map(_.columnName).toSeq
        case SomeColumns(cs @ _*) => checkColumnsExistence(cs)
      }

    (rowTransformer.columnNames, rowTransformer.columnCount) match {
      case (Some(cs), None) => providedColumnNames.filter(cs.toSet)
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
        val missingColumns = names.toSet -- selectedColumnNames.toSet
        assert(missingColumns.isEmpty, s"Missing columns needed by $targetType: ${missingColumns.mkString(", ")}")
      case None =>
    }

    rowTransformer.columnCount match {
      case Some(count) =>
        assert(selectedColumnNames.size >= count,
        s"Not enough columns selected for the target row type $targetType: ${selectedColumnNames.size} < $count")
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
    val partitions = new CassandraRDDPartitioner(connector, tableDef, splitSize)(tf).partitions
    logInfo(s"Created total ${partitions.size} partitions for $keyspaceName.$tableName.")
    logDebug("Partitions: \n" + partitions.mkString("\n"))
    partitions
  }

  private def tokenRangeToCqlQuery(range: CqlTokenRange): (String, Seq[Any]) = {
    val columns = selectedColumnNames.map(quote).mkString(", ")
    val filter = range.cql + where.predicates.fold("")(_ + " AND " + _) + " ALLOW FILTERING"
    val quotedKeyspaceName = quote(keyspaceName)
    val quotedTableName = quote(tableName)
    (s"SELECT $columns FROM $quotedKeyspaceName.$quotedTableName WHERE $filter", where.values)
  }

  private def createStatement(session: Session, cql: String, values: Any*): Statement = {
    val stmt = session.prepare(cql)
    stmt.setConsistencyLevel(ConsistencyLevel.LOCAL_ONE)
    val bstm = stmt.bind(values.map(_.asInstanceOf[AnyRef]): _*)
    bstm.setFetchSize(fetchSize)
    bstm
  }

  private def fetchTokenRange(session: Session, range: CqlTokenRange): Iterator[R] = {
    val (cql, values) = tokenRangeToCqlQuery(range)
    logInfo(s"Fetching data for range ${range.cql} with $cql with params ${values.mkString("[", ",", "]")}")
    val stmt = createStatement(session, cql, values: _*)
    val columnNamesArray = selectedColumnNames.toArray
    try {
      val result = session.execute(stmt).iterator.map(rowTransformer.read(_, columnNamesArray))
      logInfo(s"Row iterator for range ${range.cql} obtained successfully.")
      result
    } catch {
      case t: Throwable => throw new IOException("Exception during query execution: " + cql, t)
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

    context.addOnCompleteCallback { () =>
      val endTime = System.currentTimeMillis()
      val duration = (endTime - startTime) / 1000.0
      logInfo(f"Fetched ${countingIterator.count} rows from $keyspaceName.$tableName for partition ${partition.index} in $duration%.3f s.")
      session.close()
    }
    countingIterator
  }
}
