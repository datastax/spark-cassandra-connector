package com.datastax.driver.spark.rdd

import java.io.IOException

import com.datastax.driver.core.{ConsistencyLevel, Session, SimpleStatement, Statement}
import com.datastax.driver.spark.connector._
import com.datastax.driver.spark.mapper._
import com.datastax.driver.spark.rdd.dht.TokenFactory
import com.datastax.driver.spark.types.TypeConverter

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.collection.JavaConversions._
import scala.language.existentials
import scala.reflect._

/** RDD representing a Cassandra table.
  * Options should be passed in SparkContext configuration object:
  *   - cassandra.connection.host:         contact point to connect to the Cassandra cluster, defaults to spark master host
  *   - cassandra.connection.rpc.port:     Cassandra thrift port, defaults to 9160
  *   - cassandra.connection.native.port:  Cassandra native port, defaults to 9042
  *   - cassandra.input.split.size:        approx number of rows to be fetched per Spark partition, default 100000
  *   - cassandra.input.page.row.size:     number of rows fetched per roundtrip
  */
class CassandraRDD[R] (
    @transient sc: SparkContext,
    val keyspaceName: String,
    val tableName: String,
    val columnNames: ColumnSelector = AllColumns,
    val where: CqlWhereClause = CqlWhereClause.empty)(
  implicit
    ct : ClassTag[R], @transient rtf: RowTransformerFactory[R])
  extends RDD[R](sc, Seq.empty) {

  /** How many rows are fetched at once from server */
  val fetchSize = sc.getConf.getInt("cassandra.input.page.row.size", 1000)

  /** How many rows to fetch in a single Spark Task */
  val splitSize = sc.getConf.getInt("cassandra.input.split.size", 100000)

  val connector = CassandraConnector(sc.getConf)

  private def copy(columnNames: ColumnSelector = columnNames, where: CqlWhereClause = where): CassandraRDD[R] =
    new CassandraRDD(sc, keyspaceName, tableName, columnNames, where)

  /** Adds a CQL WHERE predicate(s) to the query. Useful for leveraging secondary indexes in Cassandra. */
  def where(cql: String, values: Any*): CassandraRDD[R] = {
    copy(where = where + CqlWhereClause(Seq(cql), values))
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

  /** Narrows down the selected set of columns */
  def select(columns: String*): CassandraRDD[R] = {
    val columnsToSelect = narrowColumnSelection(columns)
    copy(columnNames = SomeColumns(columns: _*))
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
    implicit val ft = new FunctionBasedRowTransformer1(f)
    new CassandraRDD[B](sc, keyspaceName, tableName, columnNames)
  }

  def as[B : ClassTag, A0 : TypeConverter, A1 : TypeConverter](f: (A0, A1) => B) = {
    implicit val ft = new FunctionBasedRowTransformer2(f)
    new CassandraRDD[B](sc, keyspaceName, tableName, columnNames)
  }

  def as[B : ClassTag, A0 : TypeConverter, A1 : TypeConverter, A2 : TypeConverter](f: (A0, A1, A2) => B) = {
    implicit val ft = new FunctionBasedRowTransformer3(f)
    new CassandraRDD[B](sc, keyspaceName, tableName, columnNames)
  }

  def as[B : ClassTag, A0 : TypeConverter, A1 : TypeConverter, A2 : TypeConverter,
  A3 : TypeConverter](f: (A0, A1, A2, A3) => B) = {
    implicit val ft = new FunctionBasedRowTransformer4(f)
    new CassandraRDD[B](sc, keyspaceName, tableName, columnNames)
  }

  def as[B : ClassTag, A0 : TypeConverter, A1 : TypeConverter, A2 : TypeConverter, A3 : TypeConverter,
  A4 : TypeConverter](f: (A0, A1, A2, A3, A4) => B) = {
    implicit val ft = new FunctionBasedRowTransformer5(f)
    new CassandraRDD[B](sc, keyspaceName, tableName, columnNames)
  }

  def as[B : ClassTag, A0 : TypeConverter, A1 : TypeConverter, A2 : TypeConverter, A3 : TypeConverter,
  A4 : TypeConverter, A5 : TypeConverter](f: (A0, A1, A2, A3, A4, A5) => B) = {
    implicit val ft = new FunctionBasedRowTransformer6(f)
    new CassandraRDD[B](sc, keyspaceName, tableName, columnNames)
  }

  def as[B : ClassTag, A0 : TypeConverter, A1 : TypeConverter, A2 : TypeConverter, A3 : TypeConverter,
  A4 : TypeConverter, A5 : TypeConverter, A6 : TypeConverter](f: (A0, A1, A2, A3, A4, A5, A6) => B) = {
    implicit val ft = new FunctionBasedRowTransformer7(f)
    new CassandraRDD[B](sc, keyspaceName, tableName, columnNames)
  }

  def as[B : ClassTag, A0 : TypeConverter, A1 : TypeConverter, A2 : TypeConverter, A3 : TypeConverter,
  A4 : TypeConverter, A5 : TypeConverter, A6 : TypeConverter,
  A7 : TypeConverter](f: (A0, A1, A2, A3, A4, A5, A6, A7) => B) = {
    implicit val ft = new FunctionBasedRowTransformer8(f)
    new CassandraRDD[B](sc, keyspaceName, tableName, columnNames)
  }

  def as[B : ClassTag, A0 : TypeConverter, A1 : TypeConverter, A2 : TypeConverter, A3 : TypeConverter,
  A4 : TypeConverter, A5 : TypeConverter, A6 : TypeConverter, A7: TypeConverter,
  A8 : TypeConverter](f: (A0, A1, A2, A3, A4, A5, A6, A7, A8) => B) = {
    implicit val ft = new FunctionBasedRowTransformer9(f)
    new CassandraRDD[B](sc, keyspaceName, tableName, columnNames)
  }

  def as[B : ClassTag, A0 : TypeConverter, A1 : TypeConverter, A2 : TypeConverter, A3 : TypeConverter,
  A4 : TypeConverter, A5 : TypeConverter, A6 : TypeConverter, A7: TypeConverter,
  A8 : TypeConverter, A9 : TypeConverter](f: (A0, A1, A2, A3, A4, A5, A6, A7, A8, A9) => B) = {
    implicit val ft = new FunctionBasedRowTransformer10(f)
    new CassandraRDD[B](sc, keyspaceName, tableName, columnNames)
  }

  def as[B : ClassTag, A0 : TypeConverter, A1 : TypeConverter, A2 : TypeConverter, A3 : TypeConverter,
  A4 : TypeConverter, A5 : TypeConverter, A6 : TypeConverter, A7: TypeConverter, A8: TypeConverter,
  A9 : TypeConverter, A10 : TypeConverter](f: (A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10) => B) = {
    implicit val ft = new FunctionBasedRowTransformer11(f)
    new CassandraRDD[B](sc, keyspaceName, tableName, columnNames)
  }

  def as[B : ClassTag, A0 : TypeConverter, A1 : TypeConverter, A2 : TypeConverter, A3 : TypeConverter,
  A4 : TypeConverter, A5 : TypeConverter, A6 : TypeConverter, A7: TypeConverter, A8: TypeConverter,
  A9 : TypeConverter, A10: TypeConverter, A11 : TypeConverter](
  f: (A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11) => B) = {
    implicit val ft = new FunctionBasedRowTransformer12(f)
    new CassandraRDD[B](sc, keyspaceName, tableName, columnNames)
  }

  // ===================================================================
  // no references to sc allowed below this line
  // ===================================================================

  // Lazy fields allow to recover non-serializable objects after deserialization of RDD
  // Cassandra connection is not serializable, but will be recreated on-demand basing on serialized host and port.

  // Table metadata should be fetched only once to guarantee all tasks use the same metadata.
  // TableDef is serializable, so there is no problem:
  lazy val tableDef = {
    new Schema(connector, Some(keyspaceName), Some(tableName)).tables.headOption match {
      case Some(t) => t
      case None => throw new IOException(s"Table not found: $keyspaceName.$tableName")
    }
  }

  lazy val selectedColumnNames = {
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

  def checkColumnsExistence(columnNames: Seq[String]): Seq[String] = {
    val allColumnNames = tableDef.allColumns.map(_.columnName).toSet
    def columnNotFound(c: String) = !allColumnNames.contains(c)
    columnNames.find(columnNotFound) match {
      case Some(c) => throw new IOException(s"Column $c not found in table $keyspaceName.$tableName")
      case None => columnNames
    }
  }

  val rowTransformer = implicitly[RowTransformerFactory[R]].rowTransformer(tableDef)

  /** Checks for existence of keyspace, table, columns and whether the number of selected columns corresponds to
    * the number of the columns expected by the target type constructor.
    * If successful, does nothing, otherwise throws appropriate IOException or AssertionError.*/
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

  lazy val cassandraPartitionerClassName =
    connector.withSessionDo {
      session =>
        session.execute("SELECT partitioner FROM system.local").one().getString(0)
    }

  private def quote(name: String) = "\"" + name + "\""

  override def getPartitions: Array[Partition] = {
    verify // let's fail fast
    val tf = TokenFactory.forCassandraPartitioner(cassandraPartitionerClassName)
    new CassandraRDDPartitioner(connector, tableDef, splitSize)(tf).partitions
  }

  private def tokenRangeToCqlQuery(range: CqlTokenRange): (String, Seq[Any]) = {
    val columns = selectedColumnNames.map(quote).mkString(", ")
    val filter = range.cql + where.predicates.fold("")(_ + " AND " + _) + " ALLOW FILTERING"
    val quotedKeyspaceName = quote(keyspaceName)
    val quotedTableName = quote(tableName)
    (s"SELECT $columns FROM $quotedKeyspaceName.$quotedTableName WHERE $filter", where.values)
  }

  private def createStatement(cql: String, values: Any*): Statement = {
    val stmt = new SimpleStatement(cql, values.map(_.asInstanceOf[AnyRef]): _*)
    stmt.setConsistencyLevel(ConsistencyLevel.LOCAL_ONE)
    stmt.setFetchSize(fetchSize)
    stmt
  }

  private def fetchTokenRange(session: Session, range: CqlTokenRange): Iterator[R] = {
    val (cql, values) = tokenRangeToCqlQuery(range)
    val stmt = createStatement(cql, values: _*)
    val columnNamesArray = selectedColumnNames.toArray
    try {
      session.execute(stmt).iterator.map(rowTransformer.transform(_, columnNamesArray))
    } catch {
      case t: Throwable => throw new IOException("Exception during query execution: " + cql, t)
    }
  }

  override def compute(split: Partition, context: TaskContext): Iterator[R] = {
    val session = connector.openSession()
    context.addOnCompleteCallback(session.close)

    val partition = split.asInstanceOf[CassandraPartition]
    val tokenRanges = partition.tokenRanges

    // Iterator flatMap trick flattens the iterator-of-iterator structure into a single iterator.
    // flatMap on iterator is lazy, therefore a query for the next token range is executed not earlier
    // than all of the rows returned by the previous query have been consumed
    tokenRanges.iterator.flatMap(fetchTokenRange(session, _))
  }
}
