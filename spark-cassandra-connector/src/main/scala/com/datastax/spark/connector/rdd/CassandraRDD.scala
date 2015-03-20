package com.datastax.spark.connector.rdd

import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.rdd.ClusteringOrder.{Ascending, Descending}
import com.datastax.spark.connector.rdd.reader._
import com.datastax.spark.connector.types.TypeConverter
import com.datastax.spark.connector.util.ConfigCheck
import com.datastax.spark.connector.{ColumnSelector, NamedColumnRef, SomeColumns, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Dependency, SparkContext}

import scala.language.existentials
import scala.reflect.ClassTag

abstract class CassandraRDD[R](_sc: SparkContext, dep: Seq[Dependency[_]])(implicit ct: ClassTag[R]) extends RDD[R](_sc, dep) {

  //Check for valid Spark configuration
  ConfigCheck.checkConfig(_sc.getConf)

  protected def keyspaceName: String

  protected def tableName: String

  protected def columnNames: ColumnSelector

  protected def where: CqlWhereClause

  protected def readConf: ReadConf

  protected def limit: Option[Long]

  require(limit.isEmpty || limit.get > 0, "Limit must be greater than 0")

  protected def clusteringOrder: Option[ClusteringOrder]

  protected def connector: CassandraConnector

  def toEmptyCassandraRDD: EmptyCassandraRDD[R]

  /** Allows to set custom read configuration, e.g. consistency level or fetch size. */
  def withReadConf(readConf: ReadConf) = {
    copy(readConf = readConf)
  }

  protected def copy(columnNames: ColumnSelector = columnNames,
                     where: CqlWhereClause = where,
                     limit: Option[Long] = limit,
                     clusteringOrder: Option[ClusteringOrder] = None,
                     readConf: ReadConf = readConf,
                     connector: CassandraConnector = connector): CassandraRDD[R]

  /** Returns a copy of this Cassandra RDD with specified connector */
  def withConnector(connector: CassandraConnector): CassandraRDD[R] = {
    copy(connector = connector)
  }

  /** Adds a CQL `WHERE` predicate(s) to the query.
    * Useful for leveraging secondary indexes in Cassandra.
    * Implicitly adds an `ALLOW FILTERING` clause to the WHERE clause, however beware that some predicates
    * might be rejected by Cassandra, particularly in cases when they filter on an unindexed, non-clustering column. */
  def where(cql: String, values: Any*): CassandraRDD[R] = {
    copy(where = where and CqlWhereClause(Seq(cql), values))
  }

  /** Narrows down the selected set of columns.
    * Use this for better performance, when you don't need all the columns in the result RDD.
    * When called multiple times, it selects the subset of the already selected columns, so
    * after a column was removed by the previous `select` call, it is not possible to
    * add it back.
    *
    * The selected columns are [[NamedColumnRef]] instances. This type allows to specify columns for
    * straightforward retrieval and to read TTL or write time of regular columns as well. Implicit
    * conversions included in [[com.datastax.spark.connector]] package make it possible to provide
    * just column names (which is also backward compatible) and optional add `.ttl` or `.writeTime`
    * suffix in order to create an appropriate [[NamedColumnRef]] instance.
    */
  def select(columns: SelectableColumnRef*): CassandraRDD[R] = {
    copy(columnNames = SomeColumns(narrowColumnSelection(columns): _*))
  }

  /** Adds the limit clause to CQL select statement. The limit will be applied for each created
    * Spark partition. In other words, unless the data are fetched from a single Cassandra partition
    * the number of results is unpredictable.
    *
    * The main purpose of passing limit clause is to fetch top n rows from a single Cassandra
    * partition when the table is designed so that it uses clustering keys and a partition key
    * predicate is passed to the where clause. */
  def limit(rowLimit: Long): CassandraRDD[R] = {
    copy(limit = Some(rowLimit))
  }

  /** Adds a CQL `ORDER BY` clause to the query.
    * It can be applied only in case there are clustering columns and primary key predicate is
    * pushed down in `where`.
    * It is useful when the default direction of ordering rows within a single Cassandra partition
    * needs to be changed. */
  def clusteringOrder(order: ClusteringOrder): CassandraRDD[R] = {
    copy(clusteringOrder = Some(order))
  }

  def withAscOrder: CassandraRDD[R] = clusteringOrder(Ascending)

  def withDescOrder: CassandraRDD[R] = clusteringOrder(Descending)

  override def take(num: Int): Array[R] = {
    limit match {
      case Some(_) => super.take(num)
      case None => limit(num).take(num)
    }
  }

  protected def narrowColumnSelection(columns: Seq[SelectableColumnRef]): Seq[SelectableColumnRef]

  // Needed to be public for JavaAPI
  val selectedColumnRefs: Seq[SelectableColumnRef]

  //convertTo must be implmented for classes which wish to support `.as`
  protected def convertTo[B](implicit ct: ClassTag[B], rrf: RowReaderFactory[B]): CassandraRDD[B] =
    throw new NotImplementedError(s"convertTo not implemented for this class")

  /** Maps each row into object of a different type using provided function taking column value(s) as argument(s).
    * Can be used to convert each row to a tuple or a case class object:
    * {{{
    * sc.cassandraTable("ks", "table").select("column1").as((s: String) => s)                 // yields CassandraRDD[String]
    * sc.cassandraTable("ks", "table").select("column1", "column2").as((_: String, _: Long))  // yields CassandraRDD[(String, Long)]
    *
    * case class MyRow(key: String, value: Long)
    * sc.cassandraTable("ks", "table").select("column1", "column2").as(MyRow)                 // yields CassandraRDD[MyRow]
    * }}} */
  def as[B: ClassTag, A0: TypeConverter](f: A0 => B): CassandraRDD[B] = {
    implicit val ft = new FunctionBasedRowReader1(f)
    convertTo[B]
  }

  def as[B: ClassTag, A0: TypeConverter, A1: TypeConverter](f: (A0, A1) => B) = {
    implicit val ft = new FunctionBasedRowReader2(f)
    convertTo[B]
  }

  def as[B: ClassTag, A0: TypeConverter, A1: TypeConverter, A2: TypeConverter](f: (A0, A1, A2) => B) = {
    implicit val ft = new FunctionBasedRowReader3(f)
    convertTo[B]
  }

  def as[B: ClassTag, A0: TypeConverter, A1: TypeConverter, A2: TypeConverter,
  A3: TypeConverter](f: (A0, A1, A2, A3) => B) = {
    implicit val ft = new FunctionBasedRowReader4(f)
    convertTo[B]
  }

  def as[B: ClassTag, A0: TypeConverter, A1: TypeConverter, A2: TypeConverter, A3: TypeConverter,
  A4: TypeConverter](f: (A0, A1, A2, A3, A4) => B) = {
    implicit val ft = new FunctionBasedRowReader5(f)
    convertTo[B]
  }

  def as[B: ClassTag, A0: TypeConverter, A1: TypeConverter, A2: TypeConverter, A3: TypeConverter,
  A4: TypeConverter, A5: TypeConverter](f: (A0, A1, A2, A3, A4, A5) => B) = {
    implicit val ft = new FunctionBasedRowReader6(f)
    convertTo[B]
  }

  def as[B: ClassTag, A0: TypeConverter, A1: TypeConverter, A2: TypeConverter, A3: TypeConverter,
  A4: TypeConverter, A5: TypeConverter, A6: TypeConverter](f: (A0, A1, A2, A3, A4, A5, A6) => B) = {
    implicit val ft = new FunctionBasedRowReader7(f)
    convertTo[B]
  }

  def as[B: ClassTag, A0: TypeConverter, A1: TypeConverter, A2: TypeConverter, A3: TypeConverter,
  A4: TypeConverter, A5: TypeConverter, A6: TypeConverter,
  A7: TypeConverter](f: (A0, A1, A2, A3, A4, A5, A6, A7) => B) = {
    implicit val ft = new FunctionBasedRowReader8(f)
    convertTo[B]
  }

  def as[B: ClassTag, A0: TypeConverter, A1: TypeConverter, A2: TypeConverter, A3: TypeConverter,
  A4: TypeConverter, A5: TypeConverter, A6: TypeConverter, A7: TypeConverter,
  A8: TypeConverter](f: (A0, A1, A2, A3, A4, A5, A6, A7, A8) => B) = {
    implicit val ft = new FunctionBasedRowReader9(f)
    convertTo[B]
  }

  def as[B: ClassTag, A0: TypeConverter, A1: TypeConverter, A2: TypeConverter, A3: TypeConverter,
  A4: TypeConverter, A5: TypeConverter, A6: TypeConverter, A7: TypeConverter,
  A8: TypeConverter, A9: TypeConverter](f: (A0, A1, A2, A3, A4, A5, A6, A7, A8, A9) => B) = {
    implicit val ft = new FunctionBasedRowReader10(f)
    convertTo[B]
  }

  def as[B: ClassTag, A0: TypeConverter, A1: TypeConverter, A2: TypeConverter, A3: TypeConverter,
  A4: TypeConverter, A5: TypeConverter, A6: TypeConverter, A7: TypeConverter, A8: TypeConverter,
  A9: TypeConverter, A10: TypeConverter](f: (A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10) => B) = {
    implicit val ft = new FunctionBasedRowReader11(f)
    convertTo[B]
  }

  def as[B: ClassTag, A0: TypeConverter, A1: TypeConverter, A2: TypeConverter, A3: TypeConverter,
  A4: TypeConverter, A5: TypeConverter, A6: TypeConverter, A7: TypeConverter, A8: TypeConverter,
  A9: TypeConverter, A10: TypeConverter, A11: TypeConverter](
                                                              f: (A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11) => B) = {
    implicit val ft = new FunctionBasedRowReader12(f)
    convertTo[B]
  }
}


