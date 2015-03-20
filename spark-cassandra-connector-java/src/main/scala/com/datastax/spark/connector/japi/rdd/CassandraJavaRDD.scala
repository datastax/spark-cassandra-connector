package com.datastax.spark.connector.japi.rdd

import java.lang.Iterable

import com.datastax.spark.connector.{SelectableColumnRef, NamedColumnRef}
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.japi.CassandraJavaUtil
import com.datastax.spark.connector.rdd.{ReadConf, CassandraRDD}
import com.datastax.spark.connector.util.JavaApiHelper
import com.datastax.spark.connector.util.JavaApiHelper._
import org.apache.spark.api.java.function.Function
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD}

import scala.reflect.ClassTag

class CassandraJavaRDD[R](override val rdd: CassandraRDD[R])(override implicit val classTag: ClassTag[R])
  extends JavaRDD[R](rdd) with CassandraJavaRDDLike[R, CassandraJavaRDD[R]] {

  def this(rdd: CassandraRDD[R], elementClass: Class[R]) =
    this(rdd)(getClassTag(elementClass))

  def wrap(rdd: CassandraRDD[R]) = {
    new CassandraJavaRDD[R](rdd)
  }

  override def select(columnNames: Array[String]) = {
    wrap(rdd.select(columnNames.map(c => c: NamedColumnRef): _*))
  }

  override def selectRefs(columnRefs: Array[SelectableColumnRef]) = {
    wrap(rdd.select(columnRefs: _*))
  }

  override def where(cqlWhereClause: String, args: Array[AnyRef]) = {
    wrap(rdd.where(cqlWhereClause, args: _*))
  }

  override def where(cqlWhereClause: String) = {
    wrap(rdd.where(cqlWhereClause, Nil))
  }

  override def withAscOrder = {
    wrap(rdd.withAscOrder)
  }

  override def withDescOrder = {
    wrap(rdd.withDescOrder)
  }

  override def limit(rowsNumber: Long) = {
    wrap(rdd.limit(rowsNumber))
  }

  override def selectedColumnNames: Array[String] = {
    rdd.selectedColumnRefs.map(_.asInstanceOf[NamedColumnRef]).map(_.columnName).toArray
  }

  override def selectedColumnRefs: Array[NamedColumnRef] = {
    rdd.selectedColumnRefs.map(_.asInstanceOf[NamedColumnRef]).toArray
  }

  override def withConnector(connector: CassandraConnector) = {
    wrap(rdd.withConnector(connector))
  }

  override def withReadConf(config: ReadConf) = {
    wrap(rdd.withReadConf(config))
  }

  override def toEmptyCassandraRDD = {
    wrap(rdd.toEmptyCassandraRDD)
  }

  override def spanBy[K](f: Function[R, K], keyClass: Class[K]): JavaPairRDD[K, Iterable[R]] = {
    CassandraJavaUtil.javaFunctions(this).spanBy(f)(JavaApiHelper.getClassTag(keyClass))
  }
}

