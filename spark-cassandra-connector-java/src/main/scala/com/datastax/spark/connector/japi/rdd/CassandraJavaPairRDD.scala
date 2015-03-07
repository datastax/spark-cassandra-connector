package com.datastax.spark.connector.japi.rdd

import scala.reflect.ClassTag
import org.apache.spark.api.java.JavaPairRDD
import com.datastax.spark.connector.{ColumnName, NamedColumnRef}
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.rdd.{CassandraRDD, ReadConf}
import com.datastax.spark.connector.util.JavaApiHelper._

/**
 * @see [[com.datastax.spark.connector.japi.rdd.CassandraJavaPairRDDLike]]
 */
class CassandraJavaPairRDD[K, V](override val rdd: CassandraRDD[(K, V)])
                                    (override implicit val kClassTag: ClassTag[K],
                                     override implicit val vClassTag: ClassTag[V])
    extends JavaPairRDD[K, V](rdd) with CassandraJavaPairRDDLike[K, V, CassandraJavaPairRDD[K, V]] {

  def this(rdd: CassandraRDD[(K, V)], keyClass: Class[K], valueClass: Class[V]) =
    this(rdd)(getClassTag(keyClass), getClassTag(valueClass))

  override def select(columnNames: String*): CassandraJavaPairRDD[K, V] = {
    // explicit type argument is intentional and required here
    // no inspection RedundantTypeArguments
    selectRefs(columnNames.map(ColumnName(_)): _*)
  }

  override def selectRefs(selectionColumns: NamedColumnRef*): CassandraJavaPairRDD[K, V] = {
    new CassandraJavaPairRDD(rdd.select(selectionColumns: _*))
  }

  override def where(cqlWhereClause: String, args: AnyRef*): CassandraJavaPairRDD[K, V] = {
    new CassandraJavaPairRDD[K, V](rdd.where(cqlWhereClause, args: _*))
  }

  override def withAscOrder: CassandraJavaPairRDD[K, V] = {
    new CassandraJavaPairRDD[K, V](rdd.withAscOrder)
  }

  override def withDescOrder: CassandraJavaPairRDD[K, V] = {
    new CassandraJavaPairRDD[K, V](rdd.withDescOrder)
  }

  override def limit(rowsNumber: Long): CassandraJavaPairRDD[K, V] = {
    new CassandraJavaPairRDD[K, V](rdd.limit(rowsNumber))
  }

  override def selectedColumnNames: Array[String] = {
    rdd.selectedColumnRefs.map(_.asInstanceOf[NamedColumnRef]).map(_.columnName).toArray
  }

  override def withCassandraConnector(connector: CassandraConnector): CassandraJavaPairRDD[K, V] = {
    new CassandraJavaPairRDD[K, V](rdd.withConnector(connector))
  }

  override def withReadConf(config: ReadConf): CassandraJavaPairRDD[K, V] = {
    new CassandraJavaPairRDD(rdd.withReadConf(config))
  }

  def toEmptyCassandraRDD: CassandraJavaPairRDD[K, V] = {
    new CassandraJavaPairRDD[K, V](rdd.toEmptyCassandraRDD)
  }
}

