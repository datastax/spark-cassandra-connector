package com.datastax.spark.connector.japi.rdd

import scala.reflect.ClassTag

import com.datastax.spark.connector.{SelectableColumnRef, ColumnName, NamedColumnRef}
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.rdd.{CassandraRDD, ReadConf}
import org.apache.spark.api.java.JavaPairRDD
import com.datastax.spark.connector.util.JavaApiHelper._

/**
 * @see [[com.datastax.spark.connector.japi.rdd.CassandraJavaPairRDD]]
 */
class CassandraJavaPairRDDImpl[K, V](override val rdd: CassandraRDD[(K, V)])
                                    (override implicit val kClassTag: ClassTag[K],
                                     override implicit val vClassTag: ClassTag[V])
    extends JavaPairRDD[K, V](rdd) with CassandraJavaPairRDD[K, V, CassandraJavaPairRDDImpl[K, V]] {

  def this(rdd: CassandraRDD[(K, V)], keyClass: Class[K], valueClass: Class[V]) =
    this(rdd)(getClassTag(keyClass), getClassTag(valueClass))

  override def select(columnNames: String*): CassandraJavaPairRDDImpl[K, V] = {
    // explicit type argument is intentional and required here
    // no inspection RedundantTypeArguments
    selectRefs(columnNames.map(ColumnName(_)): _*)
    //new CassandraJavaPairRDDImpl[K, V](rdd.select(columnNames.map(ColumnName(_)): _*))
  }

  override def selectRefs(selectionColumns: NamedColumnRef*): CassandraJavaPairRDDImpl[K, V] = {
    new CassandraJavaPairRDDImpl(rdd.select(selectionColumns: _*))
  }

  override def where(cqlWhereClause: String, args: AnyRef*): CassandraJavaPairRDDImpl[K, V] = {
    new CassandraJavaPairRDDImpl[K, V](rdd.where(cqlWhereClause, args: _*))
  }

  override def withAscOrder: CassandraJavaPairRDDImpl[K, V] = {
    new CassandraJavaPairRDDImpl[K, V](rdd.withAscOrder)
  }

  override def withDescOrder: CassandraJavaPairRDDImpl[K, V] = {
    new CassandraJavaPairRDDImpl[K, V](rdd.withDescOrder)
  }

  override def limit(rowsNumber: Long): CassandraJavaPairRDDImpl[K, V] = {
    new CassandraJavaPairRDDImpl[K, V](rdd.limit(rowsNumber))
  }

  override def selectedColumnNames: Array[String] = {
    rdd.selectedColumnRefs.map(_.selectedAs).toArray
    // TODO rdd.selectedColumnRefs.toArray[String](getClassTag(classOf[String]))
  }

  override def withCassandraConnector(connector: CassandraConnector): CassandraJavaPairRDDImpl[K, V] = {
    new CassandraJavaPairRDDImpl[K, V](rdd.withConnector(connector))
  }

  override def withReadConf(config: ReadConf): CassandraJavaPairRDDImpl[K, V] = {
    new CassandraJavaPairRDDImpl(rdd.withReadConf(config))
  }

  def toEmptyCassandraRDD: CassandraJavaPairRDDImpl[K, V] = {
    new CassandraJavaPairRDDImpl[K, V](rdd.toEmptyCassandraRDD)
  }
}

