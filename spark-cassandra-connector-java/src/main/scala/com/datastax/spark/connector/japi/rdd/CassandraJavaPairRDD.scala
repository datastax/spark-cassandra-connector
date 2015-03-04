package com.datastax.spark.connector.japi.rdd

import scala.reflect.ClassTag

import com.datastax.spark.connector.{ColumnName, NamedColumnRef}
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.rdd.{CassandraRDD, ReadConf}
import org.apache.spark.api.java.JavaPairRDD

/**
 * @author Jacek Lewandowski
 */
class CassandraJavaPairRDD[K: ClassTag, V: ClassTag](override val rdd: CassandraRDD[(K, V)])
    extends JavaPairRDD[K, V](rdd) with CassandraJavaPairRDDLike[K, V, CassandraJavaPairRDD[K, V]] {

  override def select(columnNames: String*): CassandraJavaPairRDD[K, V] = {
    new CassandraJavaPairRDD(rdd.select(columnNames.map(ColumnName.apply): _*))
  }

  override def selectedColumnNames(): Array[String] = {
    Array.empty[String]//rdd.selectedColumnNames.map(_.columnName).toArray
  }

  override def withCassandraConnector(connector: CassandraConnector): CassandraJavaPairRDD[K, V] = {
    new CassandraJavaPairRDD(rdd.withConnector(connector))
  }

  override def withReadConf(config: ReadConf): CassandraJavaPairRDD[K, V] = {
    new CassandraJavaPairRDD(rdd.withReadConf(config))
  }

  override def selectRefs(selectionColumns: NamedColumnRef*): CassandraJavaPairRDD[K, V] = {
    new CassandraJavaPairRDD(rdd.select(selectionColumns: _*))
  }

  override def where(cqlWhereClause: String, args: AnyRef*): CassandraJavaPairRDD[K, V] = {
    new CassandraJavaPairRDD(rdd.where(cqlWhereClause, args: _*))
  }

}
