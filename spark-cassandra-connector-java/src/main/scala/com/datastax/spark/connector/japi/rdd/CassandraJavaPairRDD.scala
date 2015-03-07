package com.datastax.spark.connector.japi.rdd

import java.lang.Iterable

import com.datastax.spark.connector.japi.CassandraJavaUtil
import com.datastax.spark.connector.rdd.CassandraRDD
import com.datastax.spark.connector.util.JavaApiHelper
import com.datastax.spark.connector.util.JavaApiHelper._
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.api.java.function.Function

import scala.reflect.ClassTag

/**
 * @see [[com.datastax.spark.connector.japi.rdd.CassandraJavaPairRDDLike]]
 */
class CassandraJavaPairRDD[K, V](override val rdd: CassandraRDD[(K, V)])
                                (override implicit val kClassTag: ClassTag[K],
                                 override implicit val vClassTag: ClassTag[V])
  extends JavaPairRDD[K, V](rdd)
  with CassandraJavaPairRDDLike[K, V, CassandraJavaPairRDD[K, V]]
  with CassandraCommonJavaRDD[(K, V), CassandraJavaPairRDD[K, V]] {

  override def wrap(rdd: CassandraRDD[(K, V)]) = {
    new CassandraJavaPairRDD[K, V](rdd)
  }

  def this(rdd: CassandraRDD[(K, V)], keyClass: Class[K], valueClass: Class[V]) =
    this(rdd)(getClassTag(keyClass), getClassTag(valueClass))

  override def spanBy[K2](f: Function[(K, V), K2], keyClass: Class[K2]): JavaPairRDD[K2, Iterable[(K, V)]] = {
    CassandraJavaUtil.javaFunctions(this).spanBy(f)(JavaApiHelper.getClassTag(keyClass))
  }

  override def spanByKey(): JavaPairRDD[K, Iterable[V]] = {
    CassandraJavaUtil.javaFunctions(this).spanByKey()
  }
}

