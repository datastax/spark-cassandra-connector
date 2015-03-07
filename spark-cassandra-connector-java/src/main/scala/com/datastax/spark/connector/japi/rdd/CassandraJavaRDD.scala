package com.datastax.spark.connector.japi.rdd

import java.lang.Iterable

import com.datastax.spark.connector.japi.CassandraJavaUtil
import com.datastax.spark.connector.rdd.CassandraRDD
import com.datastax.spark.connector.util.JavaApiHelper
import com.datastax.spark.connector.util.JavaApiHelper._
import org.apache.spark.api.java.function.Function
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD}

import scala.reflect.ClassTag


/**
 * @see [[com.datastax.spark.connector.japi.rdd.CassandraJavaRDDLike]]
 */
class CassandraJavaRDD[R](override val rdd: CassandraRDD[R])(override implicit val classTag: ClassTag[R])
  extends JavaRDD[R](rdd)
  with CassandraJavaRDDLike[R, CassandraJavaRDD[R]]
  with CassandraCommonJavaRDD[R, CassandraJavaRDD[R]] {

  def this(rdd: CassandraRDD[R], elementClass: Class[R]) =
    this(rdd)(getClassTag(elementClass))

  override def wrap(rdd: CassandraRDD[R]) = {
    new CassandraJavaRDD[R](rdd)
  }

  override def spanBy[K](f: Function[R, K], keyClass: Class[K]): JavaPairRDD[K, Iterable[R]] = {
    CassandraJavaUtil.javaFunctions(this).spanBy(f)(JavaApiHelper.getClassTag(keyClass))
  }
}

