package com.datastax.spark.connector.util

import com.datastax.spark.connector.CassandraRow
import com.datastax.spark.connector.mapper.{JavaBeanColumnMapper, ColumnMapper}
import com.datastax.spark.connector.rdd.reader.RowReaderFactory
import com.datastax.spark.connector.writer.RowWriterFactory

import scala.collection.JavaConversions._
import scala.reflect._
import scala.reflect.api._
import scala.reflect.runtime.universe._
import scala.reflect.api.{Mirror, TypeCreator}

import java.util.{Map => JavaMap}

/** A helper class to make it possible to access components written in Scala from Java code. */
object JavaApiHelper {

  /** Returns a `TypeTag` for the given class. */
  def getTypeTag[T](clazz: Class[T]): TypeTag[T] = {
    TypeTag.apply(runtimeMirror(Thread.currentThread().getContextClassLoader), new TypeCreator {
      override def apply[U <: Universe with Singleton](m: Mirror[U]): U#Type = {
        m.staticClass(clazz.getName).toTypeConstructor
      }
    })
  }

  def getClassTag[T](clazz: Class[T]): ClassTag[T] = ClassTag(clazz)

  def toScalaMap[K, V](map: JavaMap[K, V]): Map[K, V] = Map(map.toSeq: _*)

  def toScalaSeq[T](array: Array[T]): Seq[T] = array

  def toScalaSeq[T](iterable: java.lang.Iterable[T]): Seq[T] = iterable.toSeq

  def defaultRowWriterFactory[T](classTag: ClassTag[T], mapper: ColumnMapper[T]) = {
    RowWriterFactory.defaultRowWriterFactory(classTag, mapper)
  }

  def javaBeanColumnMapper[T](classTag: ClassTag[T], columnNameOverride: JavaMap[String, String]): ColumnMapper[T] =
    new JavaBeanColumnMapper[T](toScalaMap(columnNameOverride))(classTag)

  def genericRowReaderFactory: RowReaderFactory[CassandraRow] = RowReaderFactory.GenericRowReader$

}
