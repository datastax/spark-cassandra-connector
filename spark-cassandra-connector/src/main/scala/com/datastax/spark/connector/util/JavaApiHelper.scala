package com.datastax.spark.connector.util

import com.datastax.spark.connector.{AllColumns, SomeColumns, CassandraRow}
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

  def mirror = runtimeMirror(Thread.currentThread().getContextClassLoader)

  /** Returns a `TypeTag` for the given class. */
  def getTypeTag[T](clazz: Class[T]): TypeTag[T] = {
    TypeTag.apply(mirror, new TypeCreator {
      override def apply[U <: Universe with Singleton](m: Mirror[U]): U#Type = {
        m.staticClass(clazz.getName).toTypeConstructor
      }
    })
  }

  def getTypeTag[T](clazz: Class[_], typeParams: TypeTag[_]*): TypeTag[T] = {
    TypeTag.apply(mirror, new TypeCreator {
      override def apply[U <: Universe with Singleton](m: Mirror[U]) = {
        val ct = m.staticClass(clazz.getName).toTypeConstructor.asInstanceOf[m.universe.Type]
        val tpt = typeParams.map(_.in(m).tpe.asInstanceOf[m.universe.Type]).toList
        m.universe.appliedType(ct, tpt).asInstanceOf[U#Type]
      }
    })
  }

  def getClassTag[T](clazz: Class[T]): ClassTag[T] = ClassTag(clazz)

  def getRuntimeClass[T](typeTag: TypeTag[T]): Class[T] = rootMirror.runtimeClass(typeTag.tpe).asInstanceOf[Class[T]]

  def getRuntimeClass[T](classTag: ClassTag[T]): Class[T] = classTag.runtimeClass.asInstanceOf[Class[T]]

  def toScalaMap[K, V](map: JavaMap[K, V]): Map[K, V] = Map(map.toSeq: _*)

  def toScalaSeq[T](array: Array[T]): Seq[T] = array

  def toScalaSeq[T](iterable: java.lang.Iterable[T]): Seq[T] = iterable.toSeq

  def toColumns(array: Array[String]): SomeColumns = SomeColumns(array: _*)

  def defaultRowWriterFactory[T](mapper: ColumnMapper[T]) = {
    RowWriterFactory.defaultRowWriterFactory(mapper)
  }

  def javaBeanColumnMapper[T](classTag: ClassTag[T], columnNameOverride: JavaMap[String, String]): ColumnMapper[T] =
    new JavaBeanColumnMapper[T](toScalaMap(columnNameOverride))(classTag)

  def genericRowReaderFactory: RowReaderFactory[CassandraRow] = RowReaderFactory.GenericRowReader$

  def allColumns = AllColumns
}
