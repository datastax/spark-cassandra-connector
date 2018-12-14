package com.datastax.spark.connector.types

import scala.language.existentials
import scala.reflect.runtime.universe._
import scala.reflect.ClassTag
import com.datastax.spark.connector.types.TypeConverter.{MapConverter, VectorConverter, SetConverter}


trait CollectionColumnType[T] extends ColumnType[T] {
  def isCollection = true
}

case class ListType[T](elemType: ColumnType[T]) extends CollectionColumnType[Vector[T]] {
  @transient implicit lazy val elemTypeTag = SparkReflectionLock.synchronized { elemType.scalaTypeTag }
  @transient lazy val scalaTypeTag = SparkReflectionLock.synchronized { implicitly[TypeTag[Vector[T]]] }
  def cqlTypeName = s"list<${elemType.cqlTypeName}>"

  override def converterToCassandra: TypeConverter[_ <: AnyRef] =
    TypeConverter.forType[Vector[T]] match {
      case tc: VectorConverter[_] =>
          new TypeConverter.OptionToNullConverter(TypeConverter.listConverter(elemType.converterToCassandra))
      case tc => tc
    }
}

case class SetType[T](elemType: ColumnType[T]) extends CollectionColumnType[Set[T]] {
  @transient implicit lazy val elemTypeTag = SparkReflectionLock.synchronized { elemType.scalaTypeTag }
  @transient lazy val scalaTypeTag = SparkReflectionLock.synchronized { implicitly[TypeTag[Set[T]]] }
  def cqlTypeName = s"set<${elemType.cqlTypeName}>"

  override def converterToCassandra: TypeConverter[_ <: AnyRef] =
    TypeConverter.forType[Set[T]] match {
      case tc: SetConverter[_] =>
        new TypeConverter.OptionToNullConverter(
          TypeConverter.setConverter(elemType.converterToCassandra))
      case tc => tc
    }
}

case class MapType[K, V](keyType: ColumnType[K], valueType: ColumnType[V])
  extends CollectionColumnType[Map[K, V]] {
  @transient implicit lazy val keyTypeTag = SparkReflectionLock.synchronized { keyType.scalaTypeTag }
  @transient implicit lazy val valueTypeTag = SparkReflectionLock.synchronized { valueType.scalaTypeTag }
  @transient lazy val scalaTypeTag = SparkReflectionLock.synchronized { implicitly[TypeTag[Map[K, V]]] }
  def cqlTypeName = s"map<${keyType.cqlTypeName}, ${valueType.cqlTypeName}>"

  override def converterToCassandra: TypeConverter[_ <: AnyRef] =
    TypeConverter.forType[Map[K, V]] match {
      case tc: MapConverter[_, _] =>
        new TypeConverter.OptionToNullConverter(
          TypeConverter.mapConverter(keyType.converterToCassandra, valueType.converterToCassandra))
      case tc => tc
    }
}

