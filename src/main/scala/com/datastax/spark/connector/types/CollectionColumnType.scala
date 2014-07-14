package com.datastax.spark.connector.types

import com.datastax.spark.connector.types.TypeConverter.OptionToNullConverter

import scala.language.existentials

trait CollectionColumnType[T] extends ColumnType[T] {
  def isCollection = true
}

case class ListType[T](elemType: ColumnType[T]) extends CollectionColumnType[List[T]] {
  def scalaTypeName = "Vector[" + elemType.scalaTypeName + "]"
  def converterToCassandra = TypeConverter.javaArrayListConverter(elemType.converterToCassandra)
}

case class SetType[T](elemType: ColumnType[T]) extends CollectionColumnType[Set[T]] {
  def scalaTypeName = "Set[" + elemType.scalaTypeName + "]"
  def converterToCassandra =
    new OptionToNullConverter(TypeConverter.javaHashSetConverter(elemType.converterToCassandra))
}

case class MapType[K, V](keyType: ColumnType[K], valueType: ColumnType[V]) extends CollectionColumnType[Map[K, V]] {
  def scalaTypeName = "Map[" + keyType.scalaTypeName + ", " + valueType.scalaTypeName + "]"
  def converterToCassandra: TypeConverter[_] =
    new OptionToNullConverter(
      TypeConverter.javaHashMapConverter(keyType.converterToCassandra, valueType.converterToCassandra))
}

