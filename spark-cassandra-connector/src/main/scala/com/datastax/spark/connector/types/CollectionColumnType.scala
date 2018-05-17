package com.datastax.spark.connector.types

import scala.language.existentials
import scala.reflect.runtime.universe._

trait CollectionColumnType[T] extends ColumnType[T] {
  def isCollection = true
}

case class ListType[T](elemType: ColumnType[T]) extends CollectionColumnType[Vector[T]] {

  @transient
  lazy val scalaTypeTag = {
    implicit val elemTypeTag = elemType.scalaTypeTag
    implicitly[TypeTag[Vector[T]]]
  }

  def cqlTypeName = s"list<${elemType.cqlTypeName}>"

  override def converterToCassandra: TypeConverter[_ <: AnyRef] =
    new TypeConverter.OptionToNullConverter(TypeConverter.listConverter(elemType.converterToCassandra))
}

case class SetType[T](elemType: ColumnType[T]) extends CollectionColumnType[Set[T]] {

  @transient
  lazy val scalaTypeTag = {
    implicit val elemTypeTag = elemType.scalaTypeTag
    implicitly[TypeTag[Set[T]]]
  }

  def cqlTypeName = s"set<${elemType.cqlTypeName}>"

  override def converterToCassandra: TypeConverter[_ <: AnyRef] =
    new TypeConverter.OptionToNullConverter(TypeConverter.setConverter(elemType.converterToCassandra))
}

case class MapType[K, V](keyType: ColumnType[K], valueType: ColumnType[V])
  extends CollectionColumnType[Map[K, V]] {

  @transient
  lazy val scalaTypeTag = {
    implicit val keyTypeTag = keyType.scalaTypeTag
    implicit val valueTypeTag = valueType.scalaTypeTag
    implicitly[TypeTag[Map[K, V]]]
  }

  def cqlTypeName = s"map<${keyType.cqlTypeName}, ${valueType.cqlTypeName}>"

  override def converterToCassandra: TypeConverter[_ <: AnyRef] =
    new TypeConverter.OptionToNullConverter(
      TypeConverter.mapConverter(keyType.converterToCassandra, valueType.converterToCassandra))
}

