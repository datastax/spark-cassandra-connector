package com.datastax.spark.connector.types

import scala.language.existentials
import scala.reflect.runtime.universe._

case class VectorType[T](elemType: ColumnType[T], dimension: Int) extends ColumnType[Seq[T]] {

  override def isCollection: Boolean = false

  @transient
  lazy val scalaTypeTag = {
    implicit val elemTypeTag = elemType.scalaTypeTag
    implicitly[TypeTag[Seq[T]]]
  }

  def cqlTypeName = s"vector<${elemType.cqlTypeName}, ${dimension}>"

  override def converterToCassandra: TypeConverter[_ <: AnyRef] =
    new TypeConverter.OptionToNullConverter(TypeConverter.seqConverter(elemType.converterToCassandra))
}
