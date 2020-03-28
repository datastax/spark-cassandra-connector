package com.datastax.spark.connector.rdd

import java.io.{Serializable => JavaSerializable}

import com.datastax.spark.connector.types.TypeConverter

import scala.annotation.implicitNotFound

@implicitNotFound("Not a valid RDD type. There should exists either a type converter for the type or the type should implement Serializable")
trait ValidRDDType[T]

object ValidRDDType {
  implicit def withTypeConverterAsValidRDDType[T](implicit tc: TypeConverter[T]): ValidRDDType[T] = null

  implicit def javaSerializableAsValidRDDType[T <: JavaSerializable]: ValidRDDType[T] = null
}
