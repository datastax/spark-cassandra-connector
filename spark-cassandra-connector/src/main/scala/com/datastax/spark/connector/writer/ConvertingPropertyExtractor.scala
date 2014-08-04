package com.datastax.spark.connector.writer

import com.datastax.spark.connector.types.TypeConverter

/** Extracts property values from an object and additionally converts them to desired types */
class ConvertingPropertyExtractor[T](val cls: Class[T], properties: Seq[(String, TypeConverter[_])])
  extends Serializable {

  val (propertyNames, propertyTypes) = properties.toArray.unzip
  val propertyTypeByName = properties.toMap

  private val simpleExtractor =
    new PropertyExtractor[T](cls, propertyNames)

  def extract(obj: T): Array[AnyRef] =
    convert(simpleExtractor.extract(obj))


  def extract(obj: T, target: Array[AnyRef]): Array[AnyRef] =
    convert(simpleExtractor.extract(obj, target))

  def extractProperty(obj: T, propertyName: String): AnyRef = {
    val propertyValue = simpleExtractor.extractProperty(obj, propertyName)
    val converter = propertyTypeByName(propertyName)
    converter.convert(propertyValue).asInstanceOf[AnyRef]
  }

  def convert(data: Array[AnyRef]): Array[AnyRef] = {
    for (i <- 0 until data.length)
      data(i) = propertyTypes(i).convert(data(i)).asInstanceOf[AnyRef]
    data
  }
}
