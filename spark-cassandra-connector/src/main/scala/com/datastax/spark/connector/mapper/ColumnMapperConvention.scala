package com.datastax.spark.connector.mapper

import com.datastax.spark.connector.ColumnRef
import org.apache.commons.lang.StringUtils

object ColumnMapperConvention {

  def camelCaseToUnderscore(str: String): String =
    StringUtils.splitByCharacterTypeCamelCase(str).mkString("_").replaceAll("_+", "_").toLowerCase

  def columnForProperty(propertyName: String, columnByName: Map[String, ColumnRef]): Option[ColumnRef] = {
    val underscoreName = camelCaseToUnderscore(propertyName)
    val candidateColumnNames = Seq(propertyName, underscoreName)
    candidateColumnNames.iterator
      .map(name => columnByName.get(name))
      .find(_.isDefined)
      .flatten
  }
}
