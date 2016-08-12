package com.datastax.spark.connector.mapper

import com.datastax.spark.connector.ColumnRef
import com.google.common.base.CaseFormat
import org.apache.commons.lang.StringUtils

object ColumnMapperConvention {

  def camelCaseToUnderscore(str: String): String =
    StringUtils.splitByCharacterTypeCamelCase(str).mkString("_").replaceAll("_+", "_").toLowerCase

  def underscoreToCamelCase(str: String): String =
    CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, str)

  def columnForProperty(propertyName: String, columnByName: Map[String, ColumnRef]): Option[ColumnRef] = {
    val underscoreName = camelCaseToUnderscore(propertyName)
    val candidateColumnNames = Seq(propertyName, underscoreName)
    candidateColumnNames.iterator
      .map(name => columnByName.get(name))
      .find(_.isDefined)
      .flatten
  }
}
