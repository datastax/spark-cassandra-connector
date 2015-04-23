package com.datastax.spark.connector.mapper

import com.datastax.spark.connector.cql.{StructDef, TableDef}
import org.apache.commons.lang.StringUtils

object ColumnMapperConvention {

  def camelCaseToUnderscore(str: String): String =
    StringUtils.splitByCharacterTypeCamelCase(str).mkString("_").replaceAll("_+", "_").toLowerCase

  def columnNameForProperty(propertyName: String, structDef: StructDef): String = {
    val underscoreName = camelCaseToUnderscore(propertyName)
    val candidateColumnNames = Seq(propertyName, underscoreName)
    val columnRef = candidateColumnNames.iterator
      .map(name => structDef.columnByName.get(name))
      .find(_.isDefined)
      .flatten
    columnRef.fold(underscoreName)(_.columnName)
  }
}
