package com.datastax.spark.connector.mapper

import com.datastax.spark.connector.cql.TableDef
import org.apache.commons.lang.StringUtils

object ColumnMapperConvention {

  def camelCaseToUnderscore(str: String): String =
    StringUtils.splitByCharacterTypeCamelCase(str).mkString("_").replaceAll("_+", "_").toLowerCase

  def columnNameForProperty(propertyName: String, tableDef: TableDef): String = {
    val underscoreName = camelCaseToUnderscore(propertyName)
    val candidateColumnNames = Seq(propertyName, underscoreName)
    val columnRef = candidateColumnNames.iterator.map(tableDef.columnByName.get).find(_.isDefined).flatten
    columnRef.fold(underscoreName)(_.columnName)
  }
}
