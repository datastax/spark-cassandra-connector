package com.datastax.spark.connector.mapper

import java.lang.reflect.{Constructor, Method}

import com.datastax.spark.connector.cql.TableDef
import com.thoughtworks.paranamer.{ParameterNamesNotFoundException, AdaptiveParanamer}
import org.apache.commons.lang.StringUtils

import scala.reflect.ClassTag

abstract class ReflectionColumnMapper[T : ClassTag] extends ColumnMapper[T] {

  protected def isSetter(method: Method): Boolean
  protected def isGetter(method: Method): Boolean
  protected def setterToColumnName(setterName: String, tableDef: TableDef): String
  protected def getterToColumnName(getterName: String, tableDef: TableDef): String
  protected def constructorParamToColumnName(paramName: String, tableDef: TableDef): String

  protected final def camelCaseToUnderscore(str: String): String =
    StringUtils.splitByCharacterTypeCamelCase(str).mkString("_").replaceAll("_+", "_").toLowerCase

  protected final def columnNameForProperty(propertyName: String, tableDef: TableDef): String = {
    val underscoreName = camelCaseToUnderscore(propertyName)
    val candidateColumnNames = Seq(propertyName, underscoreName)
    val columnRef = candidateColumnNames.iterator.map(tableDef.columnByName.get).find(_.isDefined).flatten
    columnRef.fold(underscoreName)(_.columnName)
  }

  def columnsOf(ctor: Constructor[_], tableDef: TableDef): Seq[ColumnRef]

  override def columnMap(tableDef: TableDef): ColumnMap = {
    val cls = implicitly[ClassTag[T]].runtimeClass

    val constructor =
      columnsOf(cls.getConstructors()(0), tableDef)

    val getters: Map[String, ColumnRef] = {
      for (method <- cls.getMethods if isGetter(method)) yield {
        val methodName = method.getName
        val columnName = getterToColumnName(methodName, tableDef)
        (methodName, NamedColumnRef(columnName))
      }
    }.toMap

    val setters: Map[String, ColumnRef] = {
      for (method <- cls.getMethods if isSetter(method)) yield {
        val methodName = method.getName
        val columnName = setterToColumnName(methodName, tableDef)
        (methodName, NamedColumnRef(columnName))
      }
    }.toMap

    new SimpleColumnMap(constructor, getters, setters)
  }
}
