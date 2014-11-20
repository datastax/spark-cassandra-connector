package com.datastax.spark.connector.mapper

import java.lang.reflect.{Constructor, Method}

import com.datastax.spark.connector.cql.TableDef
import com.datastax.spark.connector.rdd.reader.AnyObjectFactory
import org.apache.commons.lang.StringUtils

import scala.reflect.ClassTag

abstract class ReflectionColumnMapper[T : ClassTag] extends ColumnMapper[T] {

  import AnyObjectFactory._

  protected def isSetter(method: Method): Boolean
  protected def isGetter(method: Method): Boolean
  protected def setterToColumnName(setterName: String, tableDef: TableDef): String
  protected def getterToColumnName(getterName: String, tableDef: TableDef): String
  protected def constructorParamToColumnName(paramName: String, tableDef: TableDef): String
  protected def allowsNull: Boolean

  protected final def camelCaseToUnderscore(str: String): String =
    StringUtils.splitByCharacterTypeCamelCase(str).mkString("_").replaceAll("_+", "_").toLowerCase

  protected final def columnNameForProperty(propertyName: String, tableDef: TableDef): String = {
    val underscoreName = camelCaseToUnderscore(propertyName)
    val candidateColumnNames = Seq(propertyName, underscoreName)
    val columnRef = candidateColumnNames.iterator.map(tableDef.columnByName.get).find(_.isDefined).flatten
    columnRef.fold(underscoreName)(_.columnName)
  }

  override def columnMap(tableDef: TableDef): ColumnMap = {

    val cls = implicitly[ClassTag[T]].runtimeClass

    def columnsOf(ctor: Constructor[_]): Seq[ColumnRef] = {
      if (isNoArgsConstructor(ctor))
        Nil
      else {
        val paramNames = paranamer.lookupParameterNames(ctor)
        val columnNames = paramNames
          .map(constructorParamToColumnName(_, tableDef))
          .filter(_ != "$_outer")
        columnNames.map(NamedColumnRef)
      }
    }

    val constructor = columnsOf(resolveConstructor(cls))

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

    new SimpleColumnMap(constructor, getters, setters, allowsNull)
  }
}
