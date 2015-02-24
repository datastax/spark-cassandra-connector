package com.datastax.spark.connector.mapper

import java.lang.reflect.{Constructor, Method}

import com.datastax.spark.connector.{ColumnRef, ColumnName}
import com.datastax.spark.connector.cql.TableDef
import com.datastax.spark.connector.rdd.reader.AnyObjectFactory

import scala.reflect.ClassTag

abstract class ReflectionColumnMapper[T : ClassTag] extends ColumnMapper[T] {

  import AnyObjectFactory._

  protected def isSetter(method: Method): Boolean
  protected def isGetter(method: Method): Boolean
  protected def setterToColumnName(setterName: String, tableDef: TableDef, aliasToColumnName: Map[String, String]): String
  protected def getterToColumnName(getterName: String, tableDef: TableDef, aliasToColumnName: Map[String, String]): String
  protected def constructorParamToColumnName(paramName: String, tableDef: TableDef, aliasToColumnName: Map[String, String]): String
  protected def allowsNull: Boolean

  override def columnMap(tableDef: TableDef, aliasToColumnName: Map[String, String]): ColumnMap = {

    val cls = implicitly[ClassTag[T]].runtimeClass

    def columnsOf(ctor: Constructor[_]): Seq[ColumnRef] = {
      if (isNoArgsConstructor(ctor))
        Nil
      else {
        val paramNames = paranamer.lookupParameterNames(ctor)
        val columnNames = paramNames
          .map(constructorParamToColumnName(_, tableDef, aliasToColumnName))
          .filter(_ != "$_outer")
        columnNames.map(ColumnName(_, None))
      }
    }

    val constructor = columnsOf(resolveConstructor(cls))

    val getters: Map[String, ColumnRef] = {
      for (method <- cls.getMethods if isGetter(method)) yield {
        val methodName = method.getName
        val columnName = getterToColumnName(methodName, tableDef, aliasToColumnName)
        (methodName, ColumnName(columnName))
      }
    }.toMap

    val setters: Map[String, ColumnRef] = {
      for (method <- cls.getMethods if isSetter(method)) yield {
        val methodName = method.getName
        val columnName = setterToColumnName(methodName, tableDef, aliasToColumnName)
        (methodName, ColumnName(columnName))
      }
    }.toMap

    new SimpleColumnMap(constructor, getters, setters, allowsNull)
  }
}
