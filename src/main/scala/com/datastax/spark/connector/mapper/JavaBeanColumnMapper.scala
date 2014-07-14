package com.datastax.spark.connector.mapper

import java.lang.reflect.Method

import com.datastax.spark.connector.cql.TableDef

import scala.reflect.ClassTag

class JavaBeanColumnMapper[T : ClassTag](columnNameOverride: Map[String, String] = Map.empty) extends ReflectionColumnMapper[T] {

  import com.datastax.spark.connector.mapper.JavaBeanColumnMapper._

  private def propertyName(accessorName: String) = {
    val AccessorRegex(_, strippedName) = accessorName
    strippedName(0).toLower + strippedName.substring(1)
  }

  override protected def isGetter(method: Method): Boolean =
    GetterRegex.findFirstMatchIn(method.getName).isDefined &&
    method.getParameterTypes.size == 0 &&
    method.getReturnType != Void.TYPE

  override protected def isSetter(method: Method): Boolean =
    SetterRegex.findFirstMatchIn(method.getName).isDefined &&
    method.getParameterTypes.size == 1 &&
    method.getReturnType == Void.TYPE

  override protected def getterToColumnName(getterName: String, tableDef: TableDef) = {
    val p = propertyName(getterName)
    columnNameOverride.getOrElse(p, columnNameForProperty(p, tableDef))
  }

  override protected def setterToColumnName(setterName: String, tableDef: TableDef) = {
    val p = propertyName(setterName)
    columnNameOverride.getOrElse(p, columnNameForProperty(p, tableDef))
  }

  override protected def constructorParamToColumnName(paramName: String, tableDef: TableDef) = {
    columnNameOverride.getOrElse(paramName, columnNameForProperty(paramName, tableDef))
  }
}

object JavaBeanColumnMapper {
  val GetterRegex = "^(get|is)(.+)$".r
  val SetterRegex = "^(set)(.+)$".r
  val AccessorRegex = "^(get|is|set)(.+)$".r
}