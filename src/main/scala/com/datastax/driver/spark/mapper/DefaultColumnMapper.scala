package com.datastax.driver.spark.mapper

import java.lang.reflect.Method

import com.datastax.driver.spark.connector.TableDef

import scala.reflect.ClassTag

class DefaultColumnMapper[T : ClassTag](columnNameOverride: Map[String, String] = Map.empty) extends ReflectionColumnMapper[T] {

  import com.datastax.driver.spark.mapper.DefaultColumnMapper._

  private def setterNameToPropertyName(str: String) =
    str.substring(0, str.length - SetterSuffix.length)

  override def isGetter(method: Method) = {
    method.getParameterTypes.size == 0 &&
    method.getReturnType != Void.TYPE
  }

  override def isSetter(method: Method) = {
    method.getParameterTypes.size == 1 &&
    method.getReturnType == Void.TYPE &&
    method.getName.endsWith(SetterSuffix)
  }

  override def constructorParamToColumnName(paramName: String, tableDef: TableDef) =
    columnNameOverride.getOrElse(paramName, columnNameForProperty(paramName, tableDef))

  override def getterToColumnName(getterName: String, tableDef: TableDef) =
    columnNameOverride.getOrElse(getterName, columnNameForProperty(getterName, tableDef))

  override def setterToColumnName(setterName: String, tableDef: TableDef) = {
    val propertyName = setterNameToPropertyName(setterName)
    columnNameOverride.getOrElse(propertyName, columnNameForProperty(propertyName, tableDef))
  }
}

object DefaultColumnMapper {
  val SetterSuffix: String = "_$eq"
}
