package com.datastax.spark.connector.mapper

import java.lang.reflect.Method

import com.datastax.spark.connector.cql.{TableDef, StructDef}

import scala.reflect.ClassTag

class JavaBeanColumnMapper[T : ClassTag](columnNameOverride: Map[String, String] = Map.empty) 
  extends ReflectionColumnMapper[T] {

  import com.datastax.spark.connector.mapper.JavaBeanColumnMapper._

  override def classTag: ClassTag[T] = implicitly[ClassTag[T]]

  private def propertyName(accessorName: String) = {
    val AccessorRegex(_, strippedName) = accessorName
    strippedName(0).toLower + strippedName.substring(1)
  }

  override protected def isGetter(method: Method): Boolean =
    GetterRegex.findFirstMatchIn(method.getName).isDefined &&
    method.getParameterTypes.isEmpty &&
    method.getReturnType != Void.TYPE

  override protected def isSetter(method: Method): Boolean =
    SetterRegex.findFirstMatchIn(method.getName).isDefined &&
    method.getParameterTypes.size == 1 &&
    method.getReturnType == Void.TYPE

  def resolve(name: String, structDef: StructDef, aliasToColumnName: Map[String, String]): String = {
    columnNameOverride
      .orElse(aliasToColumnName)
      .applyOrElse(name, ColumnMapperConvention.columnNameForProperty(_: String, structDef))
  }

  override protected def getterToColumnName(
      getterName: String,
      structDef: StructDef,
      aliasToColumnName: Map[String, String]) = {
    val p = propertyName(getterName)
    columnNameOverride.getOrElse(p, resolve(p, structDef, aliasToColumnName))
  }

  override protected def setterToColumnName(
      setterName: String,
      structDef: StructDef,
      aliasToColumnName: Map[String, String]) = {
    val p = propertyName(setterName)
    columnNameOverride.getOrElse(p, resolve(p, structDef, aliasToColumnName))
  }

  override protected def constructorParamToColumnName(
      paramName: String,
      structDef: StructDef,
      aliasToColumnName: Map[String, String]) = {
    columnNameOverride.getOrElse(paramName, resolve(paramName, structDef, aliasToColumnName))
  }

  /** Java Beans allow nulls in property values */
  override protected def allowsNull = true

  // TODO: Implement
  override def newTable(keyspaceName: String, tableName: String): TableDef = ???
}

object JavaBeanColumnMapper {
  val GetterRegex = "^(get|is)(.+)$".r
  val SetterRegex = "^(set)(.+)$".r
  val AccessorRegex = "^(get|is|set)(.+)$".r
}