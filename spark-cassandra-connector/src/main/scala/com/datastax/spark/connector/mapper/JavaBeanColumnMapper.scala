package com.datastax.spark.connector.mapper

import java.lang.reflect.Method

import com.datastax.spark.connector.ColumnRef
import com.datastax.spark.connector.cql.{TableDef, StructDef}

import scala.reflect.ClassTag

class JavaBeanColumnMapper[T : ClassTag](columnNameOverride: Map[String, String] = Map.empty)
  extends ReflectionColumnMapper[T] {

  import com.datastax.spark.connector.mapper.JavaBeanColumnMapper._

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

  private def resolve(name: String, columns: Map[String, ColumnRef]): Option[ColumnRef] = {
    val overridenName = columnNameOverride.getOrElse(name, name)
    ColumnMapperConvention.columnForProperty(overridenName, columns)
  }

  override protected def getterToColumnName(getterName: String, columns: Map[String, ColumnRef]) = {
    val p = propertyName(getterName)
    resolve(p, columns)
  }

  override protected def setterToColumnName(setterName: String, columns: Map[String, ColumnRef]) = {
    val p = propertyName(setterName)
    resolve(p, columns)
  }

  override protected def constructorParamToColumnName(
      paramName: String,
      columns: Map[String, ColumnRef]) = {
    resolve(paramName, columns)
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