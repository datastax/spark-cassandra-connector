package com.datastax.spark.connector.mapper

import java.lang.reflect.{Constructor, Method}

import com.datastax.spark.connector.cql.TableDef
import com.datastax.spark.connector.rdd.reader.{JavaObjectFactory, ObjectFactory}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

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

  override def objectFactory[R <: T : TypeTag]: ObjectFactory[R] = new JavaObjectFactory[R]

  override def columnsOf(ctor: Constructor[_], tableDef: TableDef): Seq[ColumnRef] = {
    // we play only with no-args constructors here
    Nil
  }
}

object JavaBeanColumnMapper {
  val GetterRegex = "^(get|is)(.+)$".r
  val SetterRegex = "^(set)(.+)$".r
  val AccessorRegex = "^(get|is|set)(.+)$".r
}