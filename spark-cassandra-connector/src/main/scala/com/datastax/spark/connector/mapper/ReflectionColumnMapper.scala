package com.datastax.spark.connector.mapper

import java.lang.reflect.{Constructor, Method}

import com.datastax.spark.connector.{ColumnRef, ColumnName}
import com.datastax.spark.connector.cql.StructDef
import com.datastax.spark.connector.rdd.reader.AnyObjectFactory

import scala.reflect.ClassTag

abstract class ReflectionColumnMapper[T : ClassTag] extends ColumnMapper[T] {

  import AnyObjectFactory._

  protected def isSetter(method: Method): Boolean
  protected def isGetter(method: Method): Boolean

  protected def setterToColumnName(
      setterName: String,
      columns: Map[String, ColumnRef]): Option[ColumnRef]

  protected def getterToColumnName(
      getterName: String,
      columns: Map[String, ColumnRef]): Option[ColumnRef]

  protected def constructorParamToColumnName(
      paramName: String,
      columns: Map[String, ColumnRef]): Option[ColumnRef]

  protected def allowsNull: Boolean

  private def columnRefByAliasName(selectedColumns: IndexedSeq[ColumnRef]): Map[String, ColumnRef] =
    (for (c <- selectedColumns) yield (c.selectedAs, c)).toMap

  private val cls = implicitly[ClassTag[T]].runtimeClass

  override def columnMapForReading(
      struct: StructDef,
      selectedColumns: IndexedSeq[ColumnRef]): ColumnMapForReading = {

    val columnByName = columnRefByAliasName(selectedColumns)

    def columnRefOrThrow(paramName: String) = {
      constructorParamToColumnName(paramName, columnByName).getOrElse {
        throw new IllegalArgumentException(
          s"Failed to map constructor parameter $paramName in $cls to a column of ${struct.name}")
      }
    }

    def columnsOf(ctor: Constructor[_]): Seq[ColumnRef] = {
      if (isNoArgsConstructor(ctor))
        Nil
      else {
        val paramNames = paranamer.lookupParameterNames(ctor)
        paramNames
          .filter(_ != "$_outer")
          .map(columnRefOrThrow)
      }
    }

    val constructor = columnsOf(resolveConstructor(cls))

    val setters: Map[String, ColumnRef] = {
      for {
        method <- cls.getMethods if isSetter(method)
        methodName = method.getName
        columnRef <- setterToColumnName(methodName, columnByName)
      } yield (methodName, columnRef)
    }.toMap

    new SimpleColumnMapForReading(constructor, setters, allowsNull)
  }

  override def columnMapForWriting(
      struct: StructDef,
      selectedColumns: IndexedSeq[ColumnRef]): ColumnMapForWriting = {

    val columnByName = columnRefByAliasName(selectedColumns)

    val getterMap: Map[String, ColumnRef] = {
      for {
        method <- cls.getMethods if isGetter(method)
        methodName = method.getName
        columnRef <- getterToColumnName(methodName, columnByName)
      } yield (methodName, columnRef)
    }.toMap

    // Check if we have all the required columns:
    val mappedColumns = getterMap.values.toSet
    val unmappedColumns = selectedColumns.filterNot(mappedColumns)
    require(unmappedColumns.isEmpty, s"Columns not found in $cls: [${unmappedColumns.mkString(", ")}]")

    new SimpleColumnMapForWriting(getterMap)
  }
}
