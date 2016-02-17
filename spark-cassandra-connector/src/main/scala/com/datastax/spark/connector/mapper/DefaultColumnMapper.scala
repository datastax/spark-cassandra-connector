package com.datastax.spark.connector.mapper

import org.apache.spark.sql.catalyst.ReflectionLock.SparkReflectionLock

import com.datastax.spark.connector.{ColumnRef, ColumnName}
import com.datastax.spark.connector.cql.{StructDef, ColumnDef, RegularColumn, PartitionKeyColumn, TableDef}
import com.datastax.spark.connector.types.ColumnType
import com.datastax.spark.connector.util.ReflectionUtil

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.{typeOf, Type, TypeTag}
import scala.util.{Success, Try}

/** A [[ColumnMapper]] that assumes camel case naming convention for property accessors and constructor
  * names and underscore naming convention for column names.
  *
  * Example mapping:
  * {{{
  *   case class User(
  *     login: String,         // mapped to "login" column
  *     emailAddress: String   // mapped to "email_address" column
  *     emailAddress2: String  // mapped to "email_address_2" column
  *   )
  * }}}
  *
  * Additionally, it is possible to name columns exactly the same as property names (case-sensitive):
  * {{{
  *   case class TaxPayer(
  *     TIN: String            // mapped to "TIN" column
  *   )
  * }}}
  *
  * @param columnNameOverride maps property names to column names; use it to override default mapping
  *                           for some properties
  */
class DefaultColumnMapper[T : TypeTag](columnNameOverride: Map[String, String] = Map.empty)
  extends ColumnMapper[T] {

  import com.datastax.spark.connector.mapper.DefaultColumnMapper._

  private def setterNameToPropertyName(str: String) =
    str.substring(0, str.length - SetterSuffix.length)

  private val tpe = SparkReflectionLock.synchronized(implicitly[TypeTag[T]].tpe)
  private val constructorParams = ReflectionUtil.constructorParams(tpe)
  private val getters = ReflectionUtil.getters(tpe)
  private val setters = ReflectionUtil.setters(tpe)


  private def resolve(name: String, columns: Map[String, ColumnRef]): Option[ColumnRef] = {
    val overridenName = columnNameOverride.getOrElse(name, name)
    ColumnMapperConvention.columnForProperty(overridenName, columns)
  }

  def ctorParamToColumnName(paramName: String, columns: Map[String, ColumnRef]): Option[ColumnRef] =
    resolve(paramName, columns)
  
  def getterToColumnName(getterName: String, columns: Map[String, ColumnRef]): Option[ColumnRef] =
    resolve(getterName, columns)

  def setterToColumnName(setterName: String, columns: Map[String, ColumnRef]): Option[ColumnRef] = {
    val propertyName = setterNameToPropertyName(setterName)
    resolve(propertyName, columns)
  }

  private def columnByName(selectedColumns: IndexedSeq[ColumnRef]): Map[String, ColumnRef] =
    (for (c <- selectedColumns) yield (c.selectedAs, c)).toMap

  override def columnMapForReading(
      struct: StructDef,
      selectedColumns: IndexedSeq[ColumnRef]): ColumnMapForReading = {
    
    val columns = columnByName(selectedColumns)

    val constructor =
      for ((paramName, _) <- constructorParams) yield {
        val column = ctorParamToColumnName(paramName, columns)
        column.getOrElse(throw new IllegalArgumentException(
          s"Failed to map constructor parameter $paramName in $tpe to a column of ${struct.name}"))
      }

    val setterMap = {
      for {
        (setterName, _) <- setters
        columnRef <- setterToColumnName(setterName, columns)
      } yield (setterName, columnRef)
    }.toMap
    
    SimpleColumnMapForReading(constructor, setterMap, allowsNull = false)
  }

  override def columnMapForWriting(
      struct: StructDef, 
      selectedColumns: IndexedSeq[ColumnRef]): ColumnMapForWriting = {

    val columns = columnByName(selectedColumns)

    val getterMap = {
      for {
        (getterName, _) <- getters
        columnRef <- getterToColumnName(getterName, columns)
      } yield (getterName, columnRef)
    }.toMap

    // Check if we have all the required columns:
    val mappedColumns = getterMap.values.toSet
    val unmappedColumns = selectedColumns.filterNot(mappedColumns)
    require(unmappedColumns.isEmpty, s"Columns not found in $tpe: [${unmappedColumns.mkString(", ")}]")

    SimpleColumnMapForWriting(getterMap)
  }
  
  private def inheritedScalaGetters: Seq[(String, Type)] = {
    for {
      bc <- tpe.baseClasses if bc.fullName.startsWith("scala.")
      tpe = bc.typeSignature
      getter <- ReflectionUtil.getters(tpe)
    } yield getter
  }

  override def newTable(keyspaceName: String, tableName: String): TableDef = {
    // filter out inherited scala getters, because they are very likely
    // not the properties users want to map
    val inheritedScalaGetterNames = inheritedScalaGetters.map(_._1)
    val paramNames = constructorParams.map(_._1)
    val getterNames = getters.map(_._1).filterNot(inheritedScalaGetterNames.toSet.contains)
    val setterNames = setters.map(_._1).map(setterNameToPropertyName)
    val propertyNames = (paramNames ++ getterNames ++ setterNames)
      .distinct
      .filterNot(_.contains("$"))  // ignore any properties generated by Scala compiler

    // pick only those properties which we know Cassandra data type for:
    val getterTypes = getters.toMap
    val mappableProperties = propertyNames
        .map { name => (name, getterTypes(name)) }
        .map { case (name, tpe) => (name, Try(ColumnType.fromScalaType(tpe))) }
        .collect { case (name, Success(columnType)) => (name, columnType) }

    require(
      mappableProperties.nonEmpty,
      "No mappable properties found in class: " + tpe.toString)

    val columns =
      for ((property, i) <- mappableProperties.zipWithIndex) yield {
        val propertyName = property._1
        val columnType = property._2
        val columnName = ColumnMapperConvention.camelCaseToUnderscore(propertyName)
        val columnRole = if (i == 0) PartitionKeyColumn else RegularColumn
        ColumnDef(columnName, columnRole, columnType)
      }
    TableDef(keyspaceName, tableName, Seq(columns.head), Seq.empty, columns.tail)
  }

}

object DefaultColumnMapper {
  private val SetterSuffix: String = "_$eq"
}
