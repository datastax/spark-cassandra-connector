package com.datastax.spark.connector.mapper

import scala.reflect.runtime.universe._

import org.apache.spark.sql.catalyst.ReflectionLock.SparkReflectionLock

import com.datastax.spark.connector.{ColumnName, ColumnRef}
import com.datastax.spark.connector.cql.{ColumnDef, PartitionKeyColumn, RegularColumn, StructDef, TableDef}
import com.datastax.spark.connector.types.ColumnType
import com.datastax.spark.connector.util.Reflect

class TupleColumnMapper[T : TypeTag] extends ColumnMapper[T] {

  val cls = SparkReflectionLock.synchronized { typeTag[T].mirror.runtimeClass(typeTag[T].tpe) }
  val ctorLength = cls.getConstructors()(0).getParameterTypes.length
  val methodNames = cls.getMethods.map(_.getName)

  override def columnMapForReading(
      struct: StructDef,
      selectedColumns: IndexedSeq[ColumnRef]): ColumnMapForReading = {
    
    require(
      ctorLength <= selectedColumns.length,
      s"Not enough columns selected from ${struct.name}. " +
        s"Only ${selectedColumns.length} column(s) were selected, but $ctorLength are required. " +
        s"Selected columns: [${selectedColumns.mkString(", ")}]")
    
    SimpleColumnMapForReading(
      constructor = selectedColumns.take(ctorLength),
      setters = Map.empty[String, ColumnRef],
      allowsNull = false)
  }
  
  override def columnMapForWriting(struct: StructDef, selectedColumns: IndexedSeq[ColumnRef]) = {
    val GetterRegex = "_([0-9]+)".r

    require(
      selectedColumns.forall(colName => colName.selectedAs == colName.columnName) ||
        selectedColumns.forall(colName => colName.selectedAs != colName.columnName),
      """No mixing of implicit and explicit column mapping when writing tuples
        |1. All columns must be un-aliased or aliased to themselves OR
        |2. Some columns aliased to fields (_1,_2 ...) but no columns are implicitly mapped"""
        .stripMargin
    )

    for (colName <- selectedColumns) {
      val columnName = colName.columnName
      val alias = colName.selectedAs
      if (alias != columnName && !methodNames.contains(alias))
        throw new IllegalArgumentException(
          s"""Found Alias: $alias
             |Tuple provided does not have a getter for that alias.'
             |Provided getters are ${methodNames.mkString(",")}""".stripMargin)
    }

    val aliasToRef = selectedColumns.map(colRef => colRef.selectedAs -> colRef).toMap

    //Implicit Mapping, Order of C* Columns == Tuple Field Order
    val getters = if (selectedColumns.forall(colRef => colRef.columnName == colRef.selectedAs)) {
      for (methodName @ GetterRegex(id) <- methodNames if id.toInt <= selectedColumns.length)
        yield (methodName, selectedColumns(id.toInt - 1))
    }.toMap
    else {
      //Explicit Mapping, Tuple field aliases used
      for (methodName @ GetterRegex(id) <- methodNames if aliasToRef.contains(methodName))
        yield (methodName, aliasToRef(methodName))
    }.toMap

    SimpleColumnMapForWriting(getters)
  }
  
  override def newTable(keyspaceName: String, tableName: String): TableDef = {
    val tpe = SparkReflectionLock.synchronized(implicitly[TypeTag[T]].tpe)
    val ctorSymbol = Reflect.constructor(tpe).asMethod
    val ctorMethod = ctorSymbol.typeSignatureIn(tpe).asInstanceOf[MethodType]
    val ctorParamTypes = ctorMethod.params.map(_.typeSignature)
    require(ctorParamTypes.nonEmpty, "Expected a constructor with at least one parameter")

    val columnTypes = ctorParamTypes.map(ColumnType.fromScalaType)
    val columns = {
      for ((columnType, i) <- columnTypes.zipWithIndex) yield {
        val columnName = "_" + (i + 1).toString
        val columnRole = if (i == 0) PartitionKeyColumn else RegularColumn
        ColumnDef(columnName, columnRole, columnType)
      }
    }
    TableDef(keyspaceName, tableName, Seq(columns.head), Seq.empty, columns.tail)
  }
}
