package com.datastax.spark.connector.mapper

import scala.reflect.runtime.universe._
import scala.util.matching.Regex

import com.datastax.spark.connector.ColumnRef
import com.datastax.spark.connector.cql.{ColumnDef, PartitionKeyColumn, RegularColumn, StructDef, TableDef}
import com.datastax.spark.connector.types.ColumnType
import com.datastax.spark.connector.util.Reflect

class TupleColumnMapper[T : TypeTag] extends ColumnMapper[T] {

  val cls = typeTag[T].mirror.runtimeClass(typeTag[T].tpe)
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
    val getters = 
      for (name @ GetterRegex(id) <- methodNames if id.toInt <= selectedColumns.length)
        yield (name, selectedColumns(id.toInt - 1))
    
    require(
      getters.length == selectedColumns.length,
      s"The number of columns ${selectedColumns.length} selected to write to ${struct.name} " +
        s"is higher than the size of the tuple ${getters.length}")
    
    SimpleColumnMapForWriting(getters.toMap)
  }
  
  override def newTable(keyspaceName: String, tableName: String): TableDef = {
    val tpe = TypeTag.synchronized(implicitly[TypeTag[T]].tpe)
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
