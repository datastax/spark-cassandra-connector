package com.datastax.spark.connector.mapper

import com.datastax.spark.connector.cql.{StructDef, TableDef}

import scala.reflect.runtime.universe._
import scala.reflect.ClassTag


/** Produces [[ColumnMap]] objects that map class `T` properties to columns
  * in a given Cassandra table.
  *
  * You can associate a custom `ColumnMapper` object with any of your classes by
  * providing an implicit `ColumnMapper` in the companion object of the mapped class:
  * {{{
  *   CREATE TABLE kv(key int primary key, value text);
  * }}}
  * {{{
  *   case class KeyValue(k: Int, v: String)
  *
  *   object KeyValue {
  *     implicit val columnMapper =
  *       new DefaultColumnMapper[KeyValue](Map("k" -> "key", "v" -> "value"))
  *   }
  * }}}
  */
trait ColumnMapper[T] {
  def columnMap(struct: StructDef, aliasToColumnName: Map[String, String] = Map.empty): ColumnMap

  /** Provides a definition of the table that class `T` could be saved to. */
  def newTable(keyspaceName: String, tableName: String): TableDef

  def classTag: ClassTag[T]
}

/** Provides implicit [[ColumnMapper]] used for mapping all non-tuple classes. */
trait LowPriorityColumnMapper {
  implicit def defaultColumnMapper[T : ClassTag : TypeTag]: ColumnMapper[T] =
    new DefaultColumnMapper[T]
}

/** Provides implicit [[ColumnMapper]] objects used for mapping tuples. */
object ColumnMapper extends LowPriorityColumnMapper {

  implicit def tuple1ColumnMapper[A1 : TypeTag] =
    new TupleColumnMapper[Tuple1[A1]]

  implicit def tuple2ColumnMapper[A1 : TypeTag, A2 : TypeTag] =
    new TupleColumnMapper[(A1, A2)]

  implicit def tuple3ColumnMapper[A1 : TypeTag, A2 : TypeTag, A3 : TypeTag] =
    new TupleColumnMapper[(A1, A2, A3)]

  implicit def tuple4ColumnMapper[A1 : TypeTag, A2 : TypeTag, A3 : TypeTag, A4 : TypeTag] =
    new TupleColumnMapper[(A1, A2, A3, A4)]

  implicit def tuple5ColumnMapper[A1 : TypeTag, A2 : TypeTag, A3 : TypeTag, A4 : TypeTag, A5 : TypeTag] =
    new TupleColumnMapper[(A1, A2, A3, A4, A5)]

  implicit def tuple6ColumnMapper[A1 : TypeTag, A2 : TypeTag, A3 : TypeTag, A4 : TypeTag, A5 : TypeTag, A6 : TypeTag] =
    new TupleColumnMapper[(A1, A2, A3, A4, A5, A6)]

  implicit def tuple7ColumnMapper[
  A1 : TypeTag, A2 : TypeTag, A3 : TypeTag, A4 : TypeTag, A5 : TypeTag, A6 : TypeTag,
  A7 : TypeTag] =
    new TupleColumnMapper[(A1, A2, A3, A4, A5, A6, A7)]

  implicit def tuple8ColumnMapper[
  A1 : TypeTag, A2 : TypeTag, A3 : TypeTag, A4 : TypeTag, A5 : TypeTag, A6 : TypeTag,
  A7 : TypeTag, A8 : TypeTag] =
    new TupleColumnMapper[(A1, A2, A3, A4, A5, A6, A7, A8)]

  implicit def tuple9ColumnMapper[
  A1 : TypeTag, A2 : TypeTag, A3 : TypeTag, A4 : TypeTag, A5 : TypeTag, A6 : TypeTag,
  A7 : TypeTag, A8 : TypeTag, A9 : TypeTag] =
    new TupleColumnMapper[(A1, A2, A3, A4, A5, A6, A7, A8, A9)]

  implicit def tuple10ColumnMapper[
  A1 : TypeTag, A2 : TypeTag, A3 : TypeTag, A4 : TypeTag, A5 : TypeTag, A6 : TypeTag,
  A7 : TypeTag, A8 : TypeTag, A9 : TypeTag, A10 : TypeTag] =
    new TupleColumnMapper[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10)]

  implicit def tuple11ColumnMapper[
  A1 : TypeTag, A2 : TypeTag, A3 : TypeTag, A4 : TypeTag, A5 : TypeTag, A6 : TypeTag,
  A7 : TypeTag, A8 : TypeTag, A9 : TypeTag, A10 : TypeTag, A11 : TypeTag] =
    new TupleColumnMapper[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11)]

  implicit def tuple12ColumnMapper[
  A1 : TypeTag, A2 : TypeTag, A3 : TypeTag, A4 : TypeTag, A5 : TypeTag, A6 : TypeTag,
  A7 : TypeTag, A8 : TypeTag, A9 : TypeTag, A10 : TypeTag, A11 : TypeTag,
  A12 : TypeTag] =
    new TupleColumnMapper[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12)]

  implicit def tuple13ColumnMapper[
  A1 : TypeTag, A2 : TypeTag, A3 : TypeTag, A4 : TypeTag, A5 : TypeTag, A6 : TypeTag,
  A7 : TypeTag, A8 : TypeTag, A9 : TypeTag, A10 : TypeTag, A11 : TypeTag,
  A12 : TypeTag, A13 : TypeTag] =
    new TupleColumnMapper[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13)]


  implicit def tuple14ColumnMapper[
  A1 : TypeTag, A2 : TypeTag, A3 : TypeTag, A4 : TypeTag, A5 : TypeTag, A6 : TypeTag,
  A7 : TypeTag, A8 : TypeTag, A9 : TypeTag, A10 : TypeTag, A11 : TypeTag,
  A12 : TypeTag, A13 : TypeTag, A14 : TypeTag] =
    new TupleColumnMapper[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14)]

  implicit def tuple15ColumnMapper[
  A1 : TypeTag, A2 : TypeTag, A3 : TypeTag, A4 : TypeTag, A5 : TypeTag, A6 : TypeTag,
  A7 : TypeTag, A8 : TypeTag, A9 : TypeTag, A10 : TypeTag, A11 : TypeTag,
  A12 : TypeTag, A13 : TypeTag, A14 : TypeTag, A15 : TypeTag] =
    new TupleColumnMapper[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15)]

  implicit def tuple16ColumnMapper[
  A1 : TypeTag, A2 : TypeTag, A3 : TypeTag, A4 : TypeTag, A5 : TypeTag, A6 : TypeTag,
  A7 : TypeTag, A8 : TypeTag, A9 : TypeTag, A10 : TypeTag, A11 : TypeTag,
  A12 : TypeTag, A13 : TypeTag, A14 : TypeTag, A15 : TypeTag, A16 : TypeTag] =
    new TupleColumnMapper[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16)]

  implicit def tuple17ColumnMapper[
  A1 : TypeTag, A2 : TypeTag, A3 : TypeTag, A4 : TypeTag, A5 : TypeTag, A6 : TypeTag,
  A7 : TypeTag, A8 : TypeTag, A9 : TypeTag, A10 : TypeTag, A11 : TypeTag,
  A12 : TypeTag, A13 : TypeTag, A14 : TypeTag, A15 : TypeTag, A16 : TypeTag,
  A17 : TypeTag] =
    new TupleColumnMapper[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17)]

  implicit def tuple18ColumnMapper[
  A1 : TypeTag, A2 : TypeTag, A3 : TypeTag, A4 : TypeTag, A5 : TypeTag, A6 : TypeTag,
  A7 : TypeTag, A8 : TypeTag, A9 : TypeTag, A10 : TypeTag, A11 : TypeTag,
  A12 : TypeTag, A13 : TypeTag, A14 : TypeTag, A15 : TypeTag, A16 : TypeTag,
  A17 : TypeTag, A18 : TypeTag] =
    new TupleColumnMapper[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18)]

  implicit def tuple19ColumnMapper[
  A1 : TypeTag, A2 : TypeTag, A3 : TypeTag, A4 : TypeTag, A5 : TypeTag, A6 : TypeTag,
  A7 : TypeTag, A8 : TypeTag, A9 : TypeTag, A10 : TypeTag, A11 : TypeTag,
  A12 : TypeTag, A13 : TypeTag, A14 : TypeTag, A15 : TypeTag, A16 : TypeTag,
  A17 : TypeTag, A18 : TypeTag, A19 : TypeTag] =
    new TupleColumnMapper[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19)]

  implicit def tuple20ColumnMapper[
  A1 : TypeTag, A2 : TypeTag, A3 : TypeTag, A4 : TypeTag, A5 : TypeTag, A6 : TypeTag,
  A7 : TypeTag, A8 : TypeTag, A9 : TypeTag, A10 : TypeTag, A11 : TypeTag,
  A12 : TypeTag, A13 : TypeTag, A14 : TypeTag, A15 : TypeTag, A16 : TypeTag,
  A17 : TypeTag, A18 : TypeTag, A19 : TypeTag, A20 : TypeTag] =
    new TupleColumnMapper[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, A20)]

  implicit def tuple21ColumnMapper[
  A1 : TypeTag, A2 : TypeTag, A3 : TypeTag, A4 : TypeTag, A5 : TypeTag, A6 : TypeTag,
  A7 : TypeTag, A8 : TypeTag, A9 : TypeTag, A10 : TypeTag, A11 : TypeTag,
  A12 : TypeTag, A13 : TypeTag, A14 : TypeTag, A15 : TypeTag, A16 : TypeTag,
  A17 : TypeTag, A18 : TypeTag, A19 : TypeTag, A20 : TypeTag, A21 : TypeTag] =
    new TupleColumnMapper[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, A20, A21)]

  implicit def tuple22ColumnMapper[
  A1 : TypeTag, A2 : TypeTag, A3 : TypeTag, A4 : TypeTag, A5 : TypeTag, A6 : TypeTag,
  A7 : TypeTag, A8 : TypeTag, A9 : TypeTag, A10 : TypeTag, A11 : TypeTag,
  A12 : TypeTag, A13 : TypeTag, A14 : TypeTag, A15 : TypeTag, A16 : TypeTag,
  A17 : TypeTag, A18 : TypeTag, A19 : TypeTag, A20 : TypeTag, A21 : TypeTag, A22 : TypeTag] =
    new TupleColumnMapper[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, A20, A21, A22)]
}
