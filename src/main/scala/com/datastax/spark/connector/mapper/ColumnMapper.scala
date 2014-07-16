package com.datastax.spark.connector.mapper

import com.datastax.spark.connector.cql.TableDef

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
trait ColumnMapper[T] extends Serializable {
  def columnMap(tableDef: TableDef): ColumnMap
}

/** Provides implicit [[ColumnMapper]] used for mapping all non-tuple classes. */
trait LowPriorityColumnMapper {
  implicit def defaultColumnMapper[T : ClassTag]: ColumnMapper[T] =
    new DefaultColumnMapper[T]
}

/** Provides implicit [[ColumnMapper]] objects used for mapping tuples. */
object ColumnMapper extends LowPriorityColumnMapper {

  implicit def tuple2ColumnMapper[A1, A2] = 
    new TupleColumnMapper[(A1, A2)]
  implicit def tuple3ColumnMapper[A1, A2, A3] =
    new TupleColumnMapper[(A1, A2, A3)]
  implicit def tuple4ColumnMapper[A1, A2, A3, A4] =
    new TupleColumnMapper[(A1, A2, A3, A4)]
  implicit def tuple5ColumnMapper[A1, A2, A3, A4, A5] =
    new TupleColumnMapper[(A1, A2, A3, A4, A5)]
  implicit def tuple6ColumnMapper[A1, A2, A3, A4, A5, A6] =
    new TupleColumnMapper[(A1, A2, A3, A4, A5, A6)]
  implicit def tuple7ColumnMapper[A1, A2, A3, A4, A5, A6, A7] =
    new TupleColumnMapper[(A1, A2, A3, A4, A5, A6, A7)]
  implicit def tuple8ColumnMapper[A1, A2, A3, A4, A5, A6, A7, A8] =
    new TupleColumnMapper[(A1, A2, A3, A4, A5, A6, A7, A8)]
  implicit def tuple9ColumnMapper[A1, A2, A3, A4, A5, A6, A7, A8, A9] =
    new TupleColumnMapper[(A1, A2, A3, A4, A5, A6, A7, A8, A9)]
  implicit def tuple10ColumnMapper[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10] =
    new TupleColumnMapper[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10)]
  implicit def tuple11ColumnMapper[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11] =
    new TupleColumnMapper[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11)]
  implicit def tuple12ColumnMapper[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12] =
    new TupleColumnMapper[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12)]
  implicit def tuple13ColumnMapper[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13] =
    new TupleColumnMapper[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13)]
  implicit def tuple14ColumnMapper[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14] =
    new TupleColumnMapper[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14)]
  implicit def tuple15ColumnMapper[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15] =
    new TupleColumnMapper[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15)]
  implicit def tuple16ColumnMapper[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16] =
    new TupleColumnMapper[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16)]
  implicit def tuple17ColumnMapper[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17] =
    new TupleColumnMapper[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17)]
  implicit def tuple18ColumnMapper[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18] =
    new TupleColumnMapper[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18)]
  implicit def tuple19ColumnMapper[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19] =
    new TupleColumnMapper[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19)]
  implicit def tuple20ColumnMapper[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, A20] =
    new TupleColumnMapper[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, A20)]
  implicit def tuple21ColumnMapper[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, A20, A21] =
    new TupleColumnMapper[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, A20, A21)]
  implicit def tuple22ColumnMapper[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, A20, A21, A22] =
    new TupleColumnMapper[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, A20, A21, A22)]

}
