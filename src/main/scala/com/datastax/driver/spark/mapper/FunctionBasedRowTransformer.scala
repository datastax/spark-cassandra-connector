package com.datastax.driver.spark.mapper

import com.datastax.driver.core.Row
import com.datastax.driver.spark.rdd.CassandraRow
import com.datastax.driver.spark.types.TypeConverter

// The below fragment may look very repetitive and copy-pasted,
// however there is no other way to code this for functions of different arity
// while preserving good type-safety.

class FunctionBasedRowTransformer1[R, A0](f: A0 => R)(
  implicit a0c: TypeConverter[A0]) extends RowTransformer[R] with ThisRowTransformerAsFactory[R] {

  override def transform(row: Row, columnNames: Array[String]) =
    f(a0c.convert(CassandraRow.get(row, 0)))

  override def columnCount = Some(1)
  override def columnNames = None
}

class FunctionBasedRowTransformer2[R, A0, A1](f: (A0, A1) => R)(
  implicit
  a0c: TypeConverter[A0],
  a1c: TypeConverter[A1])
  extends RowTransformer[R] with ThisRowTransformerAsFactory[R] {

  override def transform(row: Row, columnNames: Array[String]) =
    f(
      a0c.convert(CassandraRow.get(row, 0)),
      a1c.convert(CassandraRow.get(row, 1))
    )

  override def columnCount = Some(2)
  override def columnNames = None
}

class FunctionBasedRowTransformer3[R, A0, A1, A2](f: (A0, A1, A2) => R)(
  implicit
  a0c: TypeConverter[A0],
  a1c: TypeConverter[A1],
  a2c: TypeConverter[A2])
  extends RowTransformer[R] with ThisRowTransformerAsFactory[R] {

  override def transform(row: Row, columnNames: Array[String]) =
    f(
      a0c.convert(CassandraRow.get(row, 0)),
      a1c.convert(CassandraRow.get(row, 1)),
      a2c.convert(CassandraRow.get(row, 2)))

  override def columnCount = Some(3)
  override def columnNames = None
}

class FunctionBasedRowTransformer4[R, A0, A1, A2, A3](f: (A0, A1, A2, A3) => R)(
  implicit
  a0c: TypeConverter[A0],
  a1c: TypeConverter[A1],
  a2c: TypeConverter[A2],
  a3c: TypeConverter[A3])
  extends RowTransformer[R] with ThisRowTransformerAsFactory[R] {

  override def transform(row: Row, columnNames: Array[String]) =
    f(
      a0c.convert(CassandraRow.get(row, 0)),
      a1c.convert(CassandraRow.get(row, 1)),
      a2c.convert(CassandraRow.get(row, 2)),
      a3c.convert(CassandraRow.get(row, 3))
    )

  override def columnCount = Some(4)
  override def columnNames = None
}

class FunctionBasedRowTransformer5[R, A0, A1, A2, A3, A4](f: (A0, A1, A2, A3, A4) => R)(
  implicit
  a0c: TypeConverter[A0],
  a1c: TypeConverter[A1],
  a2c: TypeConverter[A2],
  a3c: TypeConverter[A3],
  a4c: TypeConverter[A4])
  extends RowTransformer[R] with ThisRowTransformerAsFactory[R] {

  override def transform(row: Row, columnNames: Array[String]) =
    f(
      a0c.convert(CassandraRow.get(row, 0)),
      a1c.convert(CassandraRow.get(row, 1)),
      a2c.convert(CassandraRow.get(row, 2)),
      a3c.convert(CassandraRow.get(row, 3)),
      a4c.convert(CassandraRow.get(row, 4))
    )

  override def columnCount = Some(5)
  override def columnNames = None
}

class FunctionBasedRowTransformer6[R, A0, A1, A2, A3, A4, A5](f: (A0, A1, A2, A3, A4, A5) => R)(
  implicit
  a0c: TypeConverter[A0],
  a1c: TypeConverter[A1],
  a2c: TypeConverter[A2],
  a3c: TypeConverter[A3],
  a4c: TypeConverter[A4],
  a5c: TypeConverter[A5])
  extends RowTransformer[R] with ThisRowTransformerAsFactory[R] {

  override def transform(row: Row, columnNames: Array[String]) =
    f(
      a0c.convert(CassandraRow.get(row, 0)),
      a1c.convert(CassandraRow.get(row, 1)),
      a2c.convert(CassandraRow.get(row, 2)),
      a3c.convert(CassandraRow.get(row, 3)),
      a4c.convert(CassandraRow.get(row, 4)),
      a5c.convert(CassandraRow.get(row, 5))
    )

  override def columnCount = Some(6)
  override def columnNames = None
}

class FunctionBasedRowTransformer7[R, A0, A1, A2, A3, A4, A5, A6](f: (A0, A1, A2, A3, A4, A5, A6) => R)(
  implicit
  a0c: TypeConverter[A0],
  a1c: TypeConverter[A1],
  a2c: TypeConverter[A2],
  a3c: TypeConverter[A3],
  a4c: TypeConverter[A4],
  a5c: TypeConverter[A5],
  a6c: TypeConverter[A6])
  extends RowTransformer[R] with ThisRowTransformerAsFactory[R] {

  override def transform(row: Row, columnNames: Array[String]) =
    f(
      a0c.convert(CassandraRow.get(row, 0)),
      a1c.convert(CassandraRow.get(row, 1)),
      a2c.convert(CassandraRow.get(row, 2)),
      a3c.convert(CassandraRow.get(row, 3)),
      a4c.convert(CassandraRow.get(row, 4)),
      a5c.convert(CassandraRow.get(row, 5)),
      a6c.convert(CassandraRow.get(row, 6))
    )

  override def columnCount = Some(7)
  override def columnNames = None
}

class FunctionBasedRowTransformer8[R, A0, A1, A2, A3, A4, A5, A6, A7]
(f: (A0, A1, A2, A3, A4, A5, A6, A7) => R)(
  implicit
  a0c: TypeConverter[A0],
  a1c: TypeConverter[A1],
  a2c: TypeConverter[A2],
  a3c: TypeConverter[A3],
  a4c: TypeConverter[A4],
  a5c: TypeConverter[A5],
  a6c: TypeConverter[A6],
  a7c: TypeConverter[A7])
  extends RowTransformer[R] with ThisRowTransformerAsFactory[R] {

  override def transform(row: Row, columnNames: Array[String]) =
    f(
      a0c.convert(CassandraRow.get(row, 0)),
      a1c.convert(CassandraRow.get(row, 1)),
      a2c.convert(CassandraRow.get(row, 2)),
      a3c.convert(CassandraRow.get(row, 3)),
      a4c.convert(CassandraRow.get(row, 4)),
      a5c.convert(CassandraRow.get(row, 5)),
      a6c.convert(CassandraRow.get(row, 6)),
      a7c.convert(CassandraRow.get(row, 7))
    )

  override def columnCount = Some(8)
  override def columnNames = None
}

class FunctionBasedRowTransformer9[R, A0, A1, A2, A3, A4, A5, A6, A7, A8]
(f: (A0, A1, A2, A3, A4, A5, A6, A7, A8) => R)(
  implicit
  a0c: TypeConverter[A0],
  a1c: TypeConverter[A1],
  a2c: TypeConverter[A2],
  a3c: TypeConverter[A3],
  a4c: TypeConverter[A4],
  a5c: TypeConverter[A5],
  a6c: TypeConverter[A6],
  a7c: TypeConverter[A7],
  a8c: TypeConverter[A8])
  extends RowTransformer[R] with ThisRowTransformerAsFactory[R] {

  override def transform(row: Row, columnNames: Array[String]) =
    f(
      a0c.convert(CassandraRow.get(row, 0)),
      a1c.convert(CassandraRow.get(row, 1)),
      a2c.convert(CassandraRow.get(row, 2)),
      a3c.convert(CassandraRow.get(row, 3)),
      a4c.convert(CassandraRow.get(row, 4)),
      a5c.convert(CassandraRow.get(row, 5)),
      a6c.convert(CassandraRow.get(row, 6)),
      a7c.convert(CassandraRow.get(row, 7)),
      a8c.convert(CassandraRow.get(row, 8))
    )

  override def columnCount = Some(9)
  override def columnNames = None
}

class FunctionBasedRowTransformer10[R, A0, A1, A2, A3, A4, A5, A6, A7, A8, A9]
(f: (A0, A1, A2, A3, A4, A5, A6, A7, A8, A9) => R)(
  implicit
  a0c: TypeConverter[A0],
  a1c: TypeConverter[A1],
  a2c: TypeConverter[A2],
  a3c: TypeConverter[A3],
  a4c: TypeConverter[A4],
  a5c: TypeConverter[A5],
  a6c: TypeConverter[A6],
  a7c: TypeConverter[A7],
  a8c: TypeConverter[A8],
  a9c: TypeConverter[A9])
  extends RowTransformer[R] with ThisRowTransformerAsFactory[R] {

  override def transform(row: Row, columnNames: Array[String]) =
    f(
      a0c.convert(CassandraRow.get(row, 0)),
      a1c.convert(CassandraRow.get(row, 1)),
      a2c.convert(CassandraRow.get(row, 2)),
      a3c.convert(CassandraRow.get(row, 3)),
      a4c.convert(CassandraRow.get(row, 4)),
      a5c.convert(CassandraRow.get(row, 5)),
      a6c.convert(CassandraRow.get(row, 6)),
      a7c.convert(CassandraRow.get(row, 7)),
      a8c.convert(CassandraRow.get(row, 8)),
      a9c.convert(CassandraRow.get(row, 9))
    )

  override def columnCount = Some(10)
  override def columnNames = None
}

class FunctionBasedRowTransformer11[R, A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10]
(f: (A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10) => R)(
  implicit
  a0c: TypeConverter[A0],
  a1c: TypeConverter[A1],
  a2c: TypeConverter[A2],
  a3c: TypeConverter[A3],
  a4c: TypeConverter[A4],
  a5c: TypeConverter[A5],
  a6c: TypeConverter[A6],
  a7c: TypeConverter[A7],
  a8c: TypeConverter[A8],
  a9c: TypeConverter[A9],
  a10c: TypeConverter[A10])
  extends RowTransformer[R] with ThisRowTransformerAsFactory[R] {

  override def transform(row: Row, columnNames: Array[String]) =
    f(
      a0c.convert(CassandraRow.get(row, 0)),
      a1c.convert(CassandraRow.get(row, 1)),
      a2c.convert(CassandraRow.get(row, 2)),
      a3c.convert(CassandraRow.get(row, 3)),
      a4c.convert(CassandraRow.get(row, 4)),
      a5c.convert(CassandraRow.get(row, 5)),
      a6c.convert(CassandraRow.get(row, 6)),
      a7c.convert(CassandraRow.get(row, 7)),
      a8c.convert(CassandraRow.get(row, 8)),
      a9c.convert(CassandraRow.get(row, 9)),
      a10c.convert(CassandraRow.get(row, 10))
    )

  override def columnCount = Some(11)
  override def columnNames = None
}

class FunctionBasedRowTransformer12[R, A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11]
(f: (A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11) => R)(
  implicit
  a0c: TypeConverter[A0],
  a1c: TypeConverter[A1],
  a2c: TypeConverter[A2],
  a3c: TypeConverter[A3],
  a4c: TypeConverter[A4],
  a5c: TypeConverter[A5],
  a6c: TypeConverter[A6],
  a7c: TypeConverter[A7],
  a8c: TypeConverter[A8],
  a9c: TypeConverter[A9],
  a10c: TypeConverter[A10],
  a11c: TypeConverter[A11])
  extends RowTransformer[R] with ThisRowTransformerAsFactory[R] {

  override def transform(row: Row, columnNames: Array[String]) =
    f(
      a0c.convert(CassandraRow.get(row, 0)),
      a1c.convert(CassandraRow.get(row, 1)),
      a2c.convert(CassandraRow.get(row, 2)),
      a3c.convert(CassandraRow.get(row, 3)),
      a4c.convert(CassandraRow.get(row, 4)),
      a5c.convert(CassandraRow.get(row, 5)),
      a6c.convert(CassandraRow.get(row, 6)),
      a7c.convert(CassandraRow.get(row, 7)),
      a8c.convert(CassandraRow.get(row, 8)),
      a9c.convert(CassandraRow.get(row, 9)),
      a10c.convert(CassandraRow.get(row, 10)),
      a11c.convert(CassandraRow.get(row, 11))
    )

  override def columnCount = Some(12)
  override def columnNames = None
}

