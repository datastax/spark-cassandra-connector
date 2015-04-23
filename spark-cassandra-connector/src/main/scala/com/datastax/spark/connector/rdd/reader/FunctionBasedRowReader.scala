package com.datastax.spark.connector.rdd.reader

import com.datastax.driver.core.{ProtocolVersion, Row}
import com.datastax.spark.connector.GettableData
import com.datastax.spark.connector.types.TypeConverter

import scala.reflect.ClassTag

// The below fragment may look very repetitive and copy-pasted,
// however there is no other way to code this for functions of different arity
// while preserving good type-safety.

trait FunctionBasedRowReader[R] extends RowReader[R] with ThisRowReaderAsFactory[R] {
  def ct: ClassTag[R]
  override val targetClass: Class[R] = ct.runtimeClass.asInstanceOf[Class[R]]
}

class FunctionBasedRowReader1[R, A0](f: A0 => R)(
  implicit a0c: TypeConverter[A0], @transient override val ct: ClassTag[R]) extends FunctionBasedRowReader[R] {

  override def read(row: Row, columnNames: Array[String])(implicit protocolVersion: ProtocolVersion) =
    f(a0c.convert(GettableData.get(row, 0)))

  override def requiredColumns = Some(1)
  override def columnNames = None
}

class FunctionBasedRowReader2[R, A0, A1](f: (A0, A1) => R)(
  implicit
  a0c: TypeConverter[A0],
  a1c: TypeConverter[A1],
  @transient override val ct: ClassTag[R])
  extends FunctionBasedRowReader[R] {

  override def read(row: Row, columnNames: Array[String])(implicit protocolVersion: ProtocolVersion) =
    f(
      a0c.convert(GettableData.get(row, 0)),
      a1c.convert(GettableData.get(row, 1))
    )

  override def requiredColumns = Some(2)
  override def columnNames = None
}

class FunctionBasedRowReader3[R, A0, A1, A2](f: (A0, A1, A2) => R)(
  implicit
  a0c: TypeConverter[A0],
  a1c: TypeConverter[A1],
  a2c: TypeConverter[A2],
  @transient override val ct: ClassTag[R])
  extends FunctionBasedRowReader[R] {

  override def read(row: Row, columnNames: Array[String])(implicit protocolVersion: ProtocolVersion) =
    f(
      a0c.convert(GettableData.get(row, 0)),
      a1c.convert(GettableData.get(row, 1)),
      a2c.convert(GettableData.get(row, 2)))

  override def requiredColumns = Some(3)
  override def columnNames = None

}

class FunctionBasedRowReader4[R, A0, A1, A2, A3](f: (A0, A1, A2, A3) => R)(
  implicit
  a0c: TypeConverter[A0],
  a1c: TypeConverter[A1],
  a2c: TypeConverter[A2],
  a3c: TypeConverter[A3],
  @transient override val ct: ClassTag[R])
  extends FunctionBasedRowReader[R] {

  override def read(row: Row, columnNames: Array[String])(implicit protocolVersion: ProtocolVersion) =
    f(
      a0c.convert(GettableData.get(row, 0)),
      a1c.convert(GettableData.get(row, 1)),
      a2c.convert(GettableData.get(row, 2)),
      a3c.convert(GettableData.get(row, 3))
    )

  override def requiredColumns = Some(4)
  override def columnNames = None

}

class FunctionBasedRowReader5[R, A0, A1, A2, A3, A4](f: (A0, A1, A2, A3, A4) => R)(
  implicit
  a0c: TypeConverter[A0],
  a1c: TypeConverter[A1],
  a2c: TypeConverter[A2],
  a3c: TypeConverter[A3],
  a4c: TypeConverter[A4],
  @transient override val ct: ClassTag[R])
  extends FunctionBasedRowReader[R] {

  override def read(row: Row, columnNames: Array[String])(implicit protocolVersion: ProtocolVersion) =
    f(
      a0c.convert(GettableData.get(row, 0)),
      a1c.convert(GettableData.get(row, 1)),
      a2c.convert(GettableData.get(row, 2)),
      a3c.convert(GettableData.get(row, 3)),
      a4c.convert(GettableData.get(row, 4))
    )

  override def requiredColumns = Some(5)
  override def columnNames = None

}

class FunctionBasedRowReader6[R, A0, A1, A2, A3, A4, A5](f: (A0, A1, A2, A3, A4, A5) => R)(
  implicit
  a0c: TypeConverter[A0],
  a1c: TypeConverter[A1],
  a2c: TypeConverter[A2],
  a3c: TypeConverter[A3],
  a4c: TypeConverter[A4],
  a5c: TypeConverter[A5],
  @transient override val ct: ClassTag[R])
  extends FunctionBasedRowReader[R] {

  override def read(row: Row, columnNames: Array[String])(implicit protocolVersion: ProtocolVersion) =
    f(
      a0c.convert(GettableData.get(row, 0)),
      a1c.convert(GettableData.get(row, 1)),
      a2c.convert(GettableData.get(row, 2)),
      a3c.convert(GettableData.get(row, 3)),
      a4c.convert(GettableData.get(row, 4)),
      a5c.convert(GettableData.get(row, 5))
    )

  override def requiredColumns = Some(6)
  override def columnNames = None
}

class FunctionBasedRowReader7[R, A0, A1, A2, A3, A4, A5, A6](f: (A0, A1, A2, A3, A4, A5, A6) => R)(
  implicit
  a0c: TypeConverter[A0],
  a1c: TypeConverter[A1],
  a2c: TypeConverter[A2],
  a3c: TypeConverter[A3],
  a4c: TypeConverter[A4],
  a5c: TypeConverter[A5],
  a6c: TypeConverter[A6],
  @transient override val ct: ClassTag[R])
  extends FunctionBasedRowReader[R] {

  override def read(row: Row, columnNames: Array[String])(implicit protocolVersion: ProtocolVersion) =
    f(
      a0c.convert(GettableData.get(row, 0)),
      a1c.convert(GettableData.get(row, 1)),
      a2c.convert(GettableData.get(row, 2)),
      a3c.convert(GettableData.get(row, 3)),
      a4c.convert(GettableData.get(row, 4)),
      a5c.convert(GettableData.get(row, 5)),
      a6c.convert(GettableData.get(row, 6))
    )

  override def requiredColumns = Some(7)
  override def columnNames = None
}

class FunctionBasedRowReader8[R, A0, A1, A2, A3, A4, A5, A6, A7]
(f: (A0, A1, A2, A3, A4, A5, A6, A7) => R)(
  implicit
  a0c: TypeConverter[A0],
  a1c: TypeConverter[A1],
  a2c: TypeConverter[A2],
  a3c: TypeConverter[A3],
  a4c: TypeConverter[A4],
  a5c: TypeConverter[A5],
  a6c: TypeConverter[A6],
  a7c: TypeConverter[A7],
  @transient override val ct: ClassTag[R])
  extends FunctionBasedRowReader[R] {

  override def read(row: Row, columnNames: Array[String])(implicit protocolVersion: ProtocolVersion) =
    f(
      a0c.convert(GettableData.get(row, 0)),
      a1c.convert(GettableData.get(row, 1)),
      a2c.convert(GettableData.get(row, 2)),
      a3c.convert(GettableData.get(row, 3)),
      a4c.convert(GettableData.get(row, 4)),
      a5c.convert(GettableData.get(row, 5)),
      a6c.convert(GettableData.get(row, 6)),
      a7c.convert(GettableData.get(row, 7))
    )

  override def requiredColumns = Some(8)
  override def columnNames = None
}

class FunctionBasedRowReader9[R, A0, A1, A2, A3, A4, A5, A6, A7, A8]
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
  a8c: TypeConverter[A8],
  @transient override val ct: ClassTag[R])
  extends FunctionBasedRowReader[R] {

  override def read(row: Row, columnNames: Array[String])(implicit protocolVersion: ProtocolVersion) =
    f(
      a0c.convert(GettableData.get(row, 0)),
      a1c.convert(GettableData.get(row, 1)),
      a2c.convert(GettableData.get(row, 2)),
      a3c.convert(GettableData.get(row, 3)),
      a4c.convert(GettableData.get(row, 4)),
      a5c.convert(GettableData.get(row, 5)),
      a6c.convert(GettableData.get(row, 6)),
      a7c.convert(GettableData.get(row, 7)),
      a8c.convert(GettableData.get(row, 8))
    )

  override def requiredColumns = Some(9)
  override def columnNames = None
}

class FunctionBasedRowReader10[R, A0, A1, A2, A3, A4, A5, A6, A7, A8, A9]
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
  a9c: TypeConverter[A9],
  @transient override val ct: ClassTag[R])
  extends FunctionBasedRowReader[R] {

  override def read(row: Row, columnNames: Array[String])(implicit protocolVersion: ProtocolVersion) =
    f(
      a0c.convert(GettableData.get(row, 0)),
      a1c.convert(GettableData.get(row, 1)),
      a2c.convert(GettableData.get(row, 2)),
      a3c.convert(GettableData.get(row, 3)),
      a4c.convert(GettableData.get(row, 4)),
      a5c.convert(GettableData.get(row, 5)),
      a6c.convert(GettableData.get(row, 6)),
      a7c.convert(GettableData.get(row, 7)),
      a8c.convert(GettableData.get(row, 8)),
      a9c.convert(GettableData.get(row, 9))
    )

  override def requiredColumns = Some(10)
  override def columnNames = None
}

class FunctionBasedRowReader11[R, A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10]
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
  a10c: TypeConverter[A10],
  @transient override val ct: ClassTag[R])
  extends FunctionBasedRowReader[R] {

  override def read(row: Row, columnNames: Array[String])(implicit protocolVersion: ProtocolVersion) =
    f(
      a0c.convert(GettableData.get(row, 0)),
      a1c.convert(GettableData.get(row, 1)),
      a2c.convert(GettableData.get(row, 2)),
      a3c.convert(GettableData.get(row, 3)),
      a4c.convert(GettableData.get(row, 4)),
      a5c.convert(GettableData.get(row, 5)),
      a6c.convert(GettableData.get(row, 6)),
      a7c.convert(GettableData.get(row, 7)),
      a8c.convert(GettableData.get(row, 8)),
      a9c.convert(GettableData.get(row, 9)),
      a10c.convert(GettableData.get(row, 10))
    )

  override def requiredColumns = Some(11)
  override def columnNames = None
}

class FunctionBasedRowReader12[R, A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11]
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
  a11c: TypeConverter[A11],
  @transient override val ct: ClassTag[R])
  extends FunctionBasedRowReader[R] {

  override def read(row: Row, columnNames: Array[String])(implicit protocolVersion: ProtocolVersion) =
    f(
      a0c.convert(GettableData.get(row, 0)),
      a1c.convert(GettableData.get(row, 1)),
      a2c.convert(GettableData.get(row, 2)),
      a3c.convert(GettableData.get(row, 3)),
      a4c.convert(GettableData.get(row, 4)),
      a5c.convert(GettableData.get(row, 5)),
      a6c.convert(GettableData.get(row, 6)),
      a7c.convert(GettableData.get(row, 7)),
      a8c.convert(GettableData.get(row, 8)),
      a9c.convert(GettableData.get(row, 9)),
      a10c.convert(GettableData.get(row, 10)),
      a11c.convert(GettableData.get(row, 11))
    )

  override def requiredColumns = Some(12)
  override def columnNames = None
}

