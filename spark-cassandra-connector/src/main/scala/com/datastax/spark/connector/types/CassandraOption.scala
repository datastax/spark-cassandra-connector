package com.datastax.spark.connector.types

/**
  * An object representing a column which will be skipped on insert.
  */
protected[connector] object Unset extends Serializable

/**
  * An Optional value with Cassandra in mind. There are three options
  * Value(value): Representing a value to be inserted from C*
  * Unset: Representing a value which should be skipped when writing to C*
  * Null: Representing a java `null`, treated as a delete in C* (or empty collection)
  */
sealed trait CassandraOption[+A] extends Product with Serializable

object CassandraOption {

  case class Value[+A](value: A) extends CassandraOption[A]
  case object Unset extends CassandraOption[Nothing]
  case object Null extends CassandraOption[Nothing]

  def apply[A](x: A): CassandraOption[A] = x match {
    case x: AnyRef if (x == null) => CassandraOption.Null
    case Unset => CassandraOption.Unset
    case x => CassandraOption.Value[A](x)
  }

  implicit def toScalaOption[A](cOption: CassandraOption[A]): Option[A] = cOption match {
    case CassandraOption.Unset | CassandraOption.Null => None
    case CassandraOption.Value(x) => Some(x)
  }

  def deleteIfNone[A](op: Option[A]): CassandraOption[A] = op match {
    case None => CassandraOption.Null
    case Some(x) => CassandraOption.Value(x)
  }

  def unsetIfNone[A](op: Option[A]): CassandraOption[A] = op match {
    case None => CassandraOption.Unset
    case Some(x) => CassandraOption.Value(x)
  }

}
