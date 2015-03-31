package com.datastax.spark.connector

import scala.language.implicitConversions

sealed trait ColumnSelector {
  def aliases: Map[String, String]
}

case object AllColumns extends ColumnSelector {
  override def aliases: Map[String, String] = Map.empty.withDefault(x => x)
}

case object PartitionKeyColumns extends ColumnSelector {
  override def aliases: Map[String, String] = Map.empty.withDefault(x => x)
}

case class SomeColumns(columns: SelectableColumnRef*) extends ColumnSelector {
  override def aliases: Map[String, String] = columns.map {
    case ref => (ref.selectedAs, ref.selectedFromCassandraAs)
  }.toMap
}

object SomeColumns {
  @deprecated("Use com.datastax.spark.connector.rdd.SomeColumns instead of Seq", "1.0")
  implicit def seqToSomeColumns(columns: Seq[String]): SomeColumns =
    SomeColumns(columns.map(x => x: NamedColumnRef): _*)
}


