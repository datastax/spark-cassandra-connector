package com.datastax.spark.connector.rdd

private[connector] trait ColumnSelector
case object AllColumns extends ColumnSelector
case class SomeColumns(columns: String*) extends ColumnSelector


