package com.datastax.driver.spark.rdd

sealed trait ColumnSelector
case object AllColumns extends ColumnSelector
case class SomeColumns(columns: String*) extends ColumnSelector


