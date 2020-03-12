package com.datastax.spark.connector.util

trait DataFrameOption {
  val sqlOptionName: String

  def sqlOption(value: Any): Map[String, String] = {
    require(value != null)
    Map(sqlOptionName -> value.toString)
  }
}
