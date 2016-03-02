package com.datastax.spark.connector.util

case class ConfigParameter[T](
  val name: String,
  val section: String,
  val default: T,
  val description: String) extends DataFrameOption {

  override val sqlOptionName = name.replaceAll("\\.", "\\_")

  def option(value: Any): Map[String, String] = {
    require(value != null)
    Map(name -> value.toString)
  }

}
