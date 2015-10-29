package com.datastax.spark.connector.rdd.typeTests

import java.lang.Boolean

class BooleanTypeTest extends AbstractTypeTest[Boolean, Boolean] {
  override val typeName = "boolean"
  override val typeData: Seq[Boolean] = Seq(new Boolean(true))
  override val typeSet: Set[Boolean] = Set(new Boolean(true),new Boolean(false))
  override val typeMap1: Map[String, Boolean] = Map("Love" -> new Boolean(true), "Hate" -> new Boolean(false), "Scala" -> new Boolean(true))
  override val typeMap2: Map[Boolean, String] = Map(new Boolean(true) -> "Cara", new Boolean(false) -> "Sundance")

  override val addData: Seq[Boolean] = Seq(new Boolean(false))
  override val addSet: Set[Boolean] = Set(new Boolean(false),new Boolean(true))
  override val addMap1: Map[String, Boolean] = Map("DataStax" -> new Boolean(true), "DataBrix" -> new Boolean(true), "Hadoop" -> new Boolean(false))
  override val addMap2: Map[Boolean, String] = Map(new Boolean(false) -> "Oranges", new Boolean(true) -> "Apples")

  override def getDriverColumn(row: com.datastax.driver.core.Row, colName: String): Boolean = {
    row.getBool(colName)
  }

}

