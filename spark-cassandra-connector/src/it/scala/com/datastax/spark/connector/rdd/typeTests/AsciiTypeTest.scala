package com.datastax.spark.connector.rdd.typeTests

class AsciiTypeTest extends AbstractTypeTest[String, String] {
  override val typeName = "ascii"
  override val typeData: Seq[String] = Seq("row1", "row2", "row3", "row4", "row5")
  override val typeSet: Set[String] = Set("elementA", "elementB", "elementC")
  override val typeMap1: Map[String, String] = Map("key1" -> "val1", "key2" -> "val2", "key3" -> "val3")
  override val typeMap2: Map[String, String] = Map("key1" -> "val1", "key2" -> "val2", "key3" -> "val3")

  override val addData: Seq[String] = Seq("row6", "row7", "row8", "row9", "row10")
  override val addSet: Set[String] = Set("elementD", "elementE", "elementF")
  override val addMap1: Map[String, String] = Map("key4" -> "val4", "key5" -> "val5", "key3" -> "val6")
  override val addMap2: Map[String, String] = Map("key4" -> "val4", "key5" -> "val5", "key3" -> "val6")


  override def getDriverColumn(row: com.datastax.driver.core.Row, colName: String): String = {
    row.getString(colName)
  }

}

