package com.datastax.spark.connector.types

import com.datastax.driver.core.DataType

trait CustomDriverConverter {

  val fromDriverRowExtension: PartialFunction[DataType, ColumnType[_]]

}
