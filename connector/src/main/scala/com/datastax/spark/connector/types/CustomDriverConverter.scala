package com.datastax.spark.connector.types

import com.datastax.driver.core.DataType
import org.apache.spark.sql.{types => catalystTypes}

trait CustomDriverConverter {

  /**
    * Function to get connector type for RDD conversions
    */
  val fromDriverRowExtension: PartialFunction[DataType, ColumnType[_]]

  /**
    * Function to get SparkSQL type for the connector type
    *  Should be overridden If sparkSQL support is needed for the types.
    */
  val catalystDataType: PartialFunction[ColumnType[_], catalystTypes.DataType] =  PartialFunction.empty

  /**
    *  Should convert custom C* types to the catalyst compatible one.
    *  Should be overridden If sparkSQL support is needed for the types.
    *  Good approach is to use kryo serializer here or correct toString implementation.
    */

  val catalystDataTypeConverter: PartialFunction[Any, AnyRef] = PartialFunction.empty
}
