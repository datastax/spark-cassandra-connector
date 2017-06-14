package com.datastax.spark.connector.rdd

import com.datastax.driver.core.DataType
import com.datastax.spark.connector.embedded.YamlTransformations
import com.datastax.spark.connector.types.{ColumnType, ColumnTypeConf, CustomDriverConverter, IntType}
import com.datastax.spark.connector.{SparkCassandraITFlatSpecBase, types}
import org.apache.spark.sql.cassandra.DataTypeConverter
import org.apache.spark.sql.{types => catalystTypes}

class CustomFromDriverSpec extends SparkCassandraITFlatSpecBase {
  useCassandraConfig(Seq(YamlTransformations.Default))
  useSparkConf(defaultConf
    .set(ColumnTypeConf.CustomDriverTypeParam.name, "com.datastax.spark.connector.rdd.DumbConverter"))

  "Custom fromDrivers converters " should "be loadable" in {
    ColumnType.fromDriverType(DataType.custom("Dummy")) should be(types.IntType)
    for ((driverType, expectedType) <- ColumnType.primitiveTypeMap) {
      ColumnType.fromDriverType(driverType) should be(expectedType)
    }
  }

  it should "support SparkSQL" in {
    DataTypeConverter.catalystDataType(types.IntType, true) should be(catalystTypes.StringType)
  }
}

object DumbConverter extends CustomDriverConverter {

  override val fromDriverRowExtension: PartialFunction[DataType, ColumnType[_]] = {
    case (x: DataType.CustomType) => {
      IntType
    }
  }

  override val catalystDataType: PartialFunction[ColumnType[_], catalystTypes.DataType] = {
    case IntType => catalystTypes.StringType
  }

  override val catalystDataTypeConverter: PartialFunction[Any, AnyRef] = {
    case x:DataType.CustomType => x.toString
  }
}
