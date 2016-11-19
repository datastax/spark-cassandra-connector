package com.datastax.spark.connector.rdd

import com.datastax.driver.core.DataType
import com.datastax.driver.core.DataType.Name
import com.datastax.spark.connector.embedded.YamlTransformations
import com.datastax.spark.connector.{SparkCassandraITFlatSpecBase, types}
import com.datastax.spark.connector.types.{ColumnType, ColumnTypeConf, CustomDriverConverter, IntType}

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
}

object DumbConverter extends CustomDriverConverter {
  val asciiType = DataType.ascii()
  override val fromDriverRowExtension: PartialFunction[DataType, ColumnType[_]] = {
    case (x: DataType.CustomType) => {
      IntType
    }
  }
}
