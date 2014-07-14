package com.datastax.spark.connector.types

import com.datastax.spark.connector.util.SerializationUtil
import org.junit.Assert._
import org.junit.Test

class TypeSerializationTest {

  private def testSerialization(t: ColumnType[_]) {
    assertEquals(t, SerializationUtil.serializeAndDeserialize(t))
  }

  @Test
  def testSerializationOfPrimitiveTypes() {
    testSerialization(AsciiType)
    testSerialization(TextType)
    testSerialization(IntType)
    testSerialization(BigIntType)
    testSerialization(DoubleType)
    testSerialization(FloatType)
    testSerialization(BooleanType)
    testSerialization(UUIDType)
    testSerialization(TimeUUIDType)
    testSerialization(TimestampType)
    testSerialization(DecimalType)
    testSerialization(BigIntType)
    testSerialization(InetType)
    testSerialization(CounterType)
  }

  @Test
  def testSerializationOfCollectionTypes() {
    testSerialization(ListType(IntType))
    testSerialization(ListType(ListType(IntType)))
    testSerialization(SetType(TextType))
    testSerialization(MapType(BigIntType, TimestampType))
  }


}
