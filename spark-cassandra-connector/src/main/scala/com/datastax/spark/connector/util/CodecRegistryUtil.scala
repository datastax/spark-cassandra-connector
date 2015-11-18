package com.datastax.spark.connector.util

import com.datastax.driver.core.{TypeCodec, CodecRegistry, DataType}

object CodecRegistryUtil {
  def codecFor(cqlType: DataType, value: AnyRef) : TypeCodec[AnyRef] = {
    if(value==null) CodecRegistry.DEFAULT_INSTANCE.codecFor(cqlType)
    else CodecRegistry.DEFAULT_INSTANCE.codecFor(cqlType, value)
  }
}
