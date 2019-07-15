package com.datastax.spark.connector.util

import com.datastax.oss.driver.api.core.`type`.DataType
import com.datastax.oss.driver.api.core.`type`.codec.TypeCodec
import com.datastax.oss.driver.api.core.`type`.codec.registry.CodecRegistry


object CodecRegistryUtil {
  def codecFor(cqlType: DataType, value: AnyRef) : TypeCodec[AnyRef] = {
    if(value==null) CodecRegistry.DEFAULT.codecFor(cqlType)
    else CodecRegistry.DEFAULT.codecFor(cqlType, value)
  }
}
