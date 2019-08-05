package com.datastax.spark.connector.util

import com.datastax.oss.driver.api.core.`type`.DataType
import com.datastax.oss.driver.api.core.`type`.codec.TypeCodec
import com.datastax.oss.driver.api.core.`type`.codec.registry.CodecRegistry


object CodecRegistryUtil {
  def codecFor(registry: CodecRegistry, cqlType: DataType, value: AnyRef) : TypeCodec[AnyRef] = {
    if(value==null) registry.codecFor(cqlType)
    else registry.codecFor(cqlType, value)
  }
}
