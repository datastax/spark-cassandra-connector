package com.datastax.spark.connector.types

/**
  * Type adapters that serve as a middle step in conversion from one type to another.
  *
  * Adapters are utilized by types with scheme ([[UserDefinedType]]], [[TupleType]]) to convert an instance of
  * an type to corresponding adapter and than to final value of the given type.
  */
private[spark] object TypeAdapters {
  /**
    * Adapter for multi-values types that my be returned as a sequence.
    *
    * It is used to extend conversion capabilities offered by Tuple type.
    */
  trait ValuesSeqAdapter {
    def toSeq(): Seq[Any]
  }

  /**
    * Adapter for multi-value types that may return values by name.
    *
    * It is used to extend conversion capabilities offered by UDT type.
    */
  trait ValueByNameAdapter {
    def getByName(name: String): Any
  }
}
