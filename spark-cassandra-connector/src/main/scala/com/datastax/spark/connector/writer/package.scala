package com.datastax.spark.connector

/** Contains components for writing RDDs to Cassandra */
package object writer {

  case class NullKeyColumnException(columnName: String)
    extends NullPointerException(s"Invalid null value for key column $columnName")
}
