package com.datastax.spark.connector.cql

import java.nio.ByteBuffer

import com.datastax.oss.driver.api.core.cql.BoundStatement
import com.datastax.spark.connector.writer.NullKeyColumnException

import scala.collection.JavaConverters._

object QueryUtils {
  /**
    * If a bound statement has all partition key components bound it will
    * return a routing key, but if all components are not bound it returns
    * null. When this is the case we want to let the user know which columns
    * were not correctly bound
    * @param bs a statement completely bound with all parameters
    * @return The routing key
    */
  def getRoutingKeyOrError(bs: BoundStatement): ByteBuffer = {
    val routingKey = bs.getRoutingKey
    if (routingKey == null) throw new NullKeyColumnException(nullPartitionKeyValues(bs))
    routingKey
  }

  private def nullPartitionKeyValues(bs: BoundStatement) = {
    val pkIndicies = bs.getPreparedStatement.getPartitionKeyIndices
    val boundValues = bs.getValues
    pkIndicies.asScala
      .filter(bs.isNull(_))
      .map(bs.getPreparedStatement.getVariableDefinitions.get(_))
      .map(_.getName)
      .mkString(", ")
  }


}
