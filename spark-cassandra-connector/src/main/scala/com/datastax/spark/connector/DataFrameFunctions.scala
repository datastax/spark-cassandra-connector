package com.datastax.spark.connector

import com.datastax.spark.connector.cql._
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame

/** Provides Cassandra-specific methods on [[org.apache.spark.sql.DataFrame]] */
class DataFrameFunctions(dataFrame: DataFrame) extends Serializable {

  val sparkContext: SparkContext = dataFrame.sqlContext.sparkContext

  def createCassandraTable(keyspaceName: String,
                           tableName: String)
                          (implicit connector: CassandraConnector = CassandraConnector(sparkContext.getConf)): Unit = {

    val table = TableDef.fromDataFrame(dataFrame, keyspaceName, tableName)
    connector.withSessionDo(session => session.execute(table.cql))
  }

}
