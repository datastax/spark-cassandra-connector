package org.apache.spark.sql.cassandra

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.writer.SqlRowWriter
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, Row}
import org.apache.spark.sql.execution.{SparkPlan, UnaryNode}

@DeveloperApi
case class InsertIntoCassandraTable(cassandraRelation: CassandraRelation,
                               childPlan: SparkPlan,
                               overwrite: Boolean)
                              (@transient cc: CassandraSQLContext) extends UnaryNode {
  self: Product =>

  override def output: Seq[Attribute] = childPlan.output

  override def execute(): RDD[Row] = result

  override def child: SparkPlan = childPlan

  override def otherCopyArgs = cc :: Nil

  /**
   * Insert RDD[[Row]] to Cassandra
   */
  private lazy val result: RDD[Row] = {
    val childRdd = child.execute()

    //TODO: cluster level CassandraConnector, write configuration settings
    childRdd.saveToCassandra(cassandraRelation.keyspaceName, cassandraRelation.tableName)(CassandraConnector(sparkContext.getConf), SqlRowWriter.Factory)

    cc.sparkContext.makeRDD(Nil, 1)
  }
}
