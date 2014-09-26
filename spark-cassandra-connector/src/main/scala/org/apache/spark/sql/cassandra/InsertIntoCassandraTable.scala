package org.apache.spark.sql.cassandra

import com.datastax.spark.connector._
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
    childRdd.asInstanceOf[RDD[CassandraRow]].saveToCassandra(cassandraRelation.keyspaceName, cassandraRelation.tableName)

    cc.sparkContext.makeRDD(Nil, 1)
  }
}
