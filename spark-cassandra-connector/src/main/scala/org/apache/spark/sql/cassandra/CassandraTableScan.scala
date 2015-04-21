package org.apache.spark.sql.cassandra

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.rdd.ValidRDDType
import org.apache.spark.Logging

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.cassandra.CassandraSQLRow.CassandraSQLRowReader
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.LeafNode

import scala.reflect.ClassTag

@DeveloperApi
case class CassandraTableScan(
                               attributes: Seq[Attribute],
                               relation: CassandraRelation,
                               pushdownPred: Seq[Expression])(
                               @transient val context: CassandraSQLContext)
  extends LeafNode with Logging {

  private[this] val transformer = new CatalystRddTransformer(attributes,
    relation.columnNameByLowercase,
    pushdownPred)

  private def inputRdd = {
    logInfo(s"attributes : ${attributes.map(_.name).mkString(",")}")

    val readConf = context.getReadConf(relation.keyspaceName, relation.tableName, relation.cluster)

    val rdd = context.sparkContext
      .cassandraTable[CassandraSQLRow](relation.keyspaceName, relation.tableName)(
        new CassandraConnector(context.getCassandraConnConf(relation.cluster)),
        readConf,
        implicitly[ClassTag[CassandraSQLRow]],
        CassandraSQLRowReader,
        implicitly[ValidRDDType[CassandraSQLRow]])

    transformer.transform(rdd)
  }

  /**
   * Runs this query returning the result as an RDD[Row].
   * cast CassandraRDD to RDD[ROW]
   */
  override def execute() = inputRdd.asInstanceOf[RDD[Row]]

  override def output = if (attributes.isEmpty) relation.output else attributes

}

