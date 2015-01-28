package com.datastax.spark.connector.rdd

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.reflect.ClassTag

class EmptyCassandraRDD[R](@transient sc: SparkContext,
                           val keyspaceName: String,
                           val tableName: String,
                           val columnNames: ColumnSelector = AllColumns,
                           val where: CqlWhereClause = CqlWhereClause.empty,
                           val limit: Option[Long] = None,
                           val clusteringOrder: Option[ClusteringOrder] = None,
                           val readConf: ReadConf = ReadConf())
                          (implicit val rct: ClassTag[R])
  extends CassandraRDD[R](sc, Seq.empty) {
  override protected def copy(columnNames: ColumnSelector = columnNames,
                              where: CqlWhereClause = where, limit: Option[Long] = limit, clusteringOrder: Option[ClusteringOrder] = None,
                              readConf: ReadConf = readConf, connector: CassandraConnector = connector) =
    new EmptyCassandraRDD[R](sc, keyspaceName, tableName, columnNames, where, limit, clusteringOrder, readConf).asInstanceOf[this.type]

  override protected def getPartitions: Array[Partition] = Array.empty

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[R] = throw new UnsupportedOperationException("Cannot call compute on an EmptyRDD")

  lazy val selectedColumnRefs: Seq[SelectableColumnRef] = {
    val providedColumnNames =
      columnNames match {
        case AllColumns => Seq()
        case PartitionKeyColumns => Seq()
        case SomeColumns(cs@_*) => cs
      }
    providedColumnNames
  }

  override protected def connector: CassandraConnector = throw new UnsupportedOperationException("Empty Cassandra RDD's Don't Have Connections to Cassandra")

  override def toEmptyCassandraRDD: EmptyCassandraRDD[R] = copy()

  override protected def narrowColumnSelection(columns: Seq[SelectableColumnRef]): Seq[SelectableColumnRef] = columns
}
