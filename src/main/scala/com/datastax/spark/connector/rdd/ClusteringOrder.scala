package com.datastax.spark.connector.rdd

import com.datastax.spark.connector.cql.TableDef

sealed trait ClusteringOrder extends Serializable {
  private[connector] def toCql(tableDef: TableDef): String
}

object ClusteringOrder {
  private[connector] def cqlClause(tableDef: TableDef, order: String) =
    tableDef.clusteringColumns.headOption.map(cc => s"""ORDER BY "${cc.columnName}" $order""")
      .getOrElse(throw new IllegalArgumentException("Order by can be specified only if there are some clustering columns"))

  case object Ascending extends ClusteringOrder {
    override private[connector] def toCql(tableDef: TableDef): String = cqlClause(tableDef, "ASC")
  }

  case object Descending extends ClusteringOrder {
    override private[connector] def toCql(tableDef: TableDef): String = cqlClause(tableDef, "DESC")
  }

}
