package com.datastax.spark.connector.rdd

import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata
import com.datastax.spark.connector.cql.{ColumnDef, TableDef}

sealed trait ClusteringOrder extends Serializable {
  private[connector] def toCql(tableDef: TableDef): String
  private[connector] def toCql(table: TableMetadata): String
}

// TODO: Keeping both TableDef and TableMetadata impls here to minimize intrusiveness of Java driver metadata changes
object ClusteringOrder {

  private[connector] def cqlClause(tableDef: TableDef, order: String) =
    tableDef.clusteringColumns.headOption.map(cc => s"""ORDER BY "${cc.columnName}" $order""")
      .getOrElse(throw new IllegalArgumentException("Order by can be specified only if there are some clustering columns"))

  private[connector] def cqlClause(table: TableMetadata, order: String) =
    TableDef.clusteringColumns(table).headOption.map(cc => s"""ORDER BY "${ColumnDef.columnName(cc)}" $order""")
      .getOrElse(throw new IllegalArgumentException("Order by can be specified only if there are some clustering columns"))

  case object Ascending extends ClusteringOrder {
    override private[connector] def toCql(tableDef: TableDef): String = cqlClause(tableDef, "ASC")
    override private[connector] def toCql(table: TableMetadata): String = cqlClause(table, "ASC")
  }

  case object Descending extends ClusteringOrder {
    override private[connector] def toCql(tableDef: TableDef): String = cqlClause(tableDef, "DESC")
    override private[connector] def toCql(table: TableMetadata): String = cqlClause(table, "DESC")
  }

}
