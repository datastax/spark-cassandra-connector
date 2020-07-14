package com.datastax.spark.connector.util

import java.io.IOException

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.PreparedStatement
import com.datastax.oss.driver.api.core.metadata.schema.{ColumnMetadata, TableMetadata}
import com.datastax.spark.connector.cql.{ColumnDef, TableDef}
import com.datastax.spark.connector.util.Quote._

import scala.collection.Seq

object PatitionKeyTools {
  /**
    * This query is only used to build a prepared statement so we can more easily extract
    * partition tokens from tables. We prepare a statement of the form SELECT * FROM keyspace.table
    * where x= .... This statement is never executed.
    */
  private[connector] def querySelectUsingOnlyPartitionKeys(table: TableMetadata): String = {
    val partitionKeys = TableDef.partitionKey(table)
    def quotedColumnNames(columns: Seq[ColumnMetadata]) = partitionKeys.map(ColumnDef.columnName(_)).map(quote)
    val whereClause = quotedColumnNames(partitionKeys).map(c => s"$c = :$c").mkString(" AND ")
    s"SELECT * FROM ${quote(TableDef.keyspaceName(table))}.${quote(TableDef.tableName(table))} WHERE $whereClause"
  }

  private[connector] def prepareDummyStatement(session: CqlSession, table: TableMetadata): PreparedStatement = {
    try {
      session.prepare(querySelectUsingOnlyPartitionKeys(table))
    }
    catch {
      case t: Throwable =>
        throw new IOException(
          s"""Failed to prepare statement
             | ${querySelectUsingOnlyPartitionKeys(table)}: """.stripMargin + t.getMessage, t)
    }
  }

}
