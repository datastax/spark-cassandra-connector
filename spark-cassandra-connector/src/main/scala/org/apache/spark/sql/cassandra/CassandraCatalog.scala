package org.apache.spark.sql.cassandra

import java.io.IOException
import java.util.concurrent.TimeUnit

import com.datastax.spark.connector.cql.{CassandraConnector, Schema}
import com.google.common.cache.{CacheBuilder, CacheLoader}
import org.apache.spark.sql.catalyst.analysis.Catalog
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Subquery}
import org.apache.spark.sql.sources.LogicalRelation

private[cassandra] class CassandraCatalog(cc: CassandraSQLContext) extends Catalog {

  val caseSensitive: Boolean = true

  override def lookupRelation(tableIdentifier: Seq[String], alias: Option[String]): LogicalPlan = {
    val (cluster, database, table) = getClusterDBTableNames(tableIdentifier)
    val tableRef = TableRef(table, database, Option(cluster))
    val sourceRelation = CassandraSourceRelation(tableRef, cc, CassandraSourceOptions())
    val tableWithQualifiers = Subquery(table, LogicalRelation(sourceRelation))
    alias.map(a => Subquery(a, tableWithQualifiers)).getOrElse(tableWithQualifiers)
  }

  private def getClusterDBTableNames(tableIdentifier: Seq[String]): (String, String, String) = {
    val id = processTableIdentifier(tableIdentifier).reverse.lift
    val cluster = id(2).getOrElse(CassandraSourceRelation.defaultClusterName)
    val database = id(1).getOrElse(cc.getKeyspace)
    val table = id(0).getOrElse(throw new IOException(s"Missing table name"))
    (cluster, database, table)
  }

  override def registerTable(tableIdentifier: Seq[String], plan: LogicalPlan): Unit = ???

  override def unregisterTable(tableIdentifier: Seq[String]): Unit = ???

  override def unregisterAllTables(): Unit = ???

  override def tableExists(tableIdentifier: Seq[String]): Boolean = {
    val (cluster, database, table) = getClusterDBTableNames(tableIdentifier)
    false
  }

  override def getTables(databaseName: Option[String]): Seq[(String, Boolean)] = ???

  def refreshTable(databaseName: String, tableName: String): Unit = ???

}
