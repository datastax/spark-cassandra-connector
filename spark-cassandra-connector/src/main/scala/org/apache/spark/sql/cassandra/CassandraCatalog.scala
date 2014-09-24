package org.apache.spark.sql.cassandra

import java.io.IOException
import java.util.concurrent.TimeUnit

import com.datastax.spark.connector.cql.{CassandraConnector, Schema}
import com.google.common.cache.{CacheBuilder, CacheLoader}
import org.apache.spark.sql.catalyst.analysis.Catalog
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Subquery}

private[cassandra] class CassandraCatalog(cc: CassandraSQLContext) extends Catalog {

  val caseSensitive: Boolean = true

  val schemas = CacheBuilder.newBuilder
       .maximumSize(100)
       .expireAfterWrite(cc.conf.getLong("schema.expire.in.minutes", 10), TimeUnit.MINUTES)
       .build(
          new CacheLoader[String, Schema] {
            def load(cluster: String) : Schema = {
              Schema.fromCassandra(CassandraConnector(cc.conf))
            }
          })

  override def lookupRelation(
    databaseName: Option[String],
    tableName: String,
    alias: Option[String] = None): LogicalPlan = {

    lazy val defaultDatabase = databaseName.getOrElse(cc.getKeyspace)
    val defaultCluster = "default"
    val (cluster, database, table) = tableName.split("\\.") match {
      case Array(t)       => (defaultCluster, defaultDatabase, t)
      case Array(d, t)    => (defaultCluster, d, t)
      case Array(c, d, t) => (c, d, t)
      case _              => throw new IOException(s"Wrong table name: $tableName")
    }

    val schema = schemas.get(cluster)
    val keyspaceDef = schema.keyspaceByName.getOrElse(database, throw new IOException(s"Keyspace not found: $database"))
    val tableDef = keyspaceDef.tableByName.getOrElse(table, throw new IOException(s"Table not found: $database.$table"))
    val tableWithQualifiers = Subquery(table, CassandraRelation(tableDef, alias)(cc))
    alias.map(a => Subquery(a, tableWithQualifiers)).getOrElse(tableWithQualifiers)
  }

  override def registerTable(databaseName: Option[String], tableName: String, plan: LogicalPlan): Unit = ???

  override def unregisterTable(databaseName: Option[String], tableName: String): Unit = ???

  override def unregisterAllTables(): Unit = ???
}
