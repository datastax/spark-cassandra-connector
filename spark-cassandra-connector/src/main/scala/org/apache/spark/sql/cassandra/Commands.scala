package org.apache.spark.sql.cassandra

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.SQLContext

import Commands._

/**
 * Drops a table from the metastore and removes it if it is cached.
 */
private[cassandra] case class DropTable(
    tableIdentifier: Seq[String]) extends RunnableCommand {

  override def run(sqlContext: SQLContext) = {
    val cc = cassandraSQLContext(sqlContext)
    val tableRef = cc.catalog.tableRefFrom(tableIdentifier)
    try {
      //TODO OSS SPARK should be updated to use tableIdentifier
      cc.cacheManager.tryUncacheQuery(cc.table(tableRef.table))
    } catch {
      // This table's metadata is not in
      case _: org.apache.hadoop.hive.ql.metadata.InvalidTableException =>
      // Other Throwables can be caused by users providing wrong parameters in OPTIONS
      // (e.g. invalid paths). We catch it and log a warning message.
      // Users should be able to drop such kinds of tables regardless if there is an error.
      case e: Throwable => log.warn(s"${e.getMessage}")
    }
    cc.catalog.unregisterTable(tableRef)
    Seq.empty[Row]
  }
}

/**
 * Rename a table from the metastore.
 */
private[cassandra] case class RenameTable(
       tableIdentifier: Seq[String],
       newName: String) extends RunnableCommand {

  override def run(sqlContext: SQLContext) = {
    val cc = cassandraSQLContext(sqlContext)
    val tableRef = cc.catalog.tableRefFrom(tableIdentifier)
    try {
      //TODO OSS SPARK should be updated to use tableIdentifier
      cc.cacheManager.tryUncacheQuery(cc.table(tableRef.table))
    } catch {
      // This table's metadata is not in
      case _: org.apache.hadoop.hive.ql.metadata.InvalidTableException =>
      // Other Throwables can be caused by users providing wrong parameters in OPTIONS
      // (e.g. invalid paths). We catch it and log a warning message.
      // Users should be able to drop such kinds of tables regardless if there is an error.
      case e: Throwable => log.warn(s"${e.getMessage}")
    }
    val metadata = cc.catalog.getTableMetadata(tableRef)
    if (metadata.nonEmpty) {
      cc.catalog.unregisterTable(tableRef)
      val newTableIdent =
        TableRef(newName, tableRef.keyspace, tableRef.cluster)
      val data = metadata.get
      cc.catalog.registerTable(
        newTableIdent,
        data.source,
        data.schema,
        data.options)
    }
    Seq.empty[Row]
  }
}

/** Set table schema */
private[cassandra] case class SetTableSchema(
    tableIdentifier: Seq[String],
    schemaJsonString: String) extends RunnableCommand {

  override def run(sqlContext: SQLContext) = {
    val cc = cassandraSQLContext(sqlContext)
    val tableRef = cc.catalog.tableRefFrom(tableIdentifier)
    cc.catalog.setTableSchema(tableRef, schemaJsonString)
    Seq.empty[Row]
  }
}

/** Set an option of table options */
private[cassandra] case class SetTableOption(
    tableIdentifier: Seq[String],
    key: String,
    value: String) extends RunnableCommand {

  override def run(sqlContext: SQLContext) = {
    val cc = cassandraSQLContext(sqlContext)
    val tableRef = cc.catalog.tableRefFrom(tableIdentifier)
    cc.catalog.setTableOption(tableRef, key, value)
    Seq.empty[Row]
  }
}

/** Remove an option of table options */
private[cassandra] case class RemoveTableOption(
    tableIdentifier: Seq[String], key: String) extends RunnableCommand {

  override def run(sqlContext: SQLContext) = {
    val cc = cassandraSQLContext(sqlContext)
    val tableRef = cc.catalog.tableRefFrom(tableIdentifier)
    cc.catalog.removeTableOption(tableRef, key)
    Seq.empty[Row]
  }
}

/** Remove table schema */
private[cassandra] case class RemoveTableSchema(
    tableIdentifier: Seq[String]) extends RunnableCommand {
  override def run(sqlContext: SQLContext) = {
    val cc = cassandraSQLContext(sqlContext)
    val tableRef = cc.catalog.tableRefFrom(tableIdentifier)
    cc.catalog.removeTableSchema(tableRef)
    Seq.empty[Row]
  }
}

/** Change the current used cluster */
private[cassandra] case class UseCluster(
    cluster: String) extends RunnableCommand {

  override def run(sqlContext: SQLContext) = {
    val cc = cassandraSQLContext(sqlContext)
    cc.useCluster(cluster)
    Seq.empty[Row]
  }
}

/** Change the current used database */
private[cassandra] case class UseDatabase(
    databaseIdentifier: Seq[String]) extends RunnableCommand {

  override def run(sqlContext: SQLContext) = {
    val cc = cassandraSQLContext(sqlContext)
    val (clusterName, databaseName) =
      clusterDBFrom(databaseIdentifier, cc)
    cc.useCluster(clusterName)
    cc.useDatabase(databaseName)
    Seq.empty[Row]
  }
}

/** List table names for a database of a cluster */
private[cassandra] case class ShowTables(
    databaseIdentifier: Seq[String]) extends RunnableCommand {

  override def run(sqlContext: SQLContext) = {
    val cc = cassandraSQLContext(sqlContext)
    val (clusterName, databaseName) =
      clusterDBFrom(databaseIdentifier, cc)
    val tables =
      cc.catalog.getTables(Option(databaseName), Option(clusterName))
    tables.map(_._1).map(name => Row(name))
  }
}

/** List database names for a cluster */
private[cassandra] case class ShowDatabases(
    clusterIdentifier: Seq[String]) extends RunnableCommand {

  override def run(sqlContext: SQLContext) = {
    val cc = cassandraSQLContext(sqlContext)
    val id = clusterIdentifier.reverse.lift
    val clusterName = id(0).getOrElse(cc.getCluster)
    val databases = cc.catalog.getDatabases(Option(clusterName))
    databases.map(name => Row(name))
  }
}

/** List cluster names */
private[cassandra] case class ShowClusters() extends RunnableCommand {
  override def run(sqlContext: SQLContext) = {
    val cc = cassandraSQLContext(sqlContext)
    val clusters = cc.catalog.getClusters()
    clusters.map(name => Row(name))
  }
}

/** Create a database in metastore */
private[cassandra] case class CreateDatabase(
    databaseIdentifier: Seq[String]) extends RunnableCommand {

  override def run(sqlContext: SQLContext) = {
    val cc = cassandraSQLContext(sqlContext)
    val (clusterName, databaseName) =
      clusterDBFrom(databaseIdentifier, cc)
    cc.catalog.createDatabase(databaseName, Option(clusterName))
    Seq.empty[Row]
  }
}

/** Create a cluster in metastore */
private[cassandra] case class CreateCluster(
    cluster: String) extends RunnableCommand {

  override def run(sqlContext: SQLContext) = {
    val cc = cassandraSQLContext(sqlContext)
    cc.catalog.createCluster(cluster)
    Seq.empty[Row]
  }
}

/** Drop a database from metastore */
private[cassandra] case class DropDatabase(
    databaseIdentifier: Seq[String]) extends RunnableCommand {

  override def run(sqlContext: SQLContext) = {
    val cc = cassandraSQLContext(sqlContext)
    val (clusterName, databaseName) =
      clusterDBFrom(databaseIdentifier, cc)
    cc.catalog.unregisterDatabase(databaseName, Option(clusterName))
    Seq.empty[Row]
  }
}

/** Drop a cluster from metastore */
private[cassandra] case class DropCluster(
    cluster: String) extends RunnableCommand {

  override def run(sqlContext: SQLContext) = {
    val cc = cassandraSQLContext(sqlContext)
    cc.catalog.unregisterCluster(cluster)
    Seq.empty[Row]
  }
}


object Commands {
  /** Get cluster name and database name from database identifier */
  def clusterDBFrom(
    databaseIdentifier: Seq[String],
    cc: CassandraSQLContext) : (String, String) = {
    val id = databaseIdentifier.reverse.lift
    val clusterName = id(1).getOrElse(cc.getCluster)
    val databaseName = id(0).getOrElse(cc.getKeyspace)
    (clusterName, databaseName)
  }

  def cassandraSQLContext(
      sqlContext: SQLContext) : CassandraSQLContext = {
    sqlContext.asInstanceOf[CassandraSQLContext]
  }
}