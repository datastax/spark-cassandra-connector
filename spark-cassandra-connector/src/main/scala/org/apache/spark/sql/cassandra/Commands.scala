package org.apache.spark.sql.cassandra

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.SQLContext

/**
 * Drops a table from the metastore and removes it if it is cached.
 */
private[cassandra] case class DropTable(tableIdentifier: Seq[String]) extends RunnableCommand {
  override def run(sqlContext: SQLContext) = {
    val cc = sqlContext.asInstanceOf[CassandraSQLContext]
    Seq.empty[Row]
  }
}

/**
 * Rename a table from the metastore.
 */
private[cassandra] case class RenameTable(tableIdentifier: Seq[String], newName: String) extends RunnableCommand {
  override def run(sqlContext: SQLContext) = {
    val cc = sqlContext.asInstanceOf[CassandraSQLContext]
    Seq.empty[Row]
  }
}

/** Set table schema */
private[cassandra] case class SetTableSchema(tableIdentifier: Seq[String], schemaJsonString: String) extends RunnableCommand {
  override def run(sqlContext: SQLContext) = {
    val cc = sqlContext.asInstanceOf[CassandraSQLContext]
    Seq.empty[Row]
  }
}

/** Set an option of table options */
private[cassandra] case class SetTableOption(
    tableIdentifier: Seq[String],
    key: String,
    value: String) extends RunnableCommand {
  override def run(sqlContext: SQLContext) = {
    val cc = sqlContext.asInstanceOf[CassandraSQLContext]
    Seq.empty[Row]
  }
}

/** Remove an option of table options */
private[cassandra] case class RemoveTableOption(tableIdentifier: Seq[String], key: String) extends RunnableCommand {
  override def run(sqlContext: SQLContext) = {
    val cc = sqlContext.asInstanceOf[CassandraSQLContext]
    Seq.empty[Row]
  }
}

/** Remove table schema */
private[cassandra] case class RemoveTableSchema(tableIdentifier: Seq[String]) extends RunnableCommand {
  override def run(sqlContext: SQLContext) = {
    val cc = sqlContext.asInstanceOf[CassandraSQLContext]
    Seq.empty[Row]
  }
}

/** Change the current used cluster */
private[cassandra] case class UseCluster(cluster: String) extends RunnableCommand {
  override def run(sqlContext: SQLContext) = {
    val cc = sqlContext.asInstanceOf[CassandraSQLContext]
    Seq.empty[Row]
  }
}

/** Change the current used database */
private[cassandra] case class UseDatabase(databaseIdentifier: Seq[String]) extends RunnableCommand {
  override def run(sqlContext: SQLContext) = {
    val cc = sqlContext.asInstanceOf[CassandraSQLContext]
    Seq.empty[Row]
  }
}

/** Show table names for a database of a cluster */
private[cassandra] case class ShowTables(databaseIdentifier: Seq[String]) extends RunnableCommand {
  override def run(sqlContext: SQLContext) = {
    val cc = sqlContext.asInstanceOf[CassandraSQLContext]
    Seq.empty[Row]
  }
}

/** Show database names for a cluster */
private[cassandra] case class ShowDatabases(clusterIdentifier: Seq[String]) extends RunnableCommand {
  override def run(sqlContext: SQLContext) = {
    val cc = sqlContext.asInstanceOf[CassandraSQLContext]
    Seq.empty[Row]
  }
}

/** Show cluster names */
private[cassandra] case class ShowClusters() extends RunnableCommand {
  override def run(sqlContext: SQLContext) = {
    val cc = sqlContext.asInstanceOf[CassandraSQLContext]
    Seq.empty[Row]
  }
}

/** Create a database in metastore */
private[cassandra] case class CreateDatabase(databaseIdentifier: Seq[String]) extends RunnableCommand {
  override def run(sqlContext: SQLContext) = {
    val cc = sqlContext.asInstanceOf[CassandraSQLContext]
    //cc.createDatabase(databaseName, Option(clusterName))
    Seq.empty[Row]
  }
}

/** Create a cluster in metastore */
private[cassandra] case class CreateCluster(cluster: String) extends RunnableCommand {
  override def run(sqlContext: SQLContext) = {
    val cc = sqlContext.asInstanceOf[CassandraSQLContext]
    //cc.createCluster(cluster)
    Seq.empty[Row]
  }
}

/** Drop a database from metastore */
private[cassandra] case class DropDatabase(databaseIdentifier: Seq[String]) extends RunnableCommand {
  override def run(sqlContext: SQLContext) = {
    val cc = sqlContext.asInstanceOf[CassandraSQLContext]
    //cc.unregisterDatabase(databaseName, Option(clusterName))
    Seq.empty[Row]
  }
}

/** Drop a cluster from metastore */
private[cassandra] case class DropCluster(cluster: String) extends RunnableCommand {
  override def run(sqlContext: SQLContext) = {
    val cc = sqlContext.asInstanceOf[CassandraSQLContext]
    //cc.unregisterCluster(cluster)
    Seq.empty[Row]
  }
}
