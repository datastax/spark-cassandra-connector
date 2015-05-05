package org.apache.spark.sql.cassandra

import org.apache.spark.sql.catalyst.analysis.EliminateSubQueries
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.sources.{ResolvedDataSource, InsertableRelation, LogicalRelation}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{AnalysisException, DataFrame, SQLContext, SaveMode}


private[cassandra] case class CreateMetastoreDataSource(
    tableName: String,
    userSpecifiedSchema: Option[StructType],
    provider: String,
    options: Map[String, String],
    allowExisting: Boolean) extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val cc = sqlContext.asInstanceOf[CassandraSQLContext]
    val tableIdent = cc.catalog.tableIdentFrom(Seq(cc.getKeyspace, tableName))
    if (cc.tableExistsInMetastore(tableIdent)) {
      if (allowExisting) {
        return Seq.empty[Row]
      } else {
        throw new AnalysisException(s"Table $tableName already exists.")
      }
    }
    cc.registerTable(
      tableIdent,
      provider,
      userSpecifiedSchema,
      options)
    Seq.empty[Row]
  }
}

private[cassandra] case class CreateMetastoreDataSourceAsSelect(
    tableName: String,
    provider: String,
    mode: SaveMode,
    options: Map[String, String],
    query: LogicalPlan) extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val cc = sqlContext.asInstanceOf[CassandraSQLContext]
    val tableIdent = cc.catalog.tableIdentFrom(Seq(cc.getKeyspace, tableName))
    var existingSchema = None: Option[StructType]
    var createMetastoreTable = false
    if (cc.tableExistsInMetastore(tableIdent)) {
      // Check if we need to throw an exception or just return.
      mode match {
        case SaveMode.ErrorIfExists =>
          throw new AnalysisException(s"Table $tableName already exists. " +
            s"If you are using saveAsTable, you can set SaveMode to SaveMode.Append to " +
            s"insert data into the table or set SaveMode to SaveMode.Overwrite to overwrite" +
            s"the existing data. " +
            s"Or, if you are using SQL CREATE TABLE, you need to drop $tableName first.")
        case SaveMode.Ignore =>
          // Since the table already exists and the save mode is Ignore, we will just return.
          return Seq.empty[Row]
        case SaveMode.Append =>
          // Check if the specified data source match the data source of the existing table.
          val resolved =
            ResolvedDataSource(sqlContext, Some(query.schema), provider, options)
          val createdRelation = LogicalRelation(resolved.relation)
          EliminateSubQueries(sqlContext.table(tableName).logicalPlan) match {
            case l @ LogicalRelation(i: InsertableRelation) =>
              if (i != createdRelation.relation) {
                val errorDescription =
                  s"Cannot append to table $tableName because the resolved relation does not " +
                    s"match the existing relation of $tableName. " +
                    s"You can use insertInto($tableName, false) to append this DataFrame to the " +
                    s"table $tableName and using its data source and options."
                val errorMessage =
                  s"""
                |$errorDescription
                |== Relations ==
                |${sideBySide(
                    s"== Expected Relation ==" ::
                      l.toString :: Nil,
                    s"== Actual Relation ==" ::
                      createdRelation.toString :: Nil).mkString("\n")}
              """.stripMargin
                throw new AnalysisException(errorMessage)
              }
              existingSchema = Some(l.schema)
            case o =>
              throw new AnalysisException(s"Saving data in ${o.toString} is not supported.")
          }
        case SaveMode.Overwrite =>
          cc.sql(s"DROP TABLE IF EXISTS $tableName")
          // Need to create the table again.
          createMetastoreTable = true
      }
    } else {
      // The table does not exist. We need to create it in metastore.
      createMetastoreTable = true
    }

    val data = DataFrame(cc, query)
    val df = existingSchema match {
      // If we are inserting into an existing table, just use the existing schema.
      case Some(schema) => sqlContext.createDataFrame(data.queryExecution.toRdd, schema)
      case None => data
    }

    val optionsWithTableIdent = cc.optionsWithTableIdent(tableIdent, options)
    // Create the relation based on the data of df.
    val resolved = ResolvedDataSource(sqlContext, provider, mode, optionsWithTableIdent, df)
    if (createMetastoreTable) {
      // We will use the schema of resolved.relation as the schema of the table (instead of
      // the schema of df). It is important since the nullability may be changed by the relation
      // provider (for example, see org.apache.spark.sql.parquet.DefaultSource).
      cc.registerTable(
        tableIdent,
        provider,
        Some(resolved.relation.schema),
        options)
    }

    Seq.empty[Row]
  }
}

/**
 * Drops a table from the metastore and removes it if it is cached.
 */
private[cassandra] case class DropTable(tableIdentifier: Seq[String]) extends RunnableCommand {
  override def run(sqlContext: SQLContext) = {
    val cc = sqlContext.asInstanceOf[CassandraSQLContext]
    val tableIdent : TableIdent = cc.catalog.tableIdentFrom(tableIdentifier)
    try {
      cc.cacheManager.tryUncacheQuery(cc.table(tableIdent.table))
    } catch {
      // This table's metadata is not in
      case _: org.apache.hadoop.hive.ql.metadata.InvalidTableException =>
      // Other Throwables can be caused by users providing wrong parameters in OPTIONS
      // (e.g. invalid paths). We catch it and log a warning message.
      // Users should be able to drop such kinds of tables regardless if there is an error.
      case e: Throwable => log.warn(s"${e.getMessage}")
    }
    cc.unregisterTable(tableIdent)
    Seq.empty[Row]
  }
}

/**
 * Rename a table from the metastore.
 */
private[cassandra] case class RenameTable(tableIdentifier: Seq[String], newName: String) extends RunnableCommand {
  override def run(sqlContext: SQLContext) = {
    val cc = sqlContext.asInstanceOf[CassandraSQLContext]
    val tableIdent : TableIdent = cc.catalog.tableIdentFrom(tableIdentifier)
    try {
      //TODO OSS SPARK should update it to use tableIdentifier
      cc.cacheManager.tryUncacheQuery(cc.table(tableIdent.table))
    } catch {
      // This table's metadata is not in
      case _: org.apache.hadoop.hive.ql.metadata.InvalidTableException =>
      // Other Throwables can be caused by users providing wrong parameters in OPTIONS
      // (e.g. invalid paths). We catch it and log a warning message.
      // Users should be able to drop such kinds of tables regardless if there is an error.
      case e: Throwable => log.warn(s"${e.getMessage}")
    }
    val metadata = cc.getTableMetadata(tableIdent)
    if (metadata.nonEmpty) {
      cc.unregisterTable(tableIdent)
      val newTableIdent = TableIdent(newName, tableIdent.keyspace, tableIdent.cluster)
      val data = metadata.get
      cc.registerTable(newTableIdent, data.source, data.schema, data.options)
    }
    Seq.empty[Row]
  }
}

/** Set table schema */
private[cassandra] case class SetTableSchema(tableIdentifier: Seq[String], schemaJsonString: String) extends RunnableCommand {
  override def run(sqlContext: SQLContext) = {
    val cc = sqlContext.asInstanceOf[CassandraSQLContext]
    val tableIdent : TableIdent = cc.catalog.tableIdentFrom(tableIdentifier)
    cc.setTableSchema(tableIdent, schemaJsonString)
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
    val tableIdent : TableIdent = cc.catalog.tableIdentFrom(tableIdentifier)
    cc.setTableOption(tableIdent, key, value)
    Seq.empty[Row]
  }
}

/** Remove an option of table options */
private[cassandra] case class RemoveTableOption(tableIdentifier: Seq[String], key: String) extends RunnableCommand {
  override def run(sqlContext: SQLContext) = {
    val cc = sqlContext.asInstanceOf[CassandraSQLContext]
    val tableIdent : TableIdent = cc.catalog.tableIdentFrom(tableIdentifier)
    cc.removeTableOption(tableIdent, key)
    Seq.empty[Row]
  }
}

/** Remove table schema */
private[cassandra] case class RemoveTableSchema(tableIdentifier: Seq[String]) extends RunnableCommand {
  override def run(sqlContext: SQLContext) = {
    val cc = sqlContext.asInstanceOf[CassandraSQLContext]
    val tableIdent : TableIdent = cc.catalog.tableIdentFrom(tableIdentifier)
    cc.removeTableSchema(tableIdent)
    Seq.empty[Row]
  }
}

private[cassandra] case class UseCluster(cluster: String) extends RunnableCommand {
  override def run(sqlContext: SQLContext) = {
    sqlContext.useCluster(cluster)
    Seq.empty[Row]
  }
}

private[cassandra] case class UseDatabase(database: String) extends RunnableCommand {
  override def run(sqlContext: SQLContext) = {
    sqlContext.useDatabase(database)
    Seq.empty[Row]
  }
}

private[cassandra] case class ShowTables(keyspaceIdentifier: Seq[String]) extends RunnableCommand {
  override def run(sqlContext: SQLContext) = {
    val id = keyspaceIdentifier.reverse.lift
    val clusterName = id(1).getOrElse(sqlContext.getCluster)
    val keyspaceName = id(0).getOrElse(sqlContext.getDatabase)
    val cc = sqlContext.asInstanceOf[CassandraSQLContext]
    val tables = cc.getTables(Option(keyspaceName), Option(clusterName))
    tables.map(_._1).map(name => Row.fromSeq(Seq(name)))
  }
}

private[cassandra] case class ShowDatabases(clusterIdentifier: Seq[String]) extends RunnableCommand {
  override def run(sqlContext: SQLContext) = {
    val id = clusterIdentifier.reverse.lift
    val clusterName = id(0).getOrElse(sqlContext.getCluster)
    val cc = sqlContext.asInstanceOf[CassandraSQLContext]
    val databases = cc.getDatabases(Option(clusterName))
    databases.map(name => Row.fromSeq(Seq(name)))
  }
}

private[cassandra] case class ShowClusters() extends RunnableCommand {
  override def run(sqlContext: SQLContext) = {
    val cc = sqlContext.asInstanceOf[CassandraSQLContext]
    val clusters = cc.getClusters()
    clusters.map(name => Row.fromSeq(Seq(name)))
  }
}

private[cassandra] case class CreateDatabase(keyspaceIdentifier: Seq[String]) extends RunnableCommand {
  override def run(sqlContext: SQLContext) = {
    val id = keyspaceIdentifier.reverse.lift
    val clusterName = id(1).getOrElse(sqlContext.getCluster)
    val keyspaceName = id(0).getOrElse(sqlContext.getDatabase)
    val cc = sqlContext.asInstanceOf[CassandraSQLContext]
    cc.createDatabase(keyspaceName, Option(clusterName))
    Seq.empty[Row]
  }
}

private[cassandra] case class CreateCluster(cluster: String) extends RunnableCommand {
  override def run(sqlContext: SQLContext) = {
    val cc = sqlContext.asInstanceOf[CassandraSQLContext]
    cc.createCluster(cluster)
    Seq.empty[Row]
  }
}

private[cassandra] case class DropDatabase(keyspaceIdentifier: Seq[String]) extends RunnableCommand {
  override def run(sqlContext: SQLContext) = {
    val id = keyspaceIdentifier.reverse.lift
    val clusterName = id(1).getOrElse(sqlContext.getCluster)
    val keyspaceName = id(0).getOrElse(sqlContext.getDatabase)
    val cc = sqlContext.asInstanceOf[CassandraSQLContext]
    cc.unregisterDatabase(keyspaceName, Option(clusterName))
    Seq.empty[Row]
  }
}

private[cassandra] case class DropCluster(cluster: String) extends RunnableCommand {
  override def run(sqlContext: SQLContext) = {
    val cc = sqlContext.asInstanceOf[CassandraSQLContext]
    cc.unregisterCluster(cluster)
    Seq.empty[Row]
  }
}