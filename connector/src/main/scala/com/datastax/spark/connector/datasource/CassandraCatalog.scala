package com.datastax.spark.connector.datasource

import java.util
import java.util.{Locale, Optional}

import com.datastax.oss.driver.api.core.CqlIdentifier
import com.datastax.oss.driver.api.core.CqlIdentifier.fromInternal
import com.datastax.oss.driver.api.core.metadata.schema.{ClusteringOrder, TableMetadata}
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder
import com.datastax.oss.driver.api.querybuilder.schema.{AlterTableAddColumn, AlterTableAddColumnEnd, AlterTableWithOptions, AlterTableWithOptionsEnd, CreateTable, OngoingPartitionKey}
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.datasource.CassandraSourceUtil._
import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.sql.catalyst.analysis.{NamespaceAlreadyExistsException, NoSuchNamespaceException, NoSuchTableException, TableAlreadyExistsException}
import org.apache.spark.sql.connector.catalog.NamespaceChange.{RemoveProperty, SetProperty}
import org.apache.spark.sql.connector.catalog.TableChange.{AddColumn, DeleteColumn}
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, Identifier, NamespaceChange, SupportsNamespaces, Table, TableCatalog, TableChange}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

class CassandraCatalogException(msg: String) extends IllegalArgumentException(msg)

/**
  * A Spark Sql Catalog for inter-operation with Cassandra
  *
  * Namespaces naturally map to C* Keyspaces, but they are always only a single
  * element deep.
  */
class CassandraCatalog extends CatalogPlugin
  with TableCatalog
  with SupportsNamespaces {

  val OnlyOneNamespace = "Cassandra only supports a keyspace name of a single level (no periods in keyspace name)"

  /**
    * The Catalog API usually deals with non-existence of tables or keyspaces by
    * throwing exceptions, so this helper should be added whenever a namespace
    * is used to quickly bail out if the namespace is not compatible with Cassandra
    */
  def checkNamespace(namespace: Array[String]): Unit = {
    if (namespace.length != 1) throw new NoSuchNamespaceException(s"$OnlyOneNamespace : $namespace")
  }

  def nameSpaceMissing(namespace: Array[String]): NoSuchNamespaceException = {
    new NoSuchNamespaceException(namespace)
    //TODO ADD Naming Suggestions
  }


  def tableMissing(namespace: Array[String], name: String): Throwable = {
    new NoSuchTableException(namespace.head, name)
    //Todo Add naming suggestions
  }

  var cassandraConnector: CassandraConnector = _
  var catalogOptions: CaseInsensitiveStringMap = _

  //Something to distinguish this Catalog from others with different hosts
  var catalogName: String = _
  var nameIdentifier: String = _

  //Add asScala to JavaOptions to make working with the driver a little smoother
  implicit class ScalaOptionConverter[T](javaOpt: Optional[T]) {
    def asScala: Option[T] =
      if (javaOpt.isPresent) Some(javaOpt.get) else None
  }

  private def getMetadata = {
    cassandraConnector.withSessionDo(_.getMetadata)
  }

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    catalogOptions = options
    val sparkConf = Try(SparkEnv.get.conf).getOrElse(new SparkConf())
    val connectorConf = consolidateConfs(sparkConf, options.asCaseSensitiveMap().asScala.toMap, name)
    cassandraConnector = CassandraConnector(connectorConf)
    catalogName = name
    nameIdentifier = cassandraConnector.conf.contactInfo.endPointStr()

  }

  override def name(): String = s"Catalog $catalogName For Cassandra Cluster At $nameIdentifier " //TODO add identifier here

  //Namespace Support
  private def getKeyspaceMeta(namespace: Array[String]) = {
    checkNamespace(namespace)
    getMetadata
      .getKeyspace(fromInternal(namespace.head))
      .asScala
      .getOrElse( throw nameSpaceMissing(namespace))
  }

  override def listNamespaces(): Array[Array[String]] = {
    getMetadata
      .getKeyspaces
      .asScala
      .map{ case (name, _) => Array(name.asInternal()) }
      .toArray
  }

  /**
    * Since we only allow single depth keyspace identifiers in C*
    * we always either return an empty list of namespaces or
    * throw a NoSuchNamespaceException
    */
  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = {
    getKeyspaceMeta(namespace) // Thorows no such namespace if namespace is not found
    Array.empty[Array[String]]
  }

  val ReplicationClass = "class"
  val ReplicationFactor = "replication_factor"
  val DurableWrites = "durable_writes"
  val NetworkTopologyStrategy = "networktopologystrategy"
  val SimpleStrategy = "simplestrategy"
  val IgnoredReplicationOptions = Seq(ReplicationClass, DurableWrites)

  override def loadNamespaceMetadata(namespace: Array[String]): java.util.Map[String, String] = {
    val ksMetadata =  getKeyspaceMeta(namespace)

    (Map[String, String](
      DurableWrites -> ksMetadata.isDurableWrites.toString,
    ) ++ ksMetadata.getReplication.asScala).asJava
  }



  override def createNamespace(namespace: Array[String], metadata:java.util.Map[String, String]): Unit = {
    val ksMeta = metadata.asScala
    checkNamespace(namespace)
    if (getMetadata.getKeyspace(fromInternal(namespace.head)).asScala.isDefined) throw new NamespaceAlreadyExistsException(s"${namespace.head} already exists")
    cassandraConnector.withSessionDo{ session =>
      val createStmt = SchemaBuilder.createKeyspace(namespace.head)
      val replicationClass = ksMeta.getOrElse(ReplicationClass, throw new CassandraCatalogException(s"Creating a keyspace requires a $ReplicationClass DBOption for the replication strategy class"))
      val createWithReplication = replicationClass.toLowerCase(Locale.ROOT) match {
        case SimpleStrategy =>
          val replicationFactor = ksMeta.getOrElse(ReplicationFactor,
            throw new CassandraCatalogException(s"Need a $ReplicationFactor option with SimpleStrategy"))
          createStmt.withSimpleStrategy(replicationFactor.toInt)
        case NetworkTopologyStrategy =>
          val datacenters = (ksMeta -- IgnoredReplicationOptions).map(pair => (pair._1, pair._2.toInt: java.lang.Integer))
          createStmt.withNetworkTopologyStrategy(datacenters.asJava)
        case other => throw new CassandraCatalogException(s"Unknown keyspace replication strategy $other")
      }
      val finalCreateStmt = createWithReplication.withDurableWrites(ksMeta.getOrElse(DurableWrites, "True").toBoolean)
      session.execute(finalCreateStmt.asCql())
    }
  }

  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit = {
    checkNamespace(namespace)

    val ksMeta: mutable.Map[String, String] = changes.foldRight(loadNamespaceMetadata(namespace).asScala) {
      case (setProperty: SetProperty, metadata: mutable.Map[String, String]) =>
        metadata + (setProperty.property() -> setProperty.value)
      case (removeProperty: RemoveProperty, metadata: mutable.Map[String, String]) =>
        metadata - removeProperty.property()
    }

    val alterStart = SchemaBuilder.alterKeyspace(namespace.head)
    val alterWithDurable = alterStart.withDurableWrites(ksMeta.getOrElse(DurableWrites, "True").toBoolean)
    val replicationClass = ksMeta
      .getOrElse(ReplicationClass, throw new CassandraCatalogException(s"Creating a keyspace requires a $ReplicationClass option"))
      .split("\\.")
      .last
    val alterWithReplication = replicationClass.toLowerCase(Locale.ROOT) match {
      case SimpleStrategy =>
        val replicationFactor = ksMeta.getOrElse(ReplicationFactor,
          throw new CassandraCatalogException(s"Need a $ReplicationFactor option with SimpleStrategy"))
        alterWithDurable.withSimpleStrategy(replicationFactor.toInt)
      case NetworkTopologyStrategy =>
        val datacenters = (ksMeta -- IgnoredReplicationOptions).map(pair => (pair._1, pair._2.toInt: java.lang.Integer))
        alterWithDurable.withNetworkTopologyStrategy(datacenters.asJava)
      case other => throw new CassandraCatalogException(s"Unknown replication strategy $other")
    }

    cassandraConnector.withSessionDo( session =>
      session.execute(alterWithReplication.asCql())
    )
  }

  override def dropNamespace(namespace: Array[String]): Boolean = {
    checkNamespace(namespace)
    val keyspace = getMetadata.getKeyspace(fromInternal(namespace.head)).asScala
      .getOrElse(throw nameSpaceMissing(namespace))
    val dropResult = cassandraConnector.withSessionDo(session =>
      session.execute(SchemaBuilder.dropKeyspace(keyspace.getName).asCql()))
    dropResult.wasApplied()
  }

  //Table Support
  def getTableMetaData(ident: Identifier) = {
    val namespace = ident.namespace
    checkNamespace(namespace)
    val tableMeta = getMetadata
      .getKeyspace(fromInternal(namespace.head)).asScala
      .getOrElse(throw nameSpaceMissing(namespace))
      .getTable(fromInternal(ident.name)).asScala
      .getOrElse(throw tableMissing(namespace, ident.name()))
    tableMeta
  }

  override def listTables(namespace: Array[String]): Array[Identifier] = {
   getKeyspaceMeta(namespace)
      .getTables.asScala
      .map{case (tableName, _) => Identifier.of(namespace, tableName.asInternal())}
      .toArray
  }

  override def loadTable(ident: Identifier): Table = {
    val tableMeta = getTableMetaData(ident)
    CassandraTable(tableMeta)
  }

  val PartitionKey = "partition_key"
  val ClusteringKey = "clustering_key"

  val NonCassandraProperties = Seq(PartitionKey, ClusteringKey) ++ TableCatalog.RESERVED_PROPERTIES.asScala

  /**
    * Creates a Cassandra Table
    *
    * Uses properties "partition_key" and "clustering_key" to set partition key and clustering key respectively
    * for each of these properties.
    *
    * Partition key is defined as a string of comma-separated column identifiers: "col_a, col_b, col_b"
    * Clustering key is defined as a string of comma-separated column identifiers optionally marked with clustering order: "col_a.ASC, col_b.DESC"
    *
    * Additional options can be specified as table properties
    * For Example
    * caching='{keys=ALL,rows_per_partition=42}',
    * default_time_to_live='33',
    * compaction='{class=SizeTieredCompactionStrategy,bucket_high=42}'
    */
  override def createTable(ident: Identifier, schema: StructType, partitions: Array[Transform], properties: util.Map[String, String]): Table = {
    val tableProps = properties.asScala
    Try(getTableMetaData(ident)) match
      {
      case Success(_) => throw new TableAlreadyExistsException(ident)
      case Failure(noSuchTableException: NoSuchTableException) => //We can create this table
      case Failure(e) => throw e
      }

    //There is an implicit for this but it's only accessible in org.apache.spark.sql.catalog (maybe we should use it)
    val invalidPartitions = partitions.filter( _.name() != "identity" )
    if (invalidPartitions.nonEmpty) {
      throw new UnsupportedOperationException(s"Cassandra Tables can only by partitioned based on direct references to columns, found: ${invalidPartitions.mkString(",")}")
    }

    val providedPartitionKeyNames = partitions.map(_.references().head.fieldNames().head)

    val partitionKeys = if (providedPartitionKeyNames.nonEmpty) {
      providedPartitionKeyNames
    } else {
      tableProps
        .getOrElse(PartitionKey, throw new CassandraCatalogException(s"Cassandra Tables need partition keys defined in property $PartitionKey or with 'PARTITIONED BY (columns)"))
        .split(",")
        .map(_.replaceAll("\\s", ""))
    }

    val partitionKeyNames = partitionKeys.map(fromInternal)

    val clusteringKeyNames = tableProps
      .get(ClusteringKey).toSeq
      .flatMap( value => value.split(",").map(_.replaceAll("\\s", "").split("\\.")))
      .map{ case arr =>
        if (arr.length != 1 && arr.length != 2)
          throw new CassandraCatalogException(s"Unable to parse clustering column ${arr.mkString(".")}, too many components")
        if (arr.length == 2) { //Ordering Passed
          val clusteringOrder = Try(ClusteringOrder.valueOf(arr(1).toUpperCase())).getOrElse(throw new CassandraCatalogException(s"Invalid clustering order found in ${arr.mkString(".")}, must be ASC or DESC or blank"))
          (fromInternal(arr(0)), clusteringOrder)
        } else { //No Ordering Passed
          (fromInternal(arr(0)), ClusteringOrder.ASC)
        }
      }

    val protocolVersion = cassandraConnector.withSessionDo(_.getContext.getProtocolVersion)

    val columnToType = schema.fields.map( sparkField =>
      (fromInternal(sparkField.name), sparkSqlToJavaDriverType(sparkField.dataType, protocolVersion))
    ).toMap

    val namespace = fromInternal(ident.namespace.head)
    val table = fromInternal(ident.name())

    val createTableStart:OngoingPartitionKey = SchemaBuilder.createTable(namespace, table)

    val createTableWithPk: CreateTable = partitionKeyNames.foldLeft(createTableStart) { (createTable, pkName) =>
      val dataType = columnToType.getOrElse(pkName,
        throw new CassandraCatalogException(s"$pkName was defined as a partition key but it does not exist in the schema ${schema.fieldNames.mkString(",")}"))
      createTable.withPartitionKey(pkName, dataType).asInstanceOf[OngoingPartitionKey]
    }.asInstanceOf[CreateTable]

    val createTableWithClustering = clusteringKeyNames.foldLeft(createTableWithPk) { (createTable, ckName) =>
      val dataType = columnToType.getOrElse(ckName._1,
        throw new CassandraCatalogException(s"$ckName was defined as a clustering key but it does not exist in the schema ${schema.fieldNames.mkString(",")}"))
      createTable
        .withClusteringColumn(ckName._1, dataType)
        .withClusteringOrder(ckName._1, ckName._2)
        .asInstanceOf[CreateTable]
    }

    val normalColumns = schema.fieldNames.map(fromInternal).toSet -- (clusteringKeyNames.map(_._1) ++ partitionKeyNames)

    val createTableWithColumns = normalColumns.foldRight(createTableWithClustering) { (colName, createTable) =>
      val dataType = columnToType(colName)
      createTable.withColumn(colName, dataType)
    }

    val userProperties = tableProps -- (NonCassandraProperties)
    val createTableWithProperties = userProperties.foldLeft(createTableWithColumns){
      case (createStmt, (key, value)) => createStmt.withOption(key, parseProperty(value)).asInstanceOf[CreateTable]
    }

    //TODO may have to add a debounce wait
    cassandraConnector.withSessionDo(_.execute(createTableWithProperties.asCql()))

    loadTable(ident)
  }

  def checkColumnName(name: Array[String]): CqlIdentifier = {
    if (name.length != 1) throw new CassandraCatalogException(s"Cassandra Column Identifiers can only have a single identifier, given $name")
    fromInternal(name.head)
  }

  def checkRemoveNormalColumn(tableMeta: TableMetadata, name: Array[String]): CqlIdentifier = {
    val colName = checkColumnName(name)
    val primaryKeys = tableMeta.getPrimaryKey.asScala.map(_.getName.asInternal()).toSet
    if (primaryKeys.contains(colName.asInternal())) {
      throw new CassandraCatalogException(s"Cassandra cannot drop primary key columns: Tried to drop $colName")
    }
    colName
  }

  /**
    * Limited to Adding and Removing Normal Columns and Setting Properties
    *
    * The API expects that if any change is rejected, we  should not apply
    * any changes. This is basically impossible for us since we cannot batch
    * together all of our DDL changes into a single CQL statement. To ameliorate
    * this we perform as many checks as we can up front to try to avoid beginning
    * our DDL requests which we know will fail.
    *
    * We break up DDL into 3 phases
    * Set Properties
    * Remove Columns
    * Add Columns
    */
  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    val protocolVersion = cassandraConnector.withSessionDo(_.getContext.getProtocolVersion)
    val tableMetadata = getTableMetaData(ident)
    val keyspace = tableMetadata.getKeyspace
    val table = tableMetadata.getName

    //Check for unsupported table changes
    changes.foreach {
      case add: AddColumn =>
      case del: DeleteColumn =>
      case setProp: TableChange.SetProperty =>
      case other: TableChange => throw new CassandraCatalogException(s"Cassandra Catalog does not support Alter operation: ${other.getClass.getSimpleName}")
    }

    val propertiesToAdd = changes.collect { case setProperty: TableChange.SetProperty =>
      setProperty
    }
    val columnsToRemove = changes.collect{ case remove: DeleteColumn =>
      checkRemoveNormalColumn(tableMetadata, remove.fieldNames())
    }

    val columnsToAdd = changes.collect{ case add: AddColumn =>
      (checkColumnName(add.fieldNames()), sparkSqlToJavaDriverType(add.dataType(), protocolVersion))
    }

    if (propertiesToAdd.nonEmpty) {
      val setOptionsStatement = propertiesToAdd.foldLeft(
        SchemaBuilder.alterTable(keyspace, table).asInstanceOf[AlterTableWithOptionsEnd]) { case (alter, prop) =>
        alter.withOption(prop.property(), parseProperty(prop.value()))
      }
      cassandraConnector.withSessionDo(_.execute(setOptionsStatement.asCql()))
    }

    if (columnsToRemove.nonEmpty) {
      val dropColumnsStatement = SchemaBuilder.alterTable(keyspace, table).dropColumns(columnsToRemove: _*)
      cassandraConnector.withSessionDo(_.execute(dropColumnsStatement.asCql()))
    }

    if (columnsToAdd.nonEmpty) {
      val addColumnStatement = columnsToAdd.foldRight(
        SchemaBuilder.alterTable(keyspace, table).asInstanceOf[AlterTableAddColumn]
      ) { case ((colName, dataType), alterBuilder) =>
        alterBuilder.addColumn(colName, dataType)
      }.asInstanceOf[AlterTableAddColumnEnd]
      cassandraConnector.withSessionDo(_.execute(addColumnStatement.asCql()))
    }

    loadTable(ident)
  }

  override def dropTable(ident: Identifier): Boolean = {
    val tableMeta = getTableMetaData(ident)
    cassandraConnector.withSessionDo(_.execute(SchemaBuilder.dropTable(tableMeta.getKeyspace, tableMeta.getName).asCql())).wasApplied()
  }

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    throw new UnsupportedOperationException("Cassandra does not support renaming tables")
  }
}
