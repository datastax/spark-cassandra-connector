package com.datastax.spark.connector.datasource

import java.util.{Locale, Optional}

import com.datastax.oss.driver.api.querybuilder.SchemaBuilder
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.datasource.CassandraSourceUtil._
import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.sql.catalyst.analysis.{NamespaceAlreadyExistsException, NoSuchNamespaceException}
import org.apache.spark.sql.connector.catalog.NamespaceChange.{RemoveProperty, SetProperty}
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, NamespaceChange, SupportsNamespaces, SupportsRead, SupportsWrite, TableCatalog}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try

class CassandraCatalogException(msg: String) extends Exception(msg)

/**
  * A Spark Sql Catalog for inter-operation with Cassandra
  *
  * Namespaces naturally map to C* Keyspaces, but they are always only a single
  * element deep.
  */
class CassandraCatalog extends CatalogPlugin
//  with TableCatalog
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

  /**
    *
    */
  def nameSpaceMissing(namespace: Array[String]): NoSuchNamespaceException = {
    new NoSuchNamespaceException(s"Unable to find a keyspace with the name ${namespace.head}")
    //TODO ADD Naming Suggestions
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

  override def listNamespaces(): Array[Array[String]] = {
    getMetadata
      .getKeyspaces
      .asScala
      .map{ case (name, _) => Array(name.asInternal()) }
      .toArray
  }

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = {
    checkNamespace(namespace)

    val result = getMetadata
      .getKeyspace(namespace.head)
      .asScala
      .orElse( throw nameSpaceMissing(namespace))
      .map{ keyspace => Array(keyspace.getName.asInternal()) }
      .toArray

    result
  }

  val ReplicationClass = "class"
  val ReplicationFactor = "replication_factor"
  val DurableWrites = "durable_writes"
  val NetworkTopologyStrategy = "networktopologystrategy"
  val SimpleStrategy = "simplestrategy"
  val IgnoredReplicationOptions = Seq(ReplicationClass, DurableWrites)

  override def loadNamespaceMetadata(namespace: Array[String]): java.util.Map[String, String] = {
    checkNamespace(namespace)
    val ksMetadata = getMetadata
      .getKeyspace(namespace.head)
      .asScala
      .getOrElse( throw nameSpaceMissing(namespace))

    (Map[String, String](
      DurableWrites -> ksMetadata.isDurableWrites.toString,
    ) ++ ksMetadata.getReplication.asScala).asJava
  }



  override def createNamespace(namespace: Array[String], metadata:java.util.Map[String, String]): Unit = {
    val ksMeta = metadata.asScala
    checkNamespace(namespace)
    if (getMetadata.getKeyspace(namespace.head).asScala.isDefined) throw new NamespaceAlreadyExistsException(s"${namespace.head} already exists")
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
    val keyspace = getMetadata.getKeyspace(namespace.head).asScala.getOrElse(throw nameSpaceMissing(namespace))
    val dropResult = cassandraConnector.withSessionDo(session =>
      session.execute(SchemaBuilder.dropKeyspace(keyspace.getName).asCql()))
    dropResult.wasApplied()
  }
}
