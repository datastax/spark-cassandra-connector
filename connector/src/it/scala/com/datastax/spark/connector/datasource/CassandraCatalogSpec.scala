package com.datastax.spark.connector.datasource

import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cluster.DefaultCluster
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.catalyst.analysis.{NamespaceAlreadyExistsException, NoSuchNamespaceException}
import org.apache.spark.sql.internal.SQLConf
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.SpanSugar._


class CassandraCatalogSpec extends SparkCassandraITFlatSpecBase with DefaultCluster with BeforeAndAfterAll {

  override def conn: CassandraConnector = CassandraConnector(sparkConf)

  val defaultKs = "catalogtestks"

  def getMetadata() = {
    conn.withSessionDo(_.getMetadata)
  }

  def dropKeyspace(name: String) = {
    conn.withSessionDo(_.execute(s"DROP KEYSPACE IF EXISTS $name"))
  }

  def waitForKeyspaceToExist(keyspace: String, exist: Boolean) = {
    eventually(getMetadata().getKeyspace(keyspace).isPresent shouldBe exist)
  }

  implicit val patienceConfig = PatienceConfig(scaled(5 seconds), scaled(200 millis))

  val catalogName = "cassandra"

  override def beforeClass: Unit =
    {
      super.beforeClass
      spark.conf.set(s"spark.sql.catalog.$catalogName", classOf[CassandraCatalog].getCanonicalName)
      spark.conf.set(SQLConf.DEFAULT_CATALOG.key, "cassandra")
    }

  "A Cassandra Catalog Namespace Support" should "initialize successfully" in {

    spark.sessionState.catalogManager.currentCatalog.name() should include("Catalog cassandra")
  }

  it should "list available keyspaces" in {
    val currentKeyspaces = spark.sql("SHOW DATABASES").collect().map(_.getString(0))
     currentKeyspaces should contain allOf("system_auth", "system_schema", "system_distributed", "system", "system_traces")
  }

  it should "list a keyspace by name"  in {
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $defaultKs WITH DBPROPERTIES (class='SimpleStrategy',replication_factor='5')")
    waitForKeyspaceToExist(defaultKs, true)
    //Using Catalog and DefaultKS triggers listNamespaces(namespace) pathway
    val result = spark.sql(s"SHOW NAMESPACES FROM $catalogName.$defaultKs").collect
    result.head.getString(0) should be (defaultKs)
  }

  it should "give no keyspace found when looking up a keyspace which doesn't exist" in {
    intercept[NoSuchNamespaceException]{
      spark.sql(s"SHOW NAMESPACES FROM $catalogName.fakenotreal").collect
    }
  }

  it should "be able to create a new keyspace" in {
    dropKeyspace(defaultKs)
    waitForKeyspaceToExist(defaultKs, false)
    spark.sql(s"CREATE DATABASE $defaultKs WITH DBPROPERTIES (class='SimpleStrategy',replication_factor='5')")
    waitForKeyspaceToExist(defaultKs, true)
    spark.sql(s"DESCRIBE DATABASE EXTENDED $defaultKs").show
  }

  it should "give a sensible error when trying to create a keyspace without a replication class" in {
    dropKeyspace(defaultKs)
    waitForKeyspaceToExist(defaultKs, false)
    val exception = intercept[CassandraCatalogException]{
      spark.sql(s"CREATE DATABASE $defaultKs WITH DBPROPERTIES (replication_factor='5')")
    }
    exception.getMessage should include("requires a class")
  }

  it should "give a sensible error when trying to create a keyspace with an invalid replication strategy" in {
    dropKeyspace(defaultKs)
    waitForKeyspaceToExist(defaultKs, false)
    val exception = intercept[CassandraCatalogException]{
      spark.sql(s"CREATE DATABASE $defaultKs WITH DBPROPERTIES (class='foobar',replication_factor='5')")
    }
    exception.getMessage should include("Unknown keyspace replication strategy")
  }

  it should "throw a keyspace exists exception when creating a keyspace that already exists" in {
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $defaultKs WITH DBPROPERTIES (class='SimpleStrategy',replication_factor='5')")
    intercept[NamespaceAlreadyExistsException]{
      spark.sql(s"CREATE DATABASE $defaultKs WITH DBPROPERTIES (class='SimpleStrategy',replication_factor='5')")
    }
  }

  it should "explain Cassandra specific properties in the describe statement" in {
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $defaultKs WITH DBPROPERTIES (class='SimpleStrategy',replication_factor='5')")
    waitForKeyspaceToExist(defaultKs, true)
    val results = spark.sql(s"DESCRIBE NAMESPACE EXTENDED $defaultKs").collect()
    val properties = results.filter(_.getString(0) == "Properties").head.getString(1)
    properties should include("SimpleStrategy")
    properties should include("replication_factor,5")
    properties should include("durable_writes,true")
  }

  it should "alter a keyspace" in {
    dropKeyspace(defaultKs)
    waitForKeyspaceToExist(defaultKs, false)
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $defaultKs WITH DBPROPERTIES (class='SimpleStrategy',replication_factor='5')")
    waitForKeyspaceToExist(defaultKs, true)
    spark.sql(s"ALTER NAMESPACE $defaultKs SET DBPROPERTIES (replication_factor='2')")
    eventually {
      getMetadata().getKeyspace(defaultKs).get().getReplication.get("replication_factor") shouldBe ("2")
    }
  }

  it should "refuse to alter a keyspace with a bad replication class" in {
    dropKeyspace(defaultKs)
    waitForKeyspaceToExist(defaultKs, false)
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $defaultKs WITH DBPROPERTIES (class='SimpleStrategy',replication_factor='5')")
    waitForKeyspaceToExist(defaultKs, true)
    intercept[CassandraCatalogException] {
      spark.sql(s"ALTER NAMESPACE $defaultKs SET DBPROPERTIES (class='2')")
    }
  }
  // Drop (needs TableCatalog)

}
