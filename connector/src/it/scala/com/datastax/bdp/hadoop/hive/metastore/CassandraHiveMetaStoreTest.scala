/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package com.datastax.bdp.hadoop.hive.metastore

import java.util

import com.datastax.spark.connector.cluster.DefaultCluster
import org.apache.hadoop.hive.metastore.api.{Database, NoSuchObjectException, Partition, Table}
import org.apache.spark.sql.hive.HiveExternalCatalog
import org.scalatest.concurrent.Eventually
import org.scalatest.time.SpanSugar._

class CassandraHiveMetaStoreTest extends MetaStoreTestBase with DefaultCluster {

  implicit val patience: Eventually.PatienceConfig =
    Eventually.patienceConfig.copy(timeout = 30.seconds, interval = 1.second)

  it should "pass SetConf test" in {
    val metaStore = new CassandraHiveMetaStore
    metaStore.setConf(buildConfiguration)
  }

  it should "pass CreateDeleteDatabaseAndTable test" in {
    val metaStore = new CassandraHiveMetaStore
    metaStore.setConf(buildConfiguration)
    val database = new Database("db_name", "My Database", "file:///tmp/", new util.HashMap[String, String])
    metaStore.createDatabase(database)
    val foundDatabase = metaStore.getDatabase("db_name")
    foundDatabase shouldBe database
    val table = new Table
    table.setDbName("db_name")
    table.setTableName("table_name")
    metaStore.createTable(table)
    val foundTable = metaStore.getTable("db_name", "table_name")
    foundTable shouldBe table
    metaStore.getDatabase("db_name") should not be null
    metaStore.getAllTables("db_name").size shouldBe 1
    metaStore.dropTable("db_name", "table_name")
    metaStore.getTable("db_name", "table_name") shouldBe null
    metaStore.getAllTables("db_name").size shouldBe 0
  }

  it should "pass FindEmptyPatitionList test" in {
    val metaStore = new CassandraHiveMetaStore
    metaStore.setConf(buildConfiguration)
    val database = new Database("db_name", "My Database", "file:///tmp/", new util.HashMap[String, String])
    metaStore.createDatabase(database)
    val foundDatabase = metaStore.getDatabase("db_name")
    foundDatabase shouldBe database
    val table = new Table
    table.setDbName("db_name")
    table.setTableName("table_name")
    metaStore.createTable(table)
    var partitionNames: util.List[String] = metaStore.listPartitionNames("db_name", "table_name", 100.toShort)
    partitionNames.size shouldBe 0
    partitionNames = metaStore.listPartitionNames("db_name", "table_name", Byte.byte2short(-1))
    partitionNames.size shouldBe 0
  }

  it should "pass AlterTable test" in {
    val metaStore = new CassandraHiveMetaStore
    metaStore.setConf(buildConfiguration)
    val table = new Table
    table.setDbName("alter_table_db_name")
    table.setTableName("orig_table_name")
    metaStore.createTable(table)
    val foundTable = metaStore.getTable("alter_table_db_name", "orig_table_name")
    foundTable shouldBe table
    val altered = new Table
    altered.setDbName("alter_table_db_name")
    altered.setTableName("new_table_name")
    metaStore.alterTable("alter_table_db_name", "orig_table_name", altered)
    metaStore.getAllTables("alter_table_db_name").size shouldBe 1
  }

  it should "pass AlterDatabaseTable test" in {
    val metaStore = new CassandraHiveMetaStore
    metaStore.setConf(buildConfiguration)
    val database = new Database("alter_db_db_name", "My Database", "file:///tmp/", new util.HashMap[String, String])
    metaStore.createDatabase(database)
    val table = new Table
    table.setDbName("alter_db_db_name")
    table.setTableName("table_name")
    metaStore.createTable(table)
    val foundTable = metaStore.getTable("alter_db_db_name", "table_name")
    foundTable shouldBe table
    val altered = new Database("alter_db_db_name2", "My Database", "file:///tmp/", new util.HashMap[String, String])
    metaStore.alterDatabase("alter_db_db_name", altered)
    metaStore.getAllTables("alter_db_db_name2").size shouldBe 1
  }

  it should "pass AddParition test" in {
    val metaStore = new CassandraHiveMetaStore
    metaStore.setConf(buildConfiguration)
    val database = new Database("alter_part_db", "My Database", "file:///tmp/", new util.HashMap[String, String])
    metaStore.createDatabase(database)
    val foundDatabase = metaStore.getDatabase("alter_part_db")
    foundDatabase shouldBe database
    val table = new Table
    table.setDbName("alter_part_db")
    table.setTableName("table_name")
    metaStore.createTable(table)
    val part = new Partition
    part.setDbName("alter_part_db")
    part.setTableName("table_name")
    val partValues = new util.ArrayList[String]
    partValues.add("dsefs://localhost:9160/user/hive/warehouse/mydb.db/invites/ds=2008-08-08")
    partValues.add("dsefs://localhost:9160/user/hive/warehouse/mydb.db/invites/ds=2008-08-15")
    part.setValues(partValues)
    metaStore.addPartition(part)
    val foundPartition = metaStore.getPartition("alter_part_db", "table_name", partValues)
    foundPartition shouldBe part
  }

  it should "pass AddPartitions test" in {
    val metaStore = new CassandraHiveMetaStore
    metaStore.setConf(buildConfiguration)
    val dbName = "add_partitions_db"
    val tableName = "add_partitions"
    val database = new Database(dbName, "My Database", "file:///tmp/", new util.HashMap[String, String])
    metaStore.createDatabase(database)
    val foundDatabase = metaStore.getDatabase(dbName)
    foundDatabase shouldBe database
    val table = new Table
    table.setDbName(dbName)
    table.setTableName(tableName)
    metaStore.createTable(table)
    val partOne = new Partition
    partOne.setDbName(dbName)
    partOne.setTableName(tableName)
    val partOneValues = new util.ArrayList[String]
    partOneValues.add("cfs://localhost:9160/user/hive/warehouse/mydb.db/invites/ds=2008-08-08")
    partOneValues.add("cfs://localhost:9160/user/hive/warehouse/mydb.db/invites/ds=2008-08-15")
    partOne.setValues(partOneValues)
    val partTwo = new Partition
    val partTwoValues = new util.ArrayList[String]
    partTwo.setDbName(dbName)
    partTwo.setTableName(tableName)
    partTwoValues.add("cfs://localhost:9160/user/hive/warehouse/mydb.db/invites/ds=2008-08-22")
    partTwoValues.add("cfs://localhost:9160/user/hive/warehouse/mydb.db/invites/ds=2008-08-29")
    partTwo.setValues(partTwoValues)
    val parts = new util.ArrayList[Partition]
    parts.add(partOne)
    parts.add(partTwo)
    metaStore.addPartitions(dbName, tableName, parts)
    val foundPartition = metaStore.getPartitions(dbName, tableName, 10)
    foundPartition shouldBe parts
  }

  it should "pass CreateMultipleDatabases test" in {
    val metaStore = new CassandraHiveMetaStore
    metaStore.setConf(buildConfiguration)
    val database = new Database("db_1", "My Database", "file:///tmp/", new util.HashMap[String, String])
    metaStore.createDatabase(database)
    database.setName("db_2")
    metaStore.createDatabase(database)
    metaStore.getAllDatabases.size should be > 1
  }

  it should "pass AddDropReAddDatabase test" in {
    val metaStore = new CassandraHiveMetaStore
    metaStore.setConf(buildConfiguration)
    val database = new Database("add_drop_readd_db", "My Database", "file:///tmp/", new util.HashMap[String, String])
    metaStore.createDatabase(database)
    var foundDatabase: Database = metaStore.getDatabase("add_drop_readd_db")
    foundDatabase shouldBe database
    metaStore.dropDatabase("add_drop_readd_db")
    a[NoSuchObjectException] shouldBe thrownBy {
      metaStore.getDatabase("add_drop_readd_db")
    }
    metaStore.createDatabase(database)
    foundDatabase = metaStore.getDatabase("add_drop_readd_db")
    foundDatabase shouldBe database
  }

  it should "pass CaseInsensitiveNaming test" in {
    val metaStore = new CassandraHiveMetaStore
    metaStore.setConf(buildConfiguration)
    val database = new Database("CiDbNaMe", "My Database", "file:///tmp/", new util.HashMap[String, String])
    metaStore.createDatabase(database)
    val foundDb = metaStore.getDatabase("cidbname")
    foundDb should not be null
    val table = new Table
    table.setDbName("cidbname")
    table.setTableName("TaBlE")
    metaStore.createTable(table)
    val foundTable = metaStore.getTable("cidBname", "table")
    foundTable should not be null
  }

  it should "pass AutoCreateFromKeyspace test" in {
    val metaStore = cassandraHiveMetaStore
    val conf = buildConfiguration
    conn.withSessionDo(session => createKeyspace(session, """"AutoCreatedFromKeyspace""""))
    conf.setBoolean("cassandra.autoCreateHiveSchema", true)
    metaStore.setConf(conf)

    withClue("SchemaManagerService failed to populate keyspace AutoCreatedFromKeyspace") {
      Eventually.eventually({
        metaStore.getSchemaManagerService.refreshMetadata()
        metaStore.getSchemaManagerService.getKeyspaceForDatabaseName("AutoCreatedFromKeyspace") != null
      })
    }

    val foundDb = metaStore.getDatabase("AutoCreatedFromKeyspace")
    foundDb should not be null
    conn.withSessionDo(_.execute("""CREATE TABLE "AutoCreatedFromKeyspace"."OtherCf2" (a int, b int, primary key(a))"""))
    metaStore.getAllTables("AutoCreatedFromKeyspace")
    metaStore.getTable("AutoCreatedFromKeyspace", "OtherCf2") should not be null
  }

  it should "pass GetSchemaForOldEntry test" in {
    val metaStore = cassandraHiveMetaStore
    val conf = buildConfiguration
    conn.withSessionDo(session => createKeyspace(session, "oldks"))
    conf.setBoolean("cassandra.autoCreateHiveSchema", true)
    metaStore.setConf(conf)

    withClue("SchemaManagerService failed to populate keyspace AutoCreatedFromKeyspace") {
      Eventually.eventually({
        metaStore.getSchemaManagerService.refreshMetadata()
        metaStore.getSchemaManagerService.getKeyspaceForDatabaseName("AutoCreatedFromKeyspace") != null
      })
    }

    val foundDb = metaStore.getDatabase("oldks")
    foundDb should not be null
    val t = new Table
    t.putToParameters("auto_created", "true")
    t.setDbName("oldks")
    t.setTableName("oldtable")
    cassandraHiveMetaStore.createTable(t)
    conn.withSessionDo(_.execute("""CREATE TABLE oldks.oldtable (a int, b int, primary key(a))"""))
    metaStore.getAllTables("oldks")
    val parameters = cassandraHiveMetaStore.getTable("oldks", "oldtable").getParameters
    parameters.containsKey(HiveExternalCatalog.DATASOURCE_SCHEMA_NUMPARTS) shouldBe true
  }

}
