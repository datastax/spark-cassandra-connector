/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package com.datastax.bdp.hadoop.hive.metastore

import java.util.Collections

import com.datastax.spark.connector.cluster.DefaultCluster

import scala.collection.JavaConverters._
import org.apache.hadoop.hive.metastore.TableType
import org.apache.hadoop.hive.metastore.api.Table

class SchemaManagerServiceTest extends MetaStoreTestBase with DefaultCluster {

    it should "pass DiscoverUnmappedKeyspaces test" in {
        conn.withSessionDo(createKeyspace(_, """"OtherKeyspace""""))

        val keyspaces = schemaManagerService.findUnmappedKeyspaces(cassandraHiveMetaStore).asScala
        keyspaces should contain("OtherKeyspace")
    }

    it should "pass CreateKeyspaceSchema test" in {
        conn.withSessionDo(createKeyspace(_, """"CreatedKeyspace""""))
        conn.withSessionDo(_.execute("""CREATE TABLE "CreatedKeyspace".test(a int, b int, primary key(a))"""))

        schemaManagerService.createKeyspaceSchema("CreatedKeyspace", cassandraHiveMetaStore)
        val keyspaces = schemaManagerService.findUnmappedKeyspaces(cassandraHiveMetaStore).asScala
        keyspaces should not contain "CreatedKeyspace"
    }

    it should "pass MapKeyspacesIfConfigSettingAllows test" in {
        conn.withSessionDo(createKeyspace(_, """"ConfigCreatedKeyspace""""))
        conn.withSessionDo(_.execute("""CREATE TABLE "ConfigCreatedKeyspace".test(a int, b int, primary key(a))"""))

        configuration.setBoolean(CassandraClientConfiguration.AUTO_CREATE_HIVE_SCHEMA, true)
        cassandraHiveMetaStore.setConf(configuration)
        // refresh test reference to schemaManagerService
        schemaManagerService = cassandraHiveMetaStore.getSchemaManagerService
        schemaManagerService.refreshMetadata()
        schemaManagerService.createKeyspaceSchemasIfNeeded(cassandraHiveMetaStore)
        val keyspaces = schemaManagerService.findUnmappedKeyspaces(cassandraHiveMetaStore).asScala
        keyspaces should not contain "ConfigCreatedKeyspace"
    }

    it should "pass VerifyExternalTable test" in {
        conn.withSessionDo(createKeyspace(_, """"KeyspaceWithExternalTable""""))
        conn.withSessionDo(_.execute("""CREATE TABLE "KeyspaceWithExternalTable".test(a int, b int, primary key(a))"""))

        configuration.setBoolean(CassandraClientConfiguration.AUTO_CREATE_HIVE_SCHEMA, true)
        cassandraHiveMetaStore.setConf(configuration)
        schemaManagerService = cassandraHiveMetaStore.getSchemaManagerService
        schemaManagerService.createKeyspaceSchemasIfNeeded(cassandraHiveMetaStore)
        val table = cassandraHiveMetaStore.getTable("KeyspaceWithExternalTable", "test")
        table.getTableType shouldBe TableType.EXTERNAL_TABLE.toString
        schemaManagerService.refreshMetadata()
        schemaManagerService.verifyExternalTable(table) shouldBe true
        val nonExtTable = new Table
        nonExtTable.setDbName("KeyspaceWithExternalTable")
        nonExtTable.setTableName("table_name")
        cassandraHiveMetaStore.createTable(nonExtTable)
        cassandraHiveMetaStore.getAllTables("KeyspaceWithExternalTable").size shouldBe 2
        cassandraHiveMetaStore.dropTable(table.getDbName, table.getTableName)
        conn.withSessionDo(_.execute("""DROP TABLE "KeyspaceWithExternalTable".test"""))
        schemaManagerService.refreshMetadata()
        schemaManagerService.verifyExternalTable(table) shouldBe false
        cassandraHiveMetaStore.getAllTables("KeyspaceWithExternalTable").size shouldBe 1
    }

    it should "pass VerifyNonCassandraExternalTable test" in {
        val table = new Table
        table.setDbName("NonCassandraExternalTableDb")
        table.setTableName("table_name")
        table.setTableType(TableType.EXTERNAL_TABLE.toString)
        table.setParameters(Collections.singletonMap("EXTERNAL", "TRUE"))
        cassandraHiveMetaStore.createTable(table)
        var tables = cassandraHiveMetaStore.getAllTables("NonCassandraExternalTableDb").asScala
        tables should contain(table.getTableName)
        schemaManagerService.verifyExternalTable(table) shouldBe true
        cassandraHiveMetaStore.dropTable(table.getDbName, table.getTableName)
        tables = cassandraHiveMetaStore.getAllTables("NonCassandraExternalTableDb").asScala
        tables should not contain table.getTableName
    }
}
