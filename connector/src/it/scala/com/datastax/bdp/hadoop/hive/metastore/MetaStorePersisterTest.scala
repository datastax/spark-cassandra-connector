/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package com.datastax.bdp.hadoop.hive.metastore

import java.util

import org.apache.hadoop.hive.metastore.api.{Database, Table}

class MetaStorePersisterTest extends MetaStoreTestBase {
    private var metaStorePersister: MetaStorePersister = _

    override def beforeClass {
        super.beforeClass()
        metaStorePersister = MetaStorePersister.getInstance(new CassandraClientConfiguration(configuration), null)
    }

    "MetaStorePersister" should "pass BasicPersistMetaStoreEntity test" in {
        val database = new Database()
        database.setName("name")
        database.setDescription("description")
        database.setLocationUri("uri")
        database.setParameters(new util.HashMap[String, String])
        metaStorePersister.save(Database.metaDataMap, database, database.getName) // save(TBase base).. via _Fields and findByThriftId, publi MetaDataMap
    }

    it should "pass EntityNotFound test" in {
        val database = new Database()
        database.setName("foo")
        a[HiveMetaStoreNotFoundException] shouldBe thrownBy {
            metaStorePersister.load(database, "name")
        }
    }

    it should "pass BasicLoadMetaStoreEntity test" in {
        val database = new Database()
        database.setName("name")
        database.setDescription("description")
        database.setLocationUri("uri")
        database.setParameters(new util.HashMap[String, String])
        metaStorePersister.save(Database.metaDataMap, database, database.getName)
        val foundDb = new Database
        foundDb.setName("name")
        metaStorePersister.load(foundDb, "name")
        foundDb shouldBe database
    }

    it should "pass FindMetaStoreEntities test" in {
        val database = new Database()
        database.setName("dbname")
        database.setDescription("description")
        database.setLocationUri("uri")
        database.setParameters(new util.HashMap[String, String])
        metaStorePersister.save(Database.metaDataMap, database, database.getName)
        val table = new Table
        table.setDbName("dbname")
        table.setTableName("table_one")
        metaStorePersister.save(Table.metaDataMap, table, table.getDbName)
        table.setTableName("table_two")
        metaStorePersister.save(Table.metaDataMap, table, table.getDbName)
        table.setTableName("table_three")
        metaStorePersister.save(Table.metaDataMap, table, table.getDbName)
        table.setTableName("other_table")
        metaStorePersister.save(Table.metaDataMap, table, table.getDbName)
        var tables = metaStorePersister.find(table, "dbname")
        tables.size shouldBe 4
        tables = metaStorePersister.find(table, "dbname", "table", 100)
        tables.size shouldBe 3
    }

    it should "pass EntityDeletion test" in {
        val database = new Database()
        database.setName("dbname")
        database.setDescription("description")
        database.setLocationUri("uri")
        database.setParameters(new util.HashMap[String, String])
        metaStorePersister.save(Database.metaDataMap, database, database.getName)
        val table = new Table
        table.setDbName("dbname")
        table.setTableName("table_one")
        metaStorePersister.save(Table.metaDataMap, table, table.getDbName)
        val foundDb = new Database
        foundDb.setName("dbname")
        metaStorePersister.load(foundDb, "dbname")
        foundDb shouldBe database
        val foundTable = new Table
        foundTable.setDbName(table.getDbName)
        foundTable.setTableName(table.getTableName)
        metaStorePersister.load(foundTable, "dbname")
        foundTable shouldBe table
        metaStorePersister.remove(foundTable, "dbname")
        metaStorePersister.remove(foundDb, "dbname")
        a[HiveMetaStoreNotFoundException] shouldBe thrownBy {
            metaStorePersister.load(foundTable, "dbname")
        }
        a[HiveMetaStoreNotFoundException] shouldBe thrownBy {
            metaStorePersister.load(foundDb, "dbname")
        }
    }
}
