/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package com.datastax.bdp.hadoop.hive.metastore

import java.nio.file.{Files, Path}

import com.datastax.bdp.spark.DseCassandraConnectionFactory
import com.datastax.bdp.util.MiscUtil
import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cql.{CassandraConnector, CassandraConnectorConf}
import org.apache.hadoop.conf.Configuration


abstract class MetaStoreTestBase extends SparkCassandraITFlatSpecBase {

    val MetaStoreTableName = "sparkmetastore"
    val MetaStoreWarehouseDir: Path = Files.createTempDirectory("hive-warehouse")

    override lazy val conn = CassandraConnector(sparkConf)

    protected var configuration: Configuration = _
    protected var schemaManagerService: SchemaManagerService = _
    protected var cassandraHiveMetaStore: CassandraHiveMetaStore = _

    override def beforeClass {
        conn.withSessionDo { session =>
        createKeyspace(session)
        session.execute(SchemaManagerService.getMetaStoreTableSchema(getKsName, MetaStoreTableName))
        }

        configuration = buildConfiguration
        cassandraHiveMetaStore = new CassandraHiveMetaStore
        cassandraHiveMetaStore.setConf(configuration)
        schemaManagerService = cassandraHiveMetaStore.getSchemaManagerService
        schemaManagerService.setActiveSession(sparkSession)
    }

    protected def buildConfiguration: Configuration = {
        val conf: Configuration = new Configuration
        conf.set(CassandraClientConfiguration.CONF_PARAM_HOST, cluster.getConnectionHost)
        conf.set(CassandraClientConfiguration.CONF_PARAM_NATIVE_PORT, cluster.getConnectionPort)
        conf.set(CassandraConnectorConf.ConnectionHostParam.name, cluster.getConnectionHost)
        conf.set(CassandraConnectorConf.ConnectionPortParam.name, cluster.getConnectionPort)
        conf.set("hive.metastore.warehouse.dir", MetaStoreWarehouseDir.toUri.toString)
        conf.set("cassandra.connection.metaStoreColumnFamilyName", MetaStoreTableName)
        conf.set("spark.cassandra.connection.factory", MiscUtil.objectOrClassName(DseCassandraConnectionFactory))
        conf.set(CassandraClientConfiguration.CONF_PARAM_KEYSPACE_NAME, getKsName)
        conf
    }
}
