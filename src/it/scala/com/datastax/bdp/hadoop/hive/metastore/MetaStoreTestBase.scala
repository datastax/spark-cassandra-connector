/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package com.datastax.bdp.hadoop.hive.metastore

import java.nio.file.{Files, Path}

import org.apache.hadoop.conf.Configuration

import com.datastax.bdp.spark.DseCassandraConnectionFactory
import com.datastax.bdp.transport.client.HadoopBasedClientConfiguration
import com.datastax.bdp.util.MiscUtil
import com.datastax.spark.connector.DseITFlatSpecBase
import com.datastax.spark.connector.cql.{CassandraConnectionFactory, CassandraConnector, CassandraConnectorConf}
import com.datastax.spark.connector.embedded.{EmbeddedCassandra, YamlTransformations}

abstract class MetaStoreTestBase extends DseITFlatSpecBase {
    HadoopBasedClientConfiguration.setAsClientConfigurationImpl()

    val MetaStoreTableName = "sparkmetastore"
    val MetaStoreWarehouseDir: Path = Files.createTempDirectory("hive-warehouse")

    useCassandraConfig(Seq(YamlTransformations.Default))
    useSparkConf(sparkConf)

    override lazy val conn = CassandraConnector(sparkConf)
    conn.withSessionDo { session =>
        createKeyspace(session)
        session.execute(SchemaManagerService.getMetaStoreTableSchema(getKsName, MetaStoreTableName))
    }

    protected var configuration: Configuration = _
    protected var schemaManagerService: SchemaManagerService = _
    protected var cassandraHiveMetaStore: CassandraHiveMetaStore = _

    beforeClass {
        configuration = buildConfiguration
        cassandraHiveMetaStore = new CassandraHiveMetaStore
        cassandraHiveMetaStore.setConf(configuration)
        schemaManagerService = cassandraHiveMetaStore.getSchemaManagerService
        schemaManagerService.setActiveSession(sparkSession)
    }

    protected def buildConfiguration: Configuration = {
        val conf: Configuration = new Configuration
        conf.set(CassandraClientConfiguration.CONF_PARAM_HOST, EmbeddedCassandra.runner(0).baseConfiguration.nativeTransportAddress)
        conf.set(CassandraClientConfiguration.CONF_PARAM_NATIVE_PORT, EmbeddedCassandra.runner(0).baseConfiguration.nativeTransportPort.toString)
        conf.set(CassandraConnectorConf.ConnectionHostParam.name, EmbeddedCassandra.runner(0).baseConfiguration.nativeTransportAddress)
        conf.set(CassandraConnectorConf.ConnectionPortParam.name, EmbeddedCassandra.runner(0).baseConfiguration.nativeTransportPort.toString)
        conf.set("hive.metastore.warehouse.dir", MetaStoreWarehouseDir.toUri.toString)
        conf.set("cassandra.connection.metaStoreColumnFamilyName", MetaStoreTableName)
        conf.set("spark.cassandra.connection.factory", MiscUtil.objectOrClassName(DseCassandraConnectionFactory))
        conf.set(CassandraClientConfiguration.CONF_PARAM_KEYSPACE_NAME, getKsName)
        conf
    }
}
