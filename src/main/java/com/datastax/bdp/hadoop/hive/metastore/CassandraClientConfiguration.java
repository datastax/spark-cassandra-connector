/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.hadoop.hive.metastore;

import java.util.HashMap;

import com.datastax.bdp.constants.DseSchemaConstants;
import org.apache.hadoop.conf.Configuration;

/**
 * the class holds C* connection properties.
 * The main purpose of the class is to be used as a key in a connection caches.
 * It extends HashMap. All C* related properties are copied there, so equals method works correctly.
 */

public class CassandraClientConfiguration extends HashMap
{
    // TODO are there more appropriate hive config parameters to use here?
    // Local statics
    public static final String META_DB_ROW_KEY = "__meta__";
    public static final String DATABASES_ROW_KEY = "__databases__";
    public static final String DEF_META_STORE_CF = "MetaStore";

    public static final String CONF_PARAM_PREFIX = "cassandra.connection.";

    /** Name of the keyspace in which to store meta data (HiveMetaStore) */
    public static final String CONF_PARAM_KEYSPACE_NAME = CONF_PARAM_PREFIX + "metaStoreKeyspaceName";
    /** Name of the ColumnFamily in which we will store meta information (MetaStore) */
    public static final String CONF_PARAM_CF_NAME = CONF_PARAM_PREFIX + "metaStoreColumnFamilyName";
    /** Initial Apache Cassandra node to which we will connect (localhost) */
    public static final String CONF_PARAM_HOST = "cassandra.host";
    /** Native Port for Apache Cassandra (9042) */
    public static final String CONF_PARAM_NATIVE_PORT = CONF_PARAM_PREFIX+"native.port";
    /** Boolean indicating using spark metastore keyspace */
    public static final String CONF_PARAM_SPARK_ENABLE = "spark.enable";
    public static final String AUTO_CREATE_HIVE_SCHEMA = "cassandra.autoCreateHiveSchema";

    public static final String CASSANDRA_PORT = "cassandra.port"; // nativePort
    public static final String CASSANDRA_USERNAME = "cassandra.username";
    public static final String CASSANDRA_PASSWORD = "cassandra.password";

    public static final String INPUT_NATIVE_AUTH_PROVIDER = "cassandra.input.native.auth.provider";
    public static final String INPUT_NATIVE_SSL_TRUST_STORE_PATH = "cassandra.input.native.ssl.trust.store.path";
    public static final String INPUT_NATIVE_SSL_KEY_STORE_PATH = "cassandra.input.native.ssl.key.store.path";
    public static final String INPUT_NATIVE_SSL_TRUST_STORE_PASSWORD = "cassandra.input.native.ssl.trust.store.password";
    public static final String INPUT_NATIVE_SSL_KEY_STORE_PASSWORD = "cassandra.input.native.ssl.key.store.password";
    public static final String INPUT_NATIVE_SSL_CIPHER_SUITES = "cassandra.input.native.ssl.cipher.suites";
    public static final String INPUT_NATIVE_PROTOCOL_VERSION = "cassandra.input.native.protocol.version";
    public static final String DEFAULT_CASSANDRA_NATIVE_PORT = "9042";

    public static String [] cassandraPropertiesList = {
            CONF_PARAM_KEYSPACE_NAME,
            CONF_PARAM_CF_NAME,
            CONF_PARAM_HOST,
            CONF_PARAM_NATIVE_PORT,
            CONF_PARAM_SPARK_ENABLE,
            CASSANDRA_PORT,
            CASSANDRA_USERNAME,
            CASSANDRA_PASSWORD,
            INPUT_NATIVE_AUTH_PROVIDER,
            INPUT_NATIVE_SSL_TRUST_STORE_PATH,
            INPUT_NATIVE_SSL_KEY_STORE_PATH,
            INPUT_NATIVE_SSL_TRUST_STORE_PASSWORD,
            INPUT_NATIVE_SSL_KEY_STORE_PASSWORD,
            INPUT_NATIVE_SSL_CIPHER_SUITES,
            INPUT_NATIVE_PROTOCOL_VERSION,
            AUTO_CREATE_HIVE_SCHEMA,
            CONF_PARAM_SPARK_ENABLE
    };

    private final String keyspaceName;
    private final String columnFamily;
    private final String username;
    private final String password;
    private String host;
    private final int port;
    private final boolean sparkMetastoreEnabled;

    public int getPort()
    {
        return port;
    }

    public boolean isAutoCreateSchema()
    {
        return autoCreateSchema;
    }

    private final boolean autoCreateSchema;

    public boolean isSparkMetastoreEnabled()
    {
        return sparkMetastoreEnabled;
    }

    public String getKeyspaceName()
    {
        return keyspaceName;
    }

    public String getColumnFamily()
    {
        return columnFamily;
    }

    public String getUsername()
    {
        return username;
    }

    public String getPassword()
    {
        return password;
    }

    public String getHost()
    {
        return host;
    }

    public Configuration getHadoopConfiguration()
    {
        return hadoopConfiguration;
    }

    private Configuration hadoopConfiguration;

    public CassandraClientConfiguration (){this(new Configuration());};

    public CassandraClientConfiguration (Configuration conf) {
        //the class reuse HashMap equals moethod, so copy all C* related properties into the hash map.
        for (String key: cassandraPropertiesList) {
            String value = conf.get(key);
            if (value != null) {
                put(key, value);
            }
        }

        hadoopConfiguration = conf;
        this.keyspaceName = conf.get(CONF_PARAM_KEYSPACE_NAME,
                DseSchemaConstants.HIVE_DEF_META_STORE_KEYSPACE);
        this.columnFamily = conf.get(CONF_PARAM_CF_NAME, DEF_META_STORE_CF);
        this.username = conf.get(CASSANDRA_USERNAME);
        this.password = conf.get(CASSANDRA_PASSWORD);
        this.host = conf.get(CONF_PARAM_HOST);

        String cassandraPort = conf.get(CONF_PARAM_NATIVE_PORT);
        if (cassandraPort == null)
            cassandraPort = DEFAULT_CASSANDRA_NATIVE_PORT;
        try{
            this.port = Integer.parseInt(cassandraPort);
        }
        catch (NumberFormatException e)
        {
            throw new CassandraHiveMetaStoreException(CASSANDRA_PORT + " must be a number");
        }
        sparkMetastoreEnabled = conf.get(CONF_PARAM_SPARK_ENABLE, "false").equalsIgnoreCase("true");
        autoCreateSchema = conf.getBoolean(AUTO_CREATE_HIVE_SCHEMA, false);
    }
}
