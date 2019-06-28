/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.hadoop.hive.metastore;

import com.datastax.driver.core.*;

import io.netty.util.internal.StringUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.Iterator;

public class HiveMetastoreMigrateTool
{
    private static Logger logger = LoggerFactory.getLogger(com.datastax.bdp.hadoop.hive.metastore.HiveMetastoreMigrateTool.class);
    private final static String insertQuery_template = "INSERT INTO \"%s\" (key, entity, value) " +
                                                       "VALUES( ?, ?, ?) IF NOT EXISTS";
    private final static String selectQuery_template = "SELECT key, entity, value FROM \"%s\"";

    private final static int EXIT_SUCCESS = 0;
    private final static int EXIT_FAILURE = 1;

    public void migrate(String from, String to, Session session, PrintStream out, Configuration conf) throws MetaException, InvalidObjectException
    {
        String currentRelease = HiveMetaStoreVersionUtil.getDSEVersion(session.getCluster()).toString();

        if (StringUtil.isNullOrEmpty(to))
        {
            to = currentRelease;
        }
        String[] subVersion = to.split("\\.");
        if (subVersion.length == 3)
        {
            int version = versionValue(subVersion);
            if (version < 452)
            {
                out.println("Don't need upgrade metastore to release earlier than 4.5.2");
                System.exit(EXIT_SUCCESS);
            }
            subVersion = currentRelease.split("\\.");
            int currentVersion = versionValue(subVersion);
            if (currentVersion < version)
            {
                out.println("You can't upgrade metastore to release after current release: " + currentRelease);
                System.exit(EXIT_FAILURE);
            }
            if (from != null) {
                subVersion = from.split("\\.");
                if (subVersion.length != 3)
                {
                    out.println("Wrong from version number: " + from);
                    System.exit(EXIT_FAILURE);
                }
                else if (versionValue(subVersion) < 452)
                {
                    // Pre-4.5.2 release there is no verison number in meta store
                    from = null;
                }
                else if (versionValue(subVersion) >= version)
                {
                    out.println(String.format("from version %s should be before version %s", from, to));
                    System.exit(EXIT_FAILURE);
                }
            } else {
                out.println("Missing --from input");
            }
        }
        else
        {
            out.println("Wrong to version number: " + to);
            System.exit(EXIT_FAILURE);
        }

        if (HiveMetaStoreVersionUtil.getHiveMetastoreVersion(to) == 2)
        {
            CassandraHiveMetaStore metastore = new CassandraHiveMetaStore();
            metastore.setConf(conf);
            // Auto generate source tables
            for(String db: metastore.getAllDatabases())
                metastore.getAllTables(db);
        }

        String metastoreTable = conf.get(CassandraClientConfiguration.CONF_PARAM_CF_NAME, CassandraClientConfiguration.DEF_META_STORE_CF);
        String keyspace = conf.get(CassandraClientConfiguration.CONF_PARAM_KEYSPACE_NAME, CassandraClientConfiguration.HIVE_DEF_META_STORE_KEYSPACE);
        session.execute("USE \"" + keyspace + "\"");
        migrate(from, to, session, metastoreTable);
        CassandraHiveMetaStore metastore = new CassandraHiveMetaStore();
        metastore.setConf(conf);
        // convert cfs location to dsefs location
        for(String db: metastore.getAllDatabases())
            for (Table table: metastore.getTables(db))
                metastore.alterTable(db, table.getTableName(), updateTable(table));

        out.println("Upgrading is done");
        System.exit(EXIT_SUCCESS);
    }

    private void migrate(String from, String to, Session session, String metastoreTable)
    {
        String fromPrefix;
        String toPrefix;

        int fromHmsVersion = HiveMetaStoreVersionUtil.nonHiveMetastoreVersion;
        if (from == null)
        {
            fromPrefix = "";
        }
        else
        {
            fromHmsVersion = HiveMetaStoreVersionUtil.getHiveMetastoreVersion(from);
            if (fromHmsVersion == HiveMetaStoreVersionUtil.nonHiveMetastoreVersion)
                fromPrefix = String.format("_%s_", from);
            else
                fromPrefix = String.format("_%s_", fromHmsVersion);
        }

        int toHmsVersion = HiveMetaStoreVersionUtil.getHiveMetastoreVersion(to);
        if (toHmsVersion == HiveMetaStoreVersionUtil.nonHiveMetastoreVersion)
        {
            toPrefix = String.format("_%s_", to);
        }
        else
        {
            if (fromHmsVersion == toHmsVersion)
                return;
            toPrefix = String.format("_%s_", toHmsVersion);
        }

        PreparedStatement insertStmt = session.prepare(String.format(insertQuery_template, metastoreTable));
        ResultSet result = session.execute(String.format(selectQuery_template, metastoreTable));
        Iterator<Row> rows = result.iterator();
        while (rows.hasNext())
        {
            Row row = rows.next();
            String key = row.getString("key");
            String entity = row.getString("entity");
            ByteBuffer value = row.getBytes("value");
            if (startWith(key, fromPrefix))
                insert(session, insertStmt, replace(key, fromPrefix, toPrefix), entity, value);
        }
    }

    private boolean startWith(String key, String prefix)
    {
        if ("".equals(prefix))
            return !key.startsWith("_") || com.datastax.bdp.hadoop.hive.metastore.CassandraClientConfiguration.DATABASES_ROW_KEY.equals(key) || com.datastax.bdp.hadoop.hive.metastore.CassandraClientConfiguration.META_DB_ROW_KEY.equals(key);
        else
            return key.startsWith(prefix);
    }

    private String replace(String key, String fromPrefix, String toPrefix)
    {
        if (!"".equals(fromPrefix))
            return key.replaceFirst(fromPrefix, toPrefix);
        else
            return toPrefix + key;
    }

    private void insert(Session session, PreparedStatement insertStmt, String key, String entity, ByteBuffer value)
    {
        logger.info(String.format("Insert key: %s, entity: %s with CAS", key, entity));
        BoundStatement bs = new BoundStatement(insertStmt);
        bs.setString(0, key);
        bs.setString(1, entity);
        bs.setBytesUnsafe(2, value);
        session.execute(bs);
    }

    private int versionValue(String[] subVersion)
    {
        assert(subVersion.length == 3);
        return Integer.valueOf(subVersion[0]) * 100 + Integer.valueOf(subVersion[1]) * 10 + Integer.valueOf(subVersion[2]);
    }

    private static Table updateTable(Table table) {
        String location = table.getSd().getLocation();
        if (location != null && location.indexOf("cfs://") == 0)
        {
            String dseFsLocation = location.replace("cfs://", "dsefs://");
            table.getSd().setLocation(dseFsLocation);
        }
        return table;
    }
}
