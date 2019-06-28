/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.hadoop.hive.metastore;

import com.datastax.driver.core.*;
import com.datastax.spark.connector.cql.CassandraConnector;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.thrift.*;
import org.apache.thrift.meta_data.FieldMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;

import static com.datastax.bdp.util.ScalaJavaUtil.asScalaFunction;

/**
 * Generically persist and load the Hive Meta Store model classes
 *
 *
 */
public class MetaStorePersister
{
    private static final String COL_NAME_SEP = "::";

    private static final Logger log = LoggerFactory.getLogger(MetaStorePersister.class);

    private CassandraClientConfiguration configuration;
    private TSerializer serializer;
    private TDeserializer deserializer;
    private CassandraConnector cc;
    private Cluster externalCluster;
    private String dseVersion;
    private Integer hiveMetaStoreVersion;

    private final static String insertQuery_template = "INSERT INTO \"%s\".\"%s\" (key, entity, value) VALUES(?, ?, ?)";
    private final static String selectAllQuery_template = "SELECT key, entity, value FROM \"%s\".\"%s\"";
    private final static String selectPerKeyQuery_template = selectAllQuery_template + " WHERE key=?";
    private final static String selectQuery_template = selectAllQuery_template + " WHERE key=? AND entity=?";
    private final static String deleteQuery_template = "DELETE FROM \"%s\".\"%s\" WHERE key=? AND entity=?";

    /**
     * cache meta store persisters. I expect only one in 99% of cases,
     * but JobServer and Hive-Thrift server can creates connects to different clusters with different
     * configurations in one JVM
     */

    private static WeakHashMap<CassandraClientConfiguration, MetaStorePersister> metaStorePersisterInstanceCache = new WeakHashMap<>();

    public static synchronized MetaStorePersister getInstance(CassandraClientConfiguration clientConfiguration,
                                                              Cluster externalCluster)
    {
        MetaStorePersister persister = metaStorePersisterInstanceCache.get(clientConfiguration);
        if (persister == null)
        {
            persister = new MetaStorePersister(clientConfiguration, externalCluster);
            metaStorePersisterInstanceCache.put(clientConfiguration, persister);
        } else {
            // update config with latest hadoop related changes
            persister.configuration = clientConfiguration;
        }
        return persister;
    }

    private MetaStorePersister(CassandraClientConfiguration configuration, Cluster externalCluster)
    {
        this.configuration = configuration;
        this.cc = SchemaManagerService.getCassandraConnector(configuration);
        this.externalCluster = externalCluster;

        this.dseVersion = cc.withClusterDo(asScalaFunction(HiveMetaStoreVersionUtil::getDSEVersion)).toString();
        this.hiveMetaStoreVersion = HiveMetaStoreVersionUtil.getHiveMetastoreVersion(dseVersion);
    }

    public void save(Map<? extends TFieldIdEnum, FieldMetaData> metaData,
            @SuppressWarnings("rawtypes") TBase base, String databaseName) throws CassandraHiveMetaStoreException
    {
        // FIXME turns out metaData is not needed anymore. Remove from sig.
        // TODO need to add ID field to column name lookup to avoid overwrites
        if ( log.isDebugEnabled() )
            log.debug("in save with class: {} dbname: {}", base, databaseName);
        serializer = new TSerializer();
        cc.jWithSessionDo(session -> {
            try
            {
                return session.execute(
                        String.format(insertQuery_template, configuration.getKeyspaceName(), configuration.getColumnFamily()),
                        versionedName(databaseName.toLowerCase()),
                        buildEntityColumnName(base), ByteBuffer.wrap(serializer.serialize(base)));
            }
            catch (Exception e)
            {
                throw new CassandraHiveMetaStoreException(e.getMessage(), e);
            }
        });
    }

    public TBase load(TBase base, String databaseName)
        throws CassandraHiveMetaStoreException, HiveMetaStoreNotFoundException
    {
        if ( log.isDebugEnabled() )
            log.debug("in load with class: {} dbname: {}", base.getClass().getName(), databaseName);
        deserializer = new TDeserializer();

        String entity = buildEntityColumnName(base);
        ResultSet result = cc.jWithSessionDo( session ->
                session.execute(
                        String.format(selectQuery_template, configuration.getKeyspaceName(), configuration.getColumnFamily()),
                        versionedName(databaseName.toLowerCase()),
                        entity));
        Row row = result.one();
        if (row == null)
            throw new HiveMetaStoreNotFoundException();
        try
        {
            deserializer.deserialize(base, row.getBytes("value").array());
        }
        catch (Exception e)
        {
            // TODO same exception handling wrapper as above
            throw new CassandraHiveMetaStoreException(e.getMessage(), e);
        }
        return base;
    }

    public List<TBase> find(TBase base, String databaseName)
    throws CassandraHiveMetaStoreException
    {
        return find(base, databaseName, null, Integer.MAX_VALUE);
    }

    @SuppressWarnings("unchecked")
    public List<TBase> find(TBase base, String databaseName, String prefix, int count)
        throws CassandraHiveMetaStoreException
    {
        if ( log.isDebugEnabled() )
            log.debug("in find with class: {} dbname: {} prefix: {} and count: {}",
                      base.getClass().getName(), databaseName, prefix, count);

        deserializer = new TDeserializer();
        List<TBase> resultList = new ArrayList<>();
        int maxCount = (count < 0) ? Integer.MAX_VALUE : count;
        cc.jWithSessionDo(session -> {
                    try
                    {
                        ResultSet result = session.execute(String.format(selectPerKeyQuery_template, configuration.getKeyspaceName(), configuration.getColumnFamily()),
                                versionedName(databaseName.toLowerCase()));
                        Iterator<Row> rows = result.iterator();
                        int size = 0;
                        while (rows.hasNext())
                        {
                    Row row = rows.next();
                    String entityPrefix = entityPrefix(base, prefix);
                    if (size < maxCount && row.getString("entity").startsWith(entityPrefix))
                    {
                        TBase other = base.getClass().newInstance();
                        deserializer.deserialize(other, row.getBytes("value").array());
                        resultList.add(other);
                        size++;
                    }
                }
                return null;
            }
            catch (Exception e)
            {
                throw new CassandraHiveMetaStoreException(e.getMessage(), e);
            }
        });
        return resultList;
    }

    private String entityPrefix(TBase base, String prefix)
    {
        StringBuilder entityPrefix = new StringBuilder(96);
        entityPrefix.append(base.getClass().getName()).append(COL_NAME_SEP);
        if ( prefix != null && !prefix.isEmpty() )
        {
            if ( base instanceof Table || base instanceof Partition)
                prefix = prefix.toLowerCase();
            entityPrefix.append(prefix);
            if ( base instanceof Index || base instanceof Partition)
                entityPrefix.append(COL_NAME_SEP);
        }
        return entityPrefix.toString();
    }

    @SuppressWarnings("unchecked")
    public void remove(TBase base, String databaseName)
    {
        removeAll(Arrays.asList(base), databaseName);
    }

    @SuppressWarnings("unchecked")
    public void removeAll(List<TBase> bases, String databaseName)
    {
        serializer = new TSerializer();
        cc.jWithSessionDo(session -> {
            PreparedStatement deleteStmt = session.prepare(String.format(deleteQuery_template, configuration.getKeyspaceName(), configuration.getColumnFamily()));
            try
            {
                for (TBase tBase : bases)
                {
                    if ( log.isDebugEnabled() )
                        log.debug("in remove with class: {} dbname: {}", tBase, databaseName);
                    delete(session, deleteStmt, versionedName(databaseName.toLowerCase()), buildEntityColumnName(tBase));
                }
            } catch (Exception e)
            {
                throw new CassandraHiveMetaStoreException(e.getMessage(), e);
            }
            return null;
        });
    }

    private void delete(Session session, PreparedStatement deleteStmt, String key, String entity)
    {
        if (log.isDebugEnabled())
            log.debug("delete key: {}, entity: {}", key, entity);
        BoundStatement bs = new BoundStatement(deleteStmt);
        bs.setString(0, key);
        bs.setString(1, entity);
        session.execute(bs);
    }

    private String buildEntityColumnName(TBase base) {
        StringBuilder colName = new StringBuilder(96);
        colName.append(base.getClass().getName()).append(COL_NAME_SEP);
        if ( base instanceof Database)
        {
            colName.append(((Database)base).getName().toLowerCase());
        } else if ( base instanceof Table)
        {
            colName.append(((Table)base).getTableName().toLowerCase());
        } else if ( base instanceof Index)
        {
            colName.append(((Index)base).getOrigTableName().toLowerCase())
            .append(COL_NAME_SEP)
            .append(((Index)base).getIndexName());
        } else if ( base instanceof Partition)
        {
            colName.append(((Partition)base).getTableName().toLowerCase());
            for( String value : ((Partition)base).getValues())
            {
                colName.append(COL_NAME_SEP).append(value);
            }
        } else if ( base instanceof Type)
        {
            colName.append(((Type)base).getName());
        } else if ( base instanceof Role)
        {
            colName.append(((Role)base).getRoleName());
        } else if ( base instanceof Function)
        {
            colName.append(((Function)base).getFunctionName());
        }

        if ( log.isDebugEnabled() )
            log.debug("Constructed columnName: {}", colName);
        return colName.toString();
    }

    private String versionedName(String name)
    {
        if (Objects.equals(hiveMetaStoreVersion, HiveMetaStoreVersionUtil.nonHiveMetastoreVersion))
            return String.format("_%s_%s", dseVersion, name);
        else
            return String.format("_%s_%s", hiveMetaStoreVersion, name);
    }

}
