/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.hadoop.hive.metastore;

import com.datastax.bdp.constants.DseSchemaConstants;
import com.datastax.driver.core.Cluster;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.model.*;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hadoop.hive.ql.optimizer.ppr.PartitionExpressionForMetastore;
import org.apache.hive.common.util.HiveVersionInfo;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.HiveExternalCatalog;
import org.apache.thrift.TBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Pattern;

/**
 * Serializes thrift structs for Hive Meta Store to Apache Cassandra.
 *
 * All of the 'entities' in the meta store schema go into a single row for the database for which they belong.
 *
 * Database names are stored in a special row with the key '__databases__'
 *
 * Meta information such as roles and privileges (that is, 'entities' that can be cross-database) go into the row with
 * the key '__meta__'
 *
 */
public class CassandraHiveMetaStore implements RawStore
{
    public static final String DEFAULT_DECIMAL_PARAMS="(38,18)";

    private static final Logger log = LoggerFactory.getLogger(CassandraHiveMetaStore.class);

    private CassandraClientConfiguration configuration;
    private MetaStorePersister metaStorePersister;
    private SchemaManagerService schemaManagerService;

    //optional client
    private Cluster externalCluster = null;

    public CassandraHiveMetaStore()
    {
        log.debug("Creating CassandraHiveMetaStore");
    }

    /**
     * Starts the underlying Cassandra.Client as well as creating the meta store schema if it does not already exist and
     * creating schemas for keyspaces found if 'cassandra.autoCreateSchema' is set to true.
     */
    public void setConf(Configuration conf)
    {
        configuration = new CassandraClientConfiguration(conf);
        // load meta store
        metaStorePersister = MetaStorePersister.getInstance(configuration, externalCluster);
        schemaManagerService = SchemaManagerService.getInstance(this, configuration, externalCluster);

    }

    public Configuration getConf()
    {
        return configuration.getHadoopConfiguration();
    }

    public Cluster getExternalCluster()
    {
        return externalCluster;
    }

    public void setExternalCluster(Cluster externalCluster)
    {
        this.externalCluster = externalCluster;
    }

    // convenience method for testing
    public SchemaManagerService getSchemaManagerService()
    {
        return schemaManagerService;
    }

    public void createDatabase(Database database) throws InvalidObjectException, MetaException
    {
        log.debug("createDatabase with {}", database);
        try
        {
            metaStorePersister.load(database, database.getName());
        }
        catch (HiveMetaStoreNotFoundException e)
        {
            metaStorePersister.save(Database.metaDataMap, database, database.getName());
            metaStorePersister.save(Database.metaDataMap, database, CassandraClientConfiguration.DATABASES_ROW_KEY);
        }
    }


    public boolean hasDatabase(String databaseName) throws NoSuchObjectException
    {
        log.debug("in hasDatabase with database name: {}", databaseName);
        Database db = new Database();
        db.setName(databaseName);
        try
        {
            metaStorePersister.load(db, databaseName);
            return true;
        }
        catch (HiveMetaStoreNotFoundException e)
        {
            return false;
        }
    }
    public Database getDatabase(String databaseName) throws NoSuchObjectException
    {
        log.debug("in getDatabase with database name: {}", databaseName);
        Database db = new Database();
        db.setName(databaseName);
        try
        {
            metaStorePersister.load(db, databaseName);
        }
        catch (HiveMetaStoreNotFoundException e)
        {
            if (!schemaManagerService.isInternalKeyspace(databaseName) &&
                    configuration.isAutoCreateSchema()
                    && schemaManagerService.createKeyspaceSchemaIfNeeded(databaseName, this))
            {
                log.debug("Configured for auto schema creation with keyspace found: {}", databaseName);
                try
                {
                    metaStorePersister.load(db, databaseName);
                }
                catch (HiveMetaStoreNotFoundException nfe)
                {
                    throw new CassandraHiveMetaStoreException("Could not auto create schema.", nfe);
                }
            }
            else
            {
                throw new NoSuchObjectException("Database named " + databaseName + " did not exist.");
            }
        }
        return db;
    }


    public List<String> getDatabases(String databaseNamePattern) throws MetaException
    {
        // update database list from C*
        schemaManagerService.createKeyspaceSchemasIfNeeded(this);

        log.debug("in getDatabases with databaseNamePattern: {}", databaseNamePattern);
        if ("*".equals(databaseNamePattern))
            databaseNamePattern = "";
        String convertedNamePattern = databaseNamePattern.toLowerCase();
        //Simba ODBC driver convert name with '_' to '.', this is to convert it back
        convertedNamePattern = convertedNamePattern.replace('.', '_');
        log.debug("finally in getDatabases with databaseNamePattern: {}", convertedNamePattern);
        List<TBase> databases = metaStorePersister.find(new Database(),
                CassandraClientConfiguration.DATABASES_ROW_KEY, convertedNamePattern, Integer.MAX_VALUE);
        List<String> results = new ArrayList<>(databases.size());
        for (TBase tBase : databases)
        {
            Database db = (Database) tBase;
            if (StringUtils.isEmpty(databaseNamePattern) ||
                db.getName().matches(databaseNamePattern) ||
                db.getName().toLowerCase().matches(databaseNamePattern))
            {
                results.add(db.getName());
            }
        }
        return results;
    }

    public boolean alterDatabase(String oldDatabaseName, Database database)
            throws NoSuchObjectException, MetaException
    {
        try
        {
            createDatabase(database);
        }
        catch (InvalidObjectException e)
        {
            throw new CassandraHiveMetaStoreException("Error attempting to alter database: " + oldDatabaseName, e);
        }
        List<String> tables = getAllTables(oldDatabaseName);
        List<TBase> removeable = new ArrayList<>();
        for (String table : tables)
        {
            Table t = getTable(oldDatabaseName, table);
            try
            {
                Table nTable = t.deepCopy();
                nTable.setDbName(database.getName());

                removeable.addAll(updateTableComponents(oldDatabaseName, database, t.getTableName(), t));
                createTable(nTable);

            }
            catch (Exception e)
            {
                throw new MetaException("Problem in database rename");
            }
            removeable.add(t);
        }
        metaStorePersister.removeAll(removeable, oldDatabaseName);

        return true;
    }

    public boolean dropDatabase(String databaseName) throws NoSuchObjectException,
            MetaException
    {
        Database database = new Database();
        database.setName(databaseName);
        metaStorePersister.remove(database, databaseName);
        metaStorePersister.remove(database, CassandraClientConfiguration.DATABASES_ROW_KEY);
        return true;
    }

    public List<String> getAllDatabases() throws MetaException
    {
        return getDatabases(StringUtils.EMPTY);
    }

    public void createTable(Table table) throws InvalidObjectException, MetaException
    {
        metaStorePersister.save(Table.metaDataMap, table, table.getDbName());
    }

    private String getProperty(String prop, Table tbl)
    {
        String value = tbl.getParameters().get(prop);
        if (value == null) {
          value = tbl.getSd().getSerdeInfo().getParameters().get(prop);
        }

        return value;
    }

    private Pattern hive12DecimalPattern= Pattern.compile("\\bdecimal\\s*\\(.*?\\)", Pattern.CASE_INSENSITIVE);
    //                              that means not followed by "("   VVVVVVVVVVV
    private Pattern hive13DecimalPattern= Pattern.compile("\\bdecimal(?!\\s*\\()", Pattern.CASE_INSENSITIVE);
    private String fixDecimalType(String colType) {
        if ( HiveVersionInfo.getVersion().compareTo("0.13.0") < 0 ) {
            // remove (scale,precision) if any
            return hive12DecimalPattern.matcher(colType).replaceAll("decimal");
        } else {
            // add default (scale,precision), if it was not set by user
            return hive13DecimalPattern.matcher(colType).replaceAll("decimal" + DEFAULT_DECIMAL_PARAMS);
        }
    }

    public boolean hasMapping(String databaseName, String tableName) throws MetaException {
        log.debug("check mapping database name: {} and table name: {}", databaseName, tableName);
        Table table = new Table();
        table.setTableName(tableName);
        try
        {
            return metaStorePersister.load(table, databaseName) != null;
        }
        catch (HiveMetaStoreNotFoundException e)
        {
            log.debug("no mapping for database name: {} and table name: {}", databaseName, tableName);
            return false;
        }
    }

    public Table getTable(String databaseName, String tableName) throws MetaException
    {
        Table table = getTableAllowNullSchema(databaseName, tableName);

        if (table == null)
            return null;

        // On upgrade we may have entries without schema info
        Map<String, String> parameters = table.getParameters();
        if (parameters != null
                && parameters.containsKey("auto_created")
                && !parameters.containsKey(HiveExternalCatalog.DATASOURCE_SCHEMA_NUMPARTS())) {

            metaStorePersister.remove(table, databaseName);
            log.info("Found an old auto_created table without schema information. Remaking {}.{}", databaseName, tableName);
            table = this.getTableAllowNullSchema(databaseName, tableName);
        }
        table.setDbName(table.getDbName().toLowerCase());
        table.setTableName(table.getTableName().toLowerCase());

        return table;

    }

    public Table getTableAllowNullSchema(String databaseName, String tableName) throws MetaException
    {
        log.debug("in getTable with database name: {} and table name: {}", databaseName, tableName);
        Table table = new Table();
        table.setTableName(tableName);
        try
        {
            metaStorePersister.load(table, databaseName);
        }
        catch (HiveMetaStoreNotFoundException e)
        {
            if(configuration.isAutoCreateSchema())
            {
                //update table mapping
                schemaManagerService.refreshMetadata();
                schemaManagerService.createUnmappedTables(databaseName, this);

                // try one more time
                try
                {
                    metaStorePersister.load(table, databaseName);
                }
                catch (HiveMetaStoreNotFoundException e2)
                {
                    log.info("Table not found after auto mapping: " + databaseName + "." + tableName, e2);
                    return null;
                }
            } else {
                return null;
            }

        }
        try {
            StorageDescriptor sd = table.getSd();
            if (sd != null)
            {
                if (sd.getLocation() != null)
                {
                    final String storedLocation = sd.getLocation().toLowerCase();

                    if (storedLocation.startsWith("cfs:") || storedLocation.startsWith("dsefs:"))
                    {
                        // DSP-2033: Replace the cfs host with the host of this HiveMetaStore,
                        // because the table could be created on a different host of this cluster
                        // and Hive is picky about hostname:

                        // DSP-16561: Doing this to all locations will destroy references to
                        // actual external resources. So we only apply this transformation to
                        // dsefs and cfs locations. This too will require improvement for supporting
                        // multi-dc dsefs/cfs in the future.
                        sd.setLocation(removedHostLocalFile(fixFsHost(sd.getLocation())));
                    }

                    // Make sure the location physically exists because Shark requires it
                    // see: https://github.com/amplab/shark/pull/273
                    Path location = new Path(sd.getLocation());
                    FileSystem fs = location.getFileSystem(configuration.getHadoopConfiguration());
                    if (!fs.exists(location))
                        fs.mkdirs(location);
                }

                // support both hive 0.12 and 0.13+ decimal representation
                for (FieldSchema col : sd.getCols())
                {
                    String colType = col.getType();
                    if (colType.contains("decimal"))
                    {
                        col.setType(fixDecimalType(colType));
                    }

                }
            }
        }
        catch (java.io.IOException e) {
            log.error("Failed to getTable for " + databaseName + "." + tableName, e);
            throw new MetaException(e.getMessage());
        }

        // On upgrade we may have entries without schema info
        Map<String, String> parameters = table.getParameters();
        if (parameters != null
            && parameters.containsKey("auto_created")
            && !parameters.containsKey(HiveExternalCatalog.DATASOURCE_SCHEMA_NUMPARTS())) {

            metaStorePersister.remove(table, databaseName);
            log.info("Found an old auto_created table without schema information. Remaking {}.{}", databaseName, tableName);
            table = this.getTable(databaseName, tableName);
        }

        table.setDbName(table.getDbName().toLowerCase());
        table.setTableName(table.getTableName().toLowerCase());

        return table;
    }

    /**
     * Retrieve the tables for the given database and pattern.
     *
     * @param dbName
     * @param tableNamePattern
     *            the pattern passed as is to {@link String#matches(String)} of {@link org.apache.hadoop.hive.metastore.api.Table#getTableName()}
     */
    public List<String> getTables(String dbName, String tableNamePattern)
            throws MetaException
    {
        if (schemaManagerService.getSystemKeyspaces().contains(dbName))
            return new ArrayList<>();
        log.info("in getTables with dbName: {} and tableNamePattern: {}", dbName, tableNamePattern);
        schemaManagerService.refreshMetadata();
        if(configuration.isAutoCreateSchema())
            schemaManagerService.createUnmappedTables(dbName, this);
        List<TBase> tables = metaStorePersister.find(new Table(), dbName);
        List<String> results = new ArrayList<>(tables.size());
        for (TBase tBase : tables)
        {
            if ("*".equals(tableNamePattern))
                tableNamePattern = "";
            Table table = (Table) tBase;
            if (StringUtils.isEmpty(tableNamePattern) ||
                table.getTableName().matches(tableNamePattern) ||
                table.getTableName().toLowerCase().matches(tableNamePattern) )
            {
                log.debug("Adding table: {}", table);
                if (!schemaManagerService.verifyExternalTable(table))
                {
                    log.warn("Removing deleted external table: {}", table.getTableName());
                    dropTable(dbName, table.getTableName());
                }
                else
                {
                    results.add(table.getTableName());
                }
            }
        }
        return results;
    }

    public List<String> getAllTables(String databaseName) throws MetaException
    {
        log.debug("in getAllTables");
        if (schemaManagerService.getSystemKeyspaces().contains(databaseName))
            return new ArrayList<>();
        return getTables(databaseName, StringUtils.EMPTY);
    }

    public List<Table> getTables(String dbName) throws MetaException
    {
        if (schemaManagerService.getSystemKeyspaces().contains(dbName))
            return new ArrayList<>();
        log.info("in getTables with dbName: {}", dbName);
        schemaManagerService.refreshMetadata();
        if(configuration.isAutoCreateSchema())
            schemaManagerService.createUnmappedTables(dbName, this);
        List<TBase> tables = metaStorePersister.find(new Table(), dbName);
        List<Table> results = new ArrayList<>(tables.size());
        for (TBase tBase : tables)
        {
            Table table = (Table) tBase;
            log.debug("Adding table: {}", table);
            if (!schemaManagerService.verifyExternalTable(table))
            {
                log.warn("Removing deleted external table: {}", table.getTableName());
                dropTable(dbName, table.getTableName());
            }
            else
            {
                results.add(table);
            }
        }
        return results;
    }

    public void alterTable(String databaseName, String oldTableName, Table table)
            throws InvalidObjectException, MetaException
    {
        if (log.isDebugEnabled())
            log.debug("Altering oldTableName {} on datbase: {} new Table: {}", oldTableName, databaseName, table.getTableName());

        if (oldTableName.equalsIgnoreCase(table.getTableName()))
        {
            createTable(table);
        }
        else
        {
            List<TBase> removeable = updateTableComponents(databaseName, null, oldTableName, table);
            // dropTable(databaseName, oldTableName);
            Table oldTable = new Table();
            oldTable.setDbName(databaseName);
            oldTable.setTableName(oldTableName);
            metaStorePersister.remove(oldTable, databaseName);
            if (removeable != null && !removeable.isEmpty())
                metaStorePersister.removeAll(removeable, databaseName);
        }

    }

    private List<TBase> updateTableComponents(String oldDatabaseName, Database database, String oldTableName,
            Table table)
            throws InvalidObjectException, MetaException
    {

        createTable(table);
        List<TBase> toRemove = new ArrayList<>();

        // No need to alter partitions here, because Hive called alterPartition already
        // for all partitions attached to old table. Here we only need to reattach modified partitions to the
        // new table.
        List<Partition> parts = getPartitions(oldDatabaseName, oldTableName);
        for (Partition partition : parts)
        {
            toRemove.add(partition.deepCopy());
            if (database != null)
                partition.setDbName(database.getName());
            partition.setTableName(table.getTableName());
            addPartition(partition);
        }
        // getIndexes
        List<Index> indexes = getIndexes(oldDatabaseName, oldTableName);
        for (Index index : indexes)
        {
            toRemove.add(index.deepCopy());
            if (database != null)
                index.setDbName(database.getName());
            index.setOrigTableName(table.getTableName());
            addIndex(index);
        }

        return toRemove;
    }

    public boolean dropTable(String databaseName, String tableName) throws MetaException
    {
        log.debug("in dropTable with databaseName: {} and tableName: {}", databaseName, tableName);
        Table table = getTable(databaseName, tableName);

        List<TBase> removeables = new ArrayList<>();
        List<Partition> partitions = getPartitions(databaseName, tableName);
        if (partitions != null && !partitions.isEmpty())
            removeables.addAll(partitions);
        List<Index> indexes = getIndexes(databaseName, tableName);
        if (indexes != null && !indexes.isEmpty())
            removeables.addAll(indexes);
        metaStorePersister.remove(table, databaseName);
        if (!removeables.isEmpty())
            metaStorePersister.removeAll(removeables, databaseName);
        return true;
    }

    public boolean addIndex(Index index) throws InvalidObjectException,
            MetaException
    {
        if (index.getParameters() != null)
        {
            Set<Entry<String, String>> entrySet = index.getParameters().entrySet();
            for (Entry<String, String> entry : entrySet)
            {
                if (entry.getValue() == null)
                    entrySet.remove(entry);
            }
        }
        metaStorePersister.save(Index.metaDataMap, index, index.getDbName());
        return false;
    }

    public Index getIndex(String databaseName, String tableName, String indexName)
            throws MetaException
    {
        if (log.isDebugEnabled())
            log.debug("in getIndex with databaseName: {}, tableName: {} indexName: {}", databaseName, tableName,
                    indexName);
        Index index = new Index();
        index.setDbName(databaseName);
        index.setIndexName(indexName);
        index.setOrigTableName(tableName);
        try
        {
            metaStorePersister.load(index, databaseName);
        }
        catch (HiveMetaStoreNotFoundException nfe)
        {
            throw new MetaException("Index: " + indexName + " did not exist for table: " + tableName + " in database: "
                    + databaseName);
        }
        return index;
    }

    private  List<Index> getIndexes(String databaseName, String originalTableName)
            throws MetaException
    {
        return getIndexes(databaseName, originalTableName, Integer.MAX_VALUE);
    }

    public List<Index> getIndexes(String databaseName, String originalTableName, int max)
            throws MetaException
    {
        List results = metaStorePersister.find(new Index(), databaseName, originalTableName, max);
        return (List<Index>) results;
    }

    public List<String> listIndexNames(String databaseName, String originalTableName, short max)
            throws MetaException
    {
        List<Index> indexes = getIndexes(databaseName, originalTableName, max);
        List<String> results = new ArrayList<>(indexes.size());
        for (Index index : indexes)
        {
            results.add(index.getIndexName());
        }
        return results;
    }

    public void alterIndex(String databaseName, String originalTableName,
            String originalIndexName, Index index)
            throws InvalidObjectException, MetaException
    {
        if (log.isDebugEnabled())
            log.debug("Altering index {} on database: {} and table: {} Index: {}",
                      originalIndexName, databaseName, originalTableName, index);

        if (!index.getIndexName().equalsIgnoreCase(originalIndexName))
            dropIndex(databaseName, originalTableName, originalIndexName);

        addIndex(index);
    }

    public boolean dropIndex(String databaseName, String originalTableName, String indexName)
            throws MetaException
    {
        if (log.isDebugEnabled())
            log.debug("In dropIndex with databaseName: {} and originalTableName: {} indexName: {}",
                      databaseName, originalTableName, indexName);
        Index index = new Index();
        index.setDbName(databaseName);
        index.setOrigTableName(originalTableName);
        index.setIndexName(indexName);
        metaStorePersister.remove(index, databaseName);
        return true;
    }

    public boolean addPartition(Partition partition) throws InvalidObjectException,
            MetaException
    {
        log.debug("in addPartition with: {}", partition);
        metaStorePersister.save(Partition.metaDataMap, partition, partition.getDbName());
        return true;
    }

    @Override public boolean addPartitions(String databaseName, String tableName, List<Partition> parts)
            throws InvalidObjectException, MetaException
    {
        for (Partition part : parts)
        {
            if (!Objects.equals(part.getTableName(), tableName) || !Objects.equals(part.getDbName(), databaseName))
            {
                throw new MetaException("Partition does not belong to target table "
                        + databaseName + "." + tableName + ": " + part);
            }
            addPartition(part);
        }
        return true;
    }

    @Override public boolean addPartitions(String dbName, String tblName, PartitionSpecProxy partitionSpec,
            boolean ifNotExists) throws InvalidObjectException, MetaException
    {
        List<Partition> partToCreate = new ArrayList<>();
        for (PartitionSpecProxy.PartitionIterator iter = partitionSpec.getPartitionIterator(); iter.hasNext() ;) {
            Partition part = iter.next();
            try
            {
                if (doesPartitionExist(dbName, tblName, part.getValues()))
                {
                    if (ifNotExists)
                        continue;
                    else
                        throw new MetaException("Partition already exists: " + part);
                }
            }
            catch (NoSuchObjectException e)
            {
                log.debug("NoSuchObjectException in addPartitions");
            }
            partToCreate.add(part);

        }

        return addPartitions(dbName, tblName, partToCreate);
    }

    public Partition getPartition(String databaseName, String tableName, List<String> partitions)
            throws MetaException, NoSuchObjectException
    {
        log.debug("in getPartition databaseName: {} tableName: {} partitions: {}", databaseName, tableName, partitions);
        Partition partition = new Partition();
        partition.setDbName(databaseName);
        partition.setTableName(tableName);
        partition.setValues(partitions);
        try
        {
            metaStorePersister.load(partition, databaseName);
        }
        catch (HiveMetaStoreNotFoundException e)
        {
            throw new NoSuchObjectException("Could not find partition for: " + partitions + " on table: " + tableName
                    + " in database: " + databaseName);
        }
        return partition;
    }

    @Override public boolean doesPartitionExist(String databaseName, String tableName, List<String> partitions)
            throws MetaException, NoSuchObjectException
    {
        try
        {
            return getPartition(databaseName,tableName, partitions) !=null;
        } catch (MetaException | NoSuchObjectException e)
        {
            log.debug("Partitions not found. Exception {}", e);
            return false;
        }
    }

    private List<Partition> getPartitions(String databaseName, String tableName)
            throws MetaException
    {
        return getPartitions(databaseName, tableName, Integer.MAX_VALUE);
    }

    public List<Partition> getPartitions(String databaseName, String tableName, int max)
            throws MetaException
    {
        log.debug("in getPartitions: databaseName: {} tableName: {} max: {}", databaseName, tableName, max);

        List results = metaStorePersister.find(new Partition(), databaseName, tableName, max);
        log.debug("Found partitions: {}", results);
        return (List<Partition>) results;
    }

    public List<String> listPartitionNames(String databaseName, String tableName, short max)
            throws MetaException
    {
        log.debug("in listPartitionNames: databaseName: {} tableName: {} max: {}", databaseName, tableName, max);
        List<Partition> partitions = getPartitions(databaseName, tableName, max);
        List<String> results = new ArrayList<>();
        if (partitions == null)
            return results;
        for (Partition partition : partitions)
        {
            results.add(partition.getSd().getLocation());
        }
        return results;
    }

    public void alterPartition(String databaseName, String tableName, Partition partition)
            throws InvalidObjectException, MetaException
    {
        if (log.isDebugEnabled())
            log.debug("Altering partition for table {} on database: {} Partition: {}", tableName, databaseName, partition);

        try
        {
            getPartition(databaseName, tableName, partition.getValues());
        }
        catch (NoSuchObjectException nse)
        {
            throw new InvalidObjectException(nse.getMessage());
        }
        addPartition(partition);
    }

    public boolean dropPartition(String databaseName, String tableName, List<String> partValues)
            throws MetaException
    {
        Partition partition = new Partition();
        partition.setDbName(databaseName);
        partition.setTableName(tableName);
        partition.setValues(partValues);
        if (log.isDebugEnabled())
            log.debug("Dropping partition: {}", partition);
        metaStorePersister.remove(partition, databaseName);
        return true;
    }

    @Override public void dropPartitions(String databaseName, String tableName, List<String> partNames)
            throws MetaException, NoSuchObjectException
    {
        for (String partName : partNames)
        {
            dropPartition(databaseName, tableName,
                    Warehouse.getPartValuesFromPartName(partName));
        }
    }

    public boolean addRole(String roleName, String ownerName)
            throws InvalidObjectException, MetaException, NoSuchObjectException
    {
        Role role = new Role();
        role.setOwnerName(ownerName);
        role.setRoleName(roleName);
        metaStorePersister.save(Role.metaDataMap, role, CassandraClientConfiguration.META_DB_ROW_KEY);
        return true;
    }

    public Role getRole(String roleName) throws NoSuchObjectException
    {
        Role role = new Role();
        role.setRoleName(roleName);
        try
        {
            metaStorePersister.load(role, CassandraClientConfiguration.META_DB_ROW_KEY);
        }
        catch (HiveMetaStoreNotFoundException nfe)
        {
            throw new NoSuchObjectException("could not find role: " + roleName);
        }
        return role;
    }

    public boolean createType(Type type)
    {
        metaStorePersister.save(Type.metaDataMap, type, CassandraClientConfiguration.META_DB_ROW_KEY);
        return true;
    }

    public Type getType(String type)
    {
        Type t = new Type();
        t.setName(type);
        try
        {
            metaStorePersister.load(t, CassandraClientConfiguration.META_DB_ROW_KEY);
        }
        catch (HiveMetaStoreNotFoundException e)
        {
            return null;
        }
        return t;
    }

    public boolean dropType(String type)
    {
        Type t = new Type();
        t.setName(type);
        metaStorePersister.remove(t, CassandraClientConfiguration.META_DB_ROW_KEY);
        return true;
    }

    public boolean commitTransaction()
    {
        // FIXME default to true for now
        return true;
    }

    @Override
    public PrincipalPrivilegeSet getColumnPrivilegeSet(String arg0,
            String arg1, String arg2, String arg3, String arg4,
            List<String> arg5) throws InvalidObjectException, MetaException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public PrincipalPrivilegeSet getDBPrivilegeSet(String arg0, String arg1,
            List<String> arg2) throws InvalidObjectException, MetaException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public PrincipalPrivilegeSet getPartitionPrivilegeSet(String arg0,
            String arg1, String arg2, String arg3, List<String> arg4)
            throws InvalidObjectException, MetaException
    {
        // TODO Auto-generated method stub
        return null;
    }

    public Partition getPartitionWithAuth(String databaseName, String tableName,
            List<String> partVals, String userName, List<String> groupNames)
            throws MetaException, NoSuchObjectException, InvalidObjectException
    {
        log.debug("in getPartitionWithAuth: databaseName: {} tableName: {} userName: {} groupNames: {}",
                  databaseName, tableName, userName, groupNames);
        return getPartition(databaseName, tableName, partVals);
    }

    public List<Partition> getPartitionsWithAuth(String databaseName, String tableName,
            short maxParts, String userName, List<String> groupNames) throws MetaException,
            NoSuchObjectException, InvalidObjectException
    {
        log.debug("in getPartitionsWithAuth: databaseName: {} tableName: {} maxParts: {} userName: {}",
                  databaseName, tableName, maxParts, userName);

        return getPartitions(databaseName, tableName, maxParts);
    }

    @Override
    public List<Partition> getPartitionsByFilter(String databaseName, String tableName,
            String filter, short maxPartitions) throws MetaException,
            NoSuchObjectException
    {
        Table table = getTable(databaseName, tableName);
        List<Partition> partitions = getPartitions(databaseName, tableName);

        List<Partition> results = new ArrayList<>();
        if (partitions == null)
            return results;

        if (log.isDebugEnabled())
            log.debug("filter: {}", filter);

        for (Partition partition : partitions)
        {
            Map<String, String> partitionName = getPartitionName(table, partition);
            boolean valid = isValid(partitionName, filter);

            if (log.isDebugEnabled())
        	{
        	    log.debug("partition name: {}", partitionName);
                log.debug("partition location: {}", partition.getSd().getLocation());
        	    log.debug("partition isValid: " + valid);
        	}

            if (valid)
                results.add(partition);
        }
        return results;
    }

    @Override  public boolean getPartitionsByExpr(String dbName, String tblName, byte[] expr,
            String defaultPartitionName, short maxParts, List<Partition> result)
    {
        try
        {
            PartitionExpressionForMetastore partitionExpressionForMetastore = new PartitionExpressionForMetastore();
            String filter = partitionExpressionForMetastore.convertExprToFilter(expr);
            List<Partition> filtered = getPartitionsByFilter(dbName, tblName, filter,maxParts);
            result.addAll(filtered);
        }
        catch (MetaException |NoSuchObjectException e)
        {
            log.error("failed to getPartitionsByExpr", e);
        }
        return false;
    }

    private enum Operator {
		LARGER_THAN (">"),
		LESS_THAN ("<"),
		LESS_THAN_OR_EQUAL ("<="),
		LARGER_THAN_OR_EQUAL (">="),
		EQUAL ("=");

		private final String value;

		Operator(String value)
		{
			this.value = value;
		}

		String getValue()
		{
			return value;
		}
	}

    private Map<String, String> getPartitionName(Table table, Partition partition)
    {
    	Map<String, String> partitionNameValueMap = new HashMap<>();
        for (int i = 0; i < table.getPartitionKeysSize(); i++)
        {
            String fieldName = table.getPartitionKeys().get(i).getName();
            String fieldValue = partition.getValues().get(i);
            partitionNameValueMap.put(fieldName, fieldValue);
        }
    	return partitionNameValueMap;
    }

    private boolean isValid(Map<String, String> partitonNamexValueMap, String filter)
    {
    	// we only support a single filter e.g. (ds = '2008-08-15')
    	// DSP-858 is opened to create a general partition filter parser
    	assert (filter.split("=").length == 2);

    	if (partitonNamexValueMap.size() == 0)
    		return false;

    	Operator opr = getOpr(filter);
    	String oprString = opr.getValue();

    	String [] pair = filter.replace("(","").replace(" " + oprString + " '", oprString).replace(")", "").replaceAll("'", "").split(oprString);

    	String value = partitonNamexValueMap.get(pair[0].trim());

    	if (value == null)
    		return false;

    	return compare(value, pair[1].trim(), opr);
    }

    private Operator getOpr(String filter)
    {
    	if (filter.contains(Operator.LARGER_THAN_OR_EQUAL.getValue()))
    		return Operator.LARGER_THAN_OR_EQUAL;

    	if (filter.contains(Operator.LESS_THAN_OR_EQUAL.getValue()))
    		return Operator.LESS_THAN_OR_EQUAL;

    	if (filter.contains(Operator.EQUAL.getValue()))
    		return Operator.EQUAL;

    	if (filter.contains(Operator.LESS_THAN.getValue()))
    		return Operator.LESS_THAN;

    	if (filter.contains(Operator.LARGER_THAN.getValue()))
    		return Operator.LARGER_THAN;

    	return null;
    }

    private boolean compare(String value, String filterValue, Operator opr)
    {

    	switch (opr)
    	{
    	     case EQUAL:
    	    	 return value.compareTo(filterValue) == 0;

    	     case LARGER_THAN:
    	    	 return value.compareTo(filterValue) > 0;

    	     case LESS_THAN:
    	    	 return value.compareTo(filterValue) < 0;

    	     case LARGER_THAN_OR_EQUAL:
    	    	 return value.compareTo(filterValue) >= 0;

    	     case LESS_THAN_OR_EQUAL:
    	    	 return value.compareTo(filterValue) <= 0;

    	     default: return false;
    	}
    }


    @Override
    public PrincipalPrivilegeSet getTablePrivilegeSet(String arg0, String arg1,
            String arg2, List<String> arg3) throws InvalidObjectException,
            MetaException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public PrincipalPrivilegeSet getUserPrivilegeSet(String arg0,
            List<String> arg1) throws InvalidObjectException, MetaException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean grantPrivileges(PrivilegeBag arg0)
            throws InvalidObjectException, MetaException, NoSuchObjectException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override public boolean revokePrivileges(PrivilegeBag privileges, boolean grantOption)
            throws InvalidObjectException, MetaException, NoSuchObjectException
    {
        return false;
    }

    @Override
    public boolean grantRole(Role arg0, String arg1, PrincipalType arg2,
            String arg3, PrincipalType arg4, boolean arg5)
            throws MetaException, NoSuchObjectException, InvalidObjectException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override public boolean revokeRole(Role role, String userName, PrincipalType principalType, boolean grantOption)
            throws MetaException, NoSuchObjectException
    {
        return false;
    }

    @Override
    public List<MTablePrivilege> listAllTableGrants(String arg0,
            PrincipalType arg1, String arg2, String arg3)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<String> listPartitionNamesByFilter(String databaseName, String tableName,
            String filter, short limit) throws MetaException
    {
        log.debug("listPartitionNamesByFilter:" + filter);

        Table table = getTable(databaseName, tableName);
        List<Partition> partitions = getPartitions(databaseName, tableName);
        List<String> results = new ArrayList<>(limit);
        if (partitions == null)
            return results;
        for (Partition partition : partitions)
        {
            if(results.size() >= limit)
                break;

            String location = partition.getSd().getLocation();
            if (isValid(getPartitionName(table, partition), filter))
                results.add(location);
        }
        return results;
    }

    @Override
    public List<MDBPrivilege> listPrincipalDBGrants(String arg0,
            PrincipalType arg1, String arg2)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<MGlobalPrivilege> listPrincipalGlobalGrants(String arg0,
            PrincipalType arg1)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<MPartitionColumnPrivilege> listPrincipalPartitionColumnGrants(
            String arg0, PrincipalType arg1, String arg2, String arg3,
            String arg4, String arg5)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<MPartitionPrivilege> listPrincipalPartitionGrants(String arg0,
            PrincipalType arg1, String arg2, String arg3, String arg4)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<MTableColumnPrivilege> listPrincipalTableColumnGrants(
            String arg0, PrincipalType arg1, String arg2, String arg3,
            String arg4)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<String> listRoleNames()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<MRoleMap> listRoles(String arg0, PrincipalType arg1)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override public List<MRoleMap> listRoleMembers(String s)
    {
        return null;
    }

    @Override
    public boolean openTransaction()
    {
        // TODO Auto-generated method stub
        return true;
    }

    @Override
    public boolean removeRole(String arg0) throws MetaException,
            NoSuchObjectException
    {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean revokePrivileges(PrivilegeBag arg0)
            throws InvalidObjectException, MetaException, NoSuchObjectException
    {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean revokeRole(Role arg0, String arg1, PrincipalType arg2)
            throws MetaException, NoSuchObjectException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void rollbackTransaction()
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void shutdown()
    {
        // TODO Auto-generated method stub

    }

    private List<FieldSchema> convertToFieldSchemas(List<FieldSchema> mkeys) {
        List<FieldSchema> keys = null;
        if (mkeys != null) {
          keys = new ArrayList<>(mkeys.size());
          for (FieldSchema part : mkeys) {
            keys.add(new FieldSchema(part.getName(), part.getType(), part
                .getComment()));
          }
        }
        return keys;
      }

    @Override
    public void alterPartition(String databaseName, String tableName, List<String> part_vals, Partition partition)
            throws InvalidObjectException, MetaException
    {

        // Change the query to use part_vals instead of the name which is
        // redundant
        //String name = Warehouse.makePartName(convertToFieldSchemas(getTable(databaseName, tableName)
        //    .getPartitionKeys()), part_vals);
        alterPartition(databaseName, tableName, partition);
    }

    @Override
    public long cleanupEvents()
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public List<Partition> getPartitionsByNames(String databaseName, String tableName, List<String> names) throws MetaException,
            NoSuchObjectException
    {
        List<Partition> partitions = getPartitions(databaseName, tableName);
        List<Partition> results = new ArrayList<>();
        if (partitions == null)
            return results;
        for (Partition partition : partitions)
        {
            for(String name : names)
                if (partition.getSd().getLocation().equalsIgnoreCase(name))
                    results.add(partition);
        }
        return results;
    }

    @Override
    public List<Table> getTableObjectsByName(String databaseName, List<String> tableNames) throws MetaException, UnknownDBException
    {
        List<Table> results = new ArrayList<>();

        for (String tableName : tableNames)
        {
            results.add( getTable(databaseName, tableName) );
        }

        return results;
    }

    @Override
    public boolean isPartitionMarkedForEvent(String arg0, String arg1, Map<String, String> arg2, PartitionEventType arg3)
            throws MetaException, UnknownTableException, InvalidPartitionException, UnknownPartitionException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public List<String> listPartitionNamesPs(String databaseName, String tableName, List<String> maxParts, short limit)
            throws MetaException
    {
        return listPartitionNames(databaseName, tableName, limit);

    }

    @Override
    public List<Partition> listPartitionsPsWithAuth(String databaseName, String tableName, List<String> maxParts,
            short limit,
            String userName, List<String> groupNames) throws MetaException, InvalidObjectException
    {
        try
        {
            List<Partition> partitions = getPartitionsWithAuth(databaseName, tableName, limit, userName, groupNames);
            if (maxParts == null || maxParts.size() == 0)
            	return partitions;

            List<Partition> filteredPartitions = new LinkedList<>();

            for (Partition partition : partitions) {
            	String partitionName = partition.getValues() != null && partition.getValues().size() > 0 ?
            		partition.getValues().get(0): "";

            	if (maxParts.contains(partitionName))
            		filteredPartitions.add(partition);
            }
            return filteredPartitions;
        }
        catch (NoSuchObjectException e)
        {
            throw new InvalidObjectException(e.getMessage());
        }
    }

    @Override
    public List<String> listTableNamesByFilter(String databaseName, String filter, short limit) throws MetaException,
            UnknownDBException
    {
       List<String> allTables =  getAllTables(databaseName);

       List<String> filteredTabled = new ArrayList<>(limit);

       for (String table : allTables)
       {
           if (filteredTabled.size() >= limit)
               break;

           if (table.contains(filter))
               filteredTabled.add(table);
       }

       return filteredTabled;
    }

    @Override
    public Table markPartitionForEvent(String arg0, String arg1, Map<String, String> arg2, PartitionEventType arg3)
            throws MetaException, UnknownTableException, InvalidPartitionException, UnknownPartitionException
    {
        // TODO Auto-generated method stub
        return null;
    }

    private String fixFsHost(String location)
    {
        URI uri = new Path(location).toUri();
        URI localFs = FileSystem.getDefaultUri(configuration.getHadoopConfiguration());
        try
        {
            return new URI(
                    uri.getScheme(),
                    localFs.getAuthority(),
                    uri.getPath(),
                    uri.getQuery(),
                    uri.getFragment()).toString();
        }
        catch (URISyntaxException e)
        {
            throw new RuntimeException(e);
        }
    }

    private String removedHostLocalFile(String location)
    {
        // Remove host name in local file location
        if (location.startsWith("file://")) {
            String trimHost = location.substring("file://".length(), location.length());
            int slash = trimHost.indexOf("/");
            return "file:///" + trimHost.substring(slash + 1, trimHost.length());
        } else {
            return location;
        }
    }
    public void alterPartitions(String db_name, String tbl_name,
                                         List<List<String>> part_vals_list, List<Partition> new_parts)
    throws InvalidObjectException, MetaException {}

    public boolean updateTableColumnStatistics(ColumnStatistics colStats)
            throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException
    {
        return true;
    }

    public boolean updatePartitionColumnStatistics(ColumnStatistics statsObj,
                                                            List<String> partVals)
            throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException
    {
        return true;
    }

    @Override public ColumnStatistics getTableColumnStatistics(String s, String s1, List<String> list)
            throws MetaException, NoSuchObjectException
    {
        return null;
    }

    @Override public List<ColumnStatistics> getPartitionColumnStatistics(String s, String s1, List<String> list,
            List<String> list1) throws MetaException, NoSuchObjectException
    {
        return null;
    }

    public ColumnStatistics getTableColumnStatistics(String dbName, String tableName,
                                                              String colName) throws MetaException, NoSuchObjectException, InvalidInputException,
            InvalidObjectException
    {
        // TODO create a NullObject ColumnStatistics to use here
        return null;
    }

    public ColumnStatistics getPartitionColumnStatistics(String dbName, String tableName,
                                                                  String partName, List<String> partVals, String colName)
            throws MetaException, NoSuchObjectException, InvalidInputException, InvalidObjectException
    {
        // TODO create a NullObject ColumnStatistics to use here
        return null;
    }

    public boolean deletePartitionColumnStatistics(String dbName, String tableName,
                                                            String partName, List<String> partVals, String colName)
            throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException
    {
        return true;
    }

    public boolean deleteTableColumnStatistics(String dbName, String tableName,
                                                        String colName)
            throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException
    {
        return true;
    }

    public boolean addToken(String tokenIdentifier, String delegationToken){
        throw new UnsupportedOperationException("CassandraHiveMetaStore does not implement persistent storage of authentication tokens");
    }

    public boolean removeToken(String tokenIdentifier){
        throw new UnsupportedOperationException("CassandraHiveMetaStore does not implement persistent storage of authentication tokens");
    }

    public String getToken(String tokenIdentifier){
        throw new UnsupportedOperationException("CassandraHiveMetaStore does not implement persistent storage of authentication tokens");
    }

    public List<String> getAllTokenIdentifiers(){
        throw new UnsupportedOperationException("CassandraHiveMetaStore does not implement persistent storage of authentication tokens");
    }

    public int addMasterKey(String key) throws MetaException
    {
        throw new UnsupportedOperationException("CassandraHiveMetaStore does not implement persistent storage of authentication tokens");
    }

    public void updateMasterKey(Integer seqNo, String key)
    throws NoSuchObjectException, MetaException
    {
        throw new UnsupportedOperationException("CassandraHiveMetaStore does not implement persistent storage of authentication tokens");
    }

    public boolean removeMasterKey(Integer keySeq)
    {
        throw new UnsupportedOperationException("CassandraHiveMetaStore does not implement persistent storage of authentication tokens");
    }

    public String[] getMasterKeys()
    {
        throw new UnsupportedOperationException("CassandraHiveMetaStore does not implement persistent storage of authentication tokens");
    }

    public void verifySchema() throws MetaException
    {
        // nothing to do
    }

    public String getMetaStoreSchemaVersion() throws MetaException
    {
        return null;
    }

    public void setMetaStoreSchemaVersion(String version, String comment) throws MetaException
    {
        // nothing to do
    }

    @Override public List<HiveObjectPrivilege> listPrincipalDBGrantsAll(String s, PrincipalType principalType)
    {
        return new ArrayList<>();
    }

    @Override public List<HiveObjectPrivilege> listPrincipalTableGrantsAll(String s, PrincipalType principalType)
    {
        return new ArrayList<>();
    }

    @Override public List<HiveObjectPrivilege> listPrincipalPartitionGrantsAll(String s, PrincipalType principalType)
    {
        return new ArrayList<>();
    }

    @Override public List<HiveObjectPrivilege> listPrincipalTableColumnGrantsAll(String s, PrincipalType principalType)
    {
        return new ArrayList<>();
    }

    @Override public List<HiveObjectPrivilege> listPrincipalPartitionColumnGrantsAll(String s,
            PrincipalType principalType)
    {
        return new ArrayList<>();
    }

    @Override public List<HiveObjectPrivilege> listGlobalGrantsAll()
    {
        return new ArrayList<>();
    }

    @Override public List<HiveObjectPrivilege> listDBGrantsAll(String s)
    {
        return new ArrayList<>();
    }

    @Override public List<HiveObjectPrivilege> listPartitionColumnGrantsAll(String s, String s1, String s2, String s3)
    {
        return new ArrayList<>();
    }

    @Override public List<HiveObjectPrivilege> listTableGrantsAll(String s, String s1)
    {
        return new ArrayList<>();
    }

    @Override public List<HiveObjectPrivilege> listPartitionGrantsAll(String s, String s1, String s2)
    {
        return new ArrayList<>();
    }

    @Override public List<HiveObjectPrivilege> listTableColumnGrantsAll(String s, String s1, String s2)
    {
        return new ArrayList<>();
    }

    @Override public void createFunction(Function function) throws InvalidObjectException, MetaException
    {
        log.debug("createFunction with {}", function);
        metaStorePersister.save(Function.metaDataMap, function, function.getDbName());
    }

    @Override public void alterFunction(String dbName, String funcName, Function newFunction)
            throws InvalidObjectException, MetaException
    {
        log.debug("Altering funcName {} on datbase: {} new Function: {}",
                funcName, dbName, newFunction.getFunctionName());

        if (!funcName.equals(newFunction.getFunctionName()))
        {
            try
            {
                dropFunction(dbName, funcName);
            }
            catch(NoSuchObjectException |InvalidInputException e)
            {
                throw new MetaException(e.getMessage());
            }
        }
        createFunction(newFunction);
    }

    @Override public void dropFunction(String dbName, String funcName)
            throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException
    {
        Function function = getFunction(dbName, funcName);
        metaStorePersister.remove(function, dbName);
    }

    @Override public Function getFunction(String dbName, String funcName) throws MetaException
    {
        Function function = new Function();
        function.setDbName(dbName);
        function.setFunctionName(funcName);
        try
        {
            metaStorePersister.load(function, dbName);
            return function;
        }
        catch (HiveMetaStoreNotFoundException e)
        {
            return null;
        }
    }

    @Override public List<String> getFunctions(String dbName, String functionNamePattern) throws MetaException
    {
        if (schemaManagerService.getSystemKeyspaces().contains(dbName))
            return new ArrayList<>();

        log.info("in getFunctions with dbName: {} and functionNamePattern: {}", dbName, functionNamePattern);

        List<TBase> functions = metaStorePersister.find(new Function(), dbName);
        List<String> results = new ArrayList<>(functions.size());
        for (TBase tBase : functions)
        {
            if ("*".equals(functionNamePattern))
                functionNamePattern = "";
            Function function = (Function) tBase;
            if (StringUtils.isEmpty(functionNamePattern) ||
                    function.getFunctionName().matches(functionNamePattern))
            {
                log.debug("Adding function: {}", function);
                results.add(function.getFunctionName());
            }
        }
        return results;
    }

    @Override public AggrStats get_aggr_stats_for(String dbName, String tblName, List<String> partNames,
            List<String> colNames) throws MetaException, NoSuchObjectException
    {
        return null;
    }

    /**
     * Get the next notification event.
     *
     * @param rqst Request containing information on the last processed notification.
     * @return list of notifications, sorted by eventId
     */
    @Override public NotificationEventResponse getNextNotification(NotificationEventRequest rqst)
    {
        return null;
    }

    /**
     * Add a notification entry.  This should only be called from inside the metastore
     *
     * @param event the notification to add
     */
    @Override public void addNotificationEvent(NotificationEvent event)
    {

    }

    /**
     * Remove older notification events.
     *
     * @param olderThan Remove any events older than a given number of seconds
     */
    @Override public void cleanNotificationEvents(int olderThan)
    {

    }

    /**
     * Get the last issued notification event id.  This is intended for use by the export command
     * so that users can determine the state of the system at the point of the export,
     * and determine which notification events happened before or after the export.
     *
     * @return
     */
    @Override public CurrentNotificationEventId getCurrentNotificationEventId()
    {
        return null;
    }
}
