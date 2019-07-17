/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.hadoop.hive.metastore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.datastax.dse.driver.api.core.DseSession;
import com.datastax.dse.driver.api.core.graph.GraphResultSet;
import com.datastax.dse.driver.api.core.graph.ScriptGraphStatement;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.ViewMetadata;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.servererrors.SyntaxError;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.cql.CassandraConnectorConf;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.HiveExternalCatalog;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.bdp.spark.DseCassandraConnectionFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.security.UserGroupInformation;


/**
 * Service encapsulating schema management of the DSE platform. This service deals
 * with both the Hive meta store schema as well as maintaining the mappings of
 * column family and keyspace objects to meta store tables and databases respectively.
 */
public class SchemaManagerService
{
    private static final Logger log = LoggerFactory.getLogger(SchemaManagerService.class);
    private CassandraClientConfiguration configuration;
    private String wareHouseRoot;
    private CqlSession externalSession;
    private Metadata metadata;
    private Set<String> systemKeyspaces = Sets.newHashSet();//TODO FIX THIS DriverUtil.getSystemKeyspaces(null);

    /*
     * there are three states of dse_graph DB.
     * DSE graph is disabled => no dse_graph db => graphNames == None
     * DSE graph is enabled but no graph exist => dse_graph exist but empty => graphNames == Some(emptySet)
     * graphs exists => Some (Set names)
     */
    private Optional<Set<String>> graphNames = Optional.empty();

    public static final String DSE_GRAPH_DATABASE_NAME  = "dse_graph";

    // For testing
    public void setActiveSession(SparkSession spark) {
        assert(spark != null);
        SparkSession.setActiveSession(spark);
    }


    /**
     * cache meta store persisters. I expect only  the one in 99% of cases,
     * but JobServer and Hive-Thrift server can creates connects to different clusters with different
     * configurations in on JVM
     */

    private static WeakHashMap<CassandraClientConfiguration, SchemaManagerService> SchemaManagerServiceCache = new WeakHashMap<>();

    public static synchronized SchemaManagerService getInstance(CassandraHiveMetaStore metaStore,
            CassandraClientConfiguration clientConfiguration,
            CqlSession externalSession)
    {
        SchemaManagerService schemaManagerService = SchemaManagerServiceCache.get(clientConfiguration);
        if (schemaManagerService == null)
        {
            schemaManagerService = new SchemaManagerService(clientConfiguration, externalSession);
            SchemaManagerServiceCache.put(clientConfiguration, schemaManagerService);
        } else {
            // update config with latest hadoop related changes
            schemaManagerService.configuration = clientConfiguration;
        }
        return schemaManagerService;
    }

    private SchemaManagerService(CassandraClientConfiguration clientConfiguration, CqlSession externalSession)
    {
        this.configuration = clientConfiguration;
        this.wareHouseRoot = HiveConf.getVar(configuration.getHadoopConfiguration(), HiveConf.ConfVars.METASTOREWAREHOUSE);
        this.externalSession = externalSession;
    }

    public Set<String> getSystemKeyspaces()
    {
        return systemKeyspaces;
    }

    /**
     * Creates a CassandraConnector given the inherited SparkConf
     *
     * The Client configuration should only really be important for testing. In real world cases
     * all of the Hadoop configuration should be automatically set inside the SparkConf.
     */
    public static CassandraConnector getCassandraConnector(CassandraClientConfiguration configuration) {
        SparkConf base = new SparkConf(true);
        configuration.getHadoopConfiguration()
                .iterator()
                .forEachRemaining(entry -> {
                    if (entry.getKey().startsWith("spark."))
                        base.set(entry.getKey(), entry.getValue());
                    else
                        base.set("spark.hadoop." + entry.getKey(), entry.getValue());
                });

        base.set("spark.cassandra.connection.factory", DseCassandraConnectionFactory.class.getName());
        return new CassandraConnector(CassandraConnectorConf.fromSparkConf(base));
    }

    public void refreshMetadata()
    {
        log.info("Refresh cluster meta data");
        DseSession session = (DseSession) (externalSession != null ? externalSession : getCassandraConnector(configuration).openSession());

        try
        {
            metadata = session.getMetadata();
            systemKeyspaces = Sets.newHashSet(); // TODO Fix this DriverUtil.getSystemKeyspaces(session);
            // list all available graphs and check that graph is enabled
            try
            {
                GraphResultSet rs = session.execute(ScriptGraphStatement.builder("system.graphs()").build())
                        ;
                graphNames = Optional.of(StreamSupport.stream(rs.spliterator(), false)
                        .map(r -> r.asString())
                        .collect(Collectors.toSet()));
            } catch (InvalidQueryException | SyntaxError e)
            {
                //graph is not enabled or connected to OSS Cassandra so empty set
                graphNames = Optional.empty();
            }
        }
        finally
        {
            if(session != externalSession)
                session.close();
        }

    }

    public boolean isGraphEnabled() {
        return graphNames.isPresent();
    }

    private Metadata getClusterMetadata()
    {
        return metadata;
    }

    /**
     * Returns a List of Keyspace  that are not yet created as 'databases' in the Hive meta store.
     */
    public List<String> findUnmappedKeyspaces(CassandraHiveMetaStore cassandraHiveMetaStore) {
        refreshMetadata();
        // make sure no duplicate keyspaces
        Set<String> kss = new HashSet<>();
        for (KeyspaceMetadata ksMetadata : getClusterMetadata().getKeyspaces().values())
        {
            String ksName = ksMetadata.getName().asInternal();
            log.debug("Found ksDef name: {}", ksName);
            if (isInternalKeyspace(ksName) || isLegacyGraphKs(ksName) || isKeyspaceMapped(ksName, cassandraHiveMetaStore))
                continue;

            log.debug("Adding ks name from unmapped List: {}", ksName);
            kss.add(ksName);
        }
        List<String> ksList = new ArrayList<>();
        ksList.addAll(kss);
        return ksList;
    }

    private boolean isLegacyGraphKs(String ksName)
    {
        return ksName.endsWith("_pvt") || ksName.endsWith("_system") || metadata.getKeyspace(ksName + "_system") != null;
    }

    /**
     * return the keyspace for the database
     */
    public String getKeyspaceForDatabaseName(String databaseName)
    {
        for (KeyspaceMetadata ksMetadata : getClusterMetadata().getKeyspaces().values())
        {
            String ksName = ksMetadata.getName().asInternal();
            if (StringUtils.equalsIgnoreCase(ksName, databaseName))
                return ksName;
        }
        return null;
    }


    /**
     * Returns true if this keyspaceName returns a Database via
     *  {@link com.datastax.bdp.hadoop.hive.metastore.CassandraHiveMetaStore#getDatabase(String)}
     */
    public boolean isKeyspaceMapped(String keyspaceName, CassandraHiveMetaStore cassandraHiveMetaStore)
    {
        return cassandraHiveMetaStore.hasDatabase(keyspaceName);
    }

    /**
     * Creates the database based on the Keyspace's name. The tables are
     * created similarly based off the names of the column families. Column
     * family meta data will be used to define the table's fields.
     */
    public void createKeyspaceSchema(String ks,  CassandraHiveMetaStore cassandraHiveMetaStore)
    {
        if (isInternalKeyspace(ks))
            return;

        cassandraHiveMetaStore.createDatabase(buildDatabase(ks));
    }

    public boolean createKeyspaceSchemaIfNeeded(String databaseName, CassandraHiveMetaStore cassandraHiveMetaStore)
    {
        refreshMetadata();
        log.info("adding dse_graph keyspace if needed");
        if (isGraphEnabled() && databaseName.equals(DSE_GRAPH_DATABASE_NAME))
        {
            if(!isKeyspaceMapped(databaseName, cassandraHiveMetaStore))
            {
                createKeyspaceSchema(DSE_GRAPH_DATABASE_NAME, cassandraHiveMetaStore);
                return true;
            }
            else return false;
        }
        String ks = getKeyspaceForDatabaseName(databaseName);
        if (ks != null)
        {
            log.debug("Cassandra keyspace {} exists, but is not present in the metastore. Automatically creating metastore schema now.", databaseName);
            createKeyspaceSchema(ks, cassandraHiveMetaStore);
            return true;
        }
        log.debug("No Cassandra Keyspace found with the name {}. Unable to build metastore schema for non-existent keyspace", databaseName);
        return false;
    }
    /**
     * create keyspace schema  for unmapped keyspaces
     */
    public void createKeyspaceSchemasIfNeeded(CassandraHiveMetaStore cassandraHiveMetaStore)
    {
        if (configuration.isAutoCreateSchema())
        {
            try
            {
                log.info("Updating Cassandra Keyspace to Metastore Database Mapping");
                List<String> keyspaces = findUnmappedKeyspaces(cassandraHiveMetaStore);
                for (String ks : keyspaces)
                    createKeyspaceSchema(ks, cassandraHiveMetaStore);
                log.info("adding dse_graph keyspace if needed");
                if (isGraphEnabled() && !isKeyspaceMapped(DSE_GRAPH_DATABASE_NAME, cassandraHiveMetaStore))
                    createKeyspaceSchema(DSE_GRAPH_DATABASE_NAME, cassandraHiveMetaStore);
            }
            catch (Exception e)
            {
                throw new CassandraHiveMetaStoreException("Problem finding unmapped keyspaces", e);
            }
        }
    }

    /**
     * Compares the column families in the keyspace with what we have
     * in hive so far, creating tables for any that do not exist as such already.
     */
    public void createUnmappedTables(String dbName, CassandraHiveMetaStore cassandraHiveMetaStore)
    {
        // handle dse graph database separately
        if(dbName.equals(DSE_GRAPH_DATABASE_NAME)) {
            createUnmappedGraphTables(cassandraHiveMetaStore);
            return;
        }

        String ks = getKeyspaceForDatabaseName(dbName);
        log.info("Create mapping in hive db: {}, for unmapped tables from keyspace: {}", dbName, ks);

        if (ks == null || isInternalKeyspace(ks))
            return;

        try
        {
            for (CatalogTableMetadata catalogTableMetadata : getTableOrViewMetadatas(ks))
            {
                try
                {
                    if (!cassandraHiveMetaStore.hasMapping(ks, catalogTableMetadata.getTableName()))
                    {
                        createTableMapping(catalogTableMetadata, cassandraHiveMetaStore);
                    }
                }
                catch (InvalidObjectException ioe)
                {
                    throw new CassandraHiveMetaStoreException(
                            "Could not create table for CF: " + catalogTableMetadata.getTableName(), ioe);
                }
                catch (MetaException me)
                {
                    throw new CassandraHiveMetaStoreException(
                            "Problem persisting table for CF: " + catalogTableMetadata.getTableName(), me);
                }
            }
        }
        catch (Exception ex)
        {
            throw new CassandraHiveMetaStoreException(
                    "There was a problem retrieving column families for keyspace " + ks, ex);
        }
    }

    private void createUnmappedGraphTables(CassandraHiveMetaStore cassandraHiveMetaStore)
    {
        for (String graphName: graphNames.get())
        {
            try
            {
                if (!cassandraHiveMetaStore.hasMapping(DSE_GRAPH_DATABASE_NAME, graphName + GraphVertexTableMetadata.POSTFIX))
                {
                    createTableMapping(new GraphVertexTableMetadata(graphName), cassandraHiveMetaStore);
                }
                if (!cassandraHiveMetaStore.hasMapping(DSE_GRAPH_DATABASE_NAME, graphName + GraphEdgeTableMetadata.POSTFIX))
                {
                    createTableMapping(new GraphEdgeTableMetadata(graphName), cassandraHiveMetaStore);
                }
            }
            catch (InvalidObjectException ioe)
            {
                throw new CassandraHiveMetaStoreException(
                        "Could not create table for Graph: " + graphNames, ioe);
            }
            catch (MetaException me)
            {
                throw new CassandraHiveMetaStoreException(
                        "Problem persisting metadata for Graph: " + graphNames, me);
            }
        }
    }

    /**
     * If this table is external, we need to check that it still exists in
     * Cassandra. Returns true for non-external tables. Returns false if the
     * table no longer exists in Cassandra.
     */
    public boolean verifyExternalTable(Table table) throws CassandraHiveMetaStoreException
    {
        if (table.getTableType() == null ||
                !table.getTableType().equals(TableType.EXTERNAL_TABLE.toString()))
            return true;

        boolean foundName = false;
        Map<String, String> params = table.getParameters();

        // Only verify C* ks.cf if the external table was auto created
        if (! params.containsKey("auto_created"))
            return true;

        Map<String, String> serdeParameters =table.getSd().getSerdeInfo().getParameters();
        String ksName = serdeParameters.get("keyspace");
        String cfName = serdeParameters.get("table");
        try
        {
            for (CatalogTableMetadata catalogTableMetadata : getTableOrViewMetadatas(ksName))
            {
                if (StringUtils.equalsIgnoreCase(catalogTableMetadata.getTableName(), cfName))
                {
                    foundName = true;
                    break;
                }
            }
        }
        catch (Exception ex)
        {
            throw new CassandraHiveMetaStoreException(
                    "There was a problem verifying an externally mapped table", ex);
        }
        return foundName || verifyGraphExternalTable(table);
    }

    private Pattern graphTablePattern = Pattern.compile("(.*)("
            + GraphEdgeTableMetadata.POSTFIX
            + "|" + GraphVertexTableMetadata.POSTFIX + ")");

    private boolean verifyGraphExternalTable(Table table)
    {
        if(!isGraphEnabled() || !table.getDbName().equals(DSE_GRAPH_DATABASE_NAME))
            return false;

        Matcher m = graphTablePattern.matcher(table.getTableName());
        return m.matches() && graphNames.get().contains(m.group(1));
    }

    /**
     *  build database for the keyspace
     */
    private Database buildDatabase(String ks)
    {
        Database database = new Database();
        database.setLocationUri(
                new Path(wareHouseRoot,
                        ks.toLowerCase() + ".db").toString());
        database.setName(ks);
        return database;
    }
    /**
     * create appropriate hive or spark table mapping from C*
     */
    private Table createTableMapping(CatalogTableMetadata catalogTableMetadata,
                                     CassandraHiveMetaStore cassandraHiveMetaStore)
            throws InvalidObjectException, MetaException
    {
        Table table = buildSparkSourceTable(catalogTableMetadata);
        if (table != null)
            cassandraHiveMetaStore.createTable(table);
        return table;
    }

    /**
     * build a hive table to store spark source table table for the keyspace and column family
     */
    public Table buildSparkSourceTable(CatalogTableMetadata catalogTableMetadata)
    {
        Table table = new Table();
        String ksName = catalogTableMetadata.getDbName();
        String tableName = catalogTableMetadata.getTableName();

        /* We need to get the Schema that this table would have and place it in the metastore
        Otherwise we end up with intermediary nodes with incorrect schema definitions see Catalog
        Relation. Basically the Spark expects this data so we must put it
        in its proper place.
         */
        SparkSession session = SparkSession.getActiveSession().get();
        StructType schema = session
                .read()
                .format(catalogTableMetadata.getSourceProvider())
                .options(catalogTableMetadata.getSerDeInfo().getParameters())
                .load()
                .schema();

        // Translated from Scala to Java from HiveExternalCatalog in Spark 2.2
        // Serialized JSON schema string may be too long to be stored into a single metastore table
        // property. In this case, we split the JSON string and store each part as a separate table
        // property.
        int threshold = 4000; //Magic Number Copied from Spark Internal Config Param
        String schemaJsonString = schema.json();
        // Split the JSON string.
        Iterable<String> parts = Splitter.fixedLength(threshold).split(schemaJsonString);
        table.putToParameters(HiveExternalCatalog.DATASOURCE_SCHEMA_NUMPARTS(), Integer.toString(Iterables.size(parts)));
        int index = 0;
        for (String part : parts)
        {
            table.putToParameters(HiveExternalCatalog.DATASOURCE_SCHEMA_PART_PREFIX() + index, part);
            index++;
        }

        log.info("Creating external Spark table mapping for {}.{} C* table",ksName, tableName);
        table.setDbName(ksName);
        table.setTableName(tableName);
        table.setTableType(TableType.EXTERNAL_TABLE.toString());
        table.putToParameters("EXTERNAL", "TRUE");
        table.putToParameters("auto_created", "true");
        table.putToParameters("spark.sql.sources.provider", catalogTableMetadata.getSourceProvider());
        table.setPartitionKeys(Collections.<FieldSchema>emptyList());
        table.setCreateTime((int)(System.currentTimeMillis()/1000));
        try
        {
            table.setOwner(UserGroupInformation.getCurrentUser().getShortUserName());
        }
        catch (IOException e)
        {
           throw new RuntimeException(e);
        }

        table.setPrivileges(new PrincipalPrivilegeSet());
        StorageDescriptor sd = new StorageDescriptor();
        sd.setParameters(new HashMap<>());
        //Add fake columns to pass Hive table validation
        sd.addToCols(new FieldSchema("Fake", "string","Fake column for source table"));
        log.debug("create source table options");
        SerDeInfo serde = catalogTableMetadata.getSerDeInfo();
        sd.setSerdeInfo(serde);
        sd.setBucketCols(Collections.<String>emptyList());
        sd.setSortCols(Collections.<Order>emptyList());
        table.setSd(sd);

        if (log.isDebugEnabled())
            log.debug("constructed table for CF:{} {}", tableName, table.toString());
        return table;
    }
    /**
     * Get list of column families for the specified keyspace
     * from java driver cluster table
     */
    public Collection<CatalogTableMetadata> getTableOrViewMetadatas(String ksName) {
        if (!isInternalKeyspace(ksName))
            return getTableOrViewMetadatas(getClusterMetadata().getKeyspace(CqlIdentifier.fromInternal(ksName)).orElse(null)); //TODO Handle this better
        else
            return Collections.EMPTY_LIST;
    }

    public Collection<CatalogTableMetadata> getAllTableOrViewMetadatas() {
        Collection<CatalogTableMetadata> catalogTableMetadata = new LinkedList<CatalogTableMetadata>();
        for (KeyspaceMetadata ksMetadata : getClusterMetadata().getKeyspaces().values())
        {
            if (!isInternalKeyspace(ksMetadata.getName().asInternal()))
                catalogTableMetadata.addAll(getTableOrViewMetadatas(ksMetadata));
        }

        return catalogTableMetadata;
    }

    private Collection<CatalogTableMetadata> getTableOrViewMetadatas(KeyspaceMetadata ksMetadata) {
        Collection<CatalogTableMetadata> metadatas = new LinkedList<>();
        Collection<TableMetadata> tableMetadatas = (ksMetadata == null) ? Collections.<TableMetadata>emptyList() : ksMetadata.getTables().values();
        for (TableMetadata tableMetadata : tableMetadatas) {
            metadatas.add(new TableOrViewMetadata(tableMetadata));
            }
        Collection<ViewMetadata> viewMetadatas = (ksMetadata == null) ? Collections.<ViewMetadata>emptyList() : ksMetadata.getViews().values();
         for (ViewMetadata tableMetadata : viewMetadatas) {
            metadatas.add(new TableOrViewMetadata(tableMetadata));
            }
        return metadatas;
    }

    public boolean isInternalKeyspace(String ksName)
    {
        return getSystemKeyspaces().contains(ksName);
    }

    public static SimpleStatement getMetaStoreTableSchema(String keyspaceName, String tableName)
    {
        return SchemaBuilder.createTable(keyspaceName, tableName)
                .withPartitionKey("key", DataTypes.TEXT)
                .withClusteringColumn("entity", DataTypes.TEXT)
                .withColumn("value", DataTypes.BLOB)
                .build();
    }
}
