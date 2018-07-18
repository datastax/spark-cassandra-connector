/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.hadoop.hive.metastore;

import com.datastax.driver.core.Cluster;
import com.datastax.bdp.constants.DseSchemaConstants;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.spark.sql.SparkSession;

import java.util.*;

public class TableCreationQueryGenerator
{
    private final SchemaManagerService schemaManagerService;

    private CassandraClientConfiguration configuration;

    public TableCreationQueryGenerator(Cluster cluster, SparkSession spark)
    {
        configuration = new CassandraClientConfiguration(new HiveConf());
        schemaManagerService = SchemaManagerService.getInstance(null, configuration, cluster);
        schemaManagerService.refreshMetadata();
        schemaManagerService.setActiveSession(spark);
    }
    public String showAllCreateTables()
    {
        StringBuilder sb = new StringBuilder();
        try
        {
            Map<String, List<CatalogTableMetadata>> tableOrViewMetadatas = Maps.newConcurrentMap();
            for (CatalogTableMetadata catalogTableMetadata : schemaManagerService.getAllTableOrViewMetadatas())
            {
                String ksName = catalogTableMetadata.getDbName();
                // don't show schema for any system keyspaces
                if (schemaManagerService.getSystemKeyspaces().contains(ksName))
                    continue;

                List<CatalogTableMetadata>  tables = tableOrViewMetadatas.get(ksName);
                if (tables == null)
                {
                    tables = Lists.newArrayList();
                    tableOrViewMetadatas.put(ksName, tables);
                }
                tables.add(catalogTableMetadata);
            }

            for (Map.Entry<String, List<CatalogTableMetadata>> entry : tableOrViewMetadatas.entrySet())
            {
                sb.append(showCreateDatabase(entry.getKey()));
                for (CatalogTableMetadata catalogTableMetadata : entry.getValue())
                    sb.append(showCreateTable(schemaManagerService.buildSparkSourceTable(catalogTableMetadata)));
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        return sb.toString();
    }

    public String showCreateTable(List<String> keyspaces, List<String> excludeTables, List<String> tables)
    {
        StringBuilder sb = new StringBuilder();
        for (String ksName : keyspaces)
        {
            // don't show schema for any system keyspaces
            if (schemaManagerService.getSystemKeyspaces().contains(ksName))
                continue;

            sb.append(showCreateTable(ksName, excludeTables, tables));
        }
        return sb.toString();
    }

    private String showCreateTable(String ksName, List<String> excludeTables, List<String> tables)
    {
        // don't show schema for any system keyspaces
        if (schemaManagerService.getSystemKeyspaces().contains(ksName))
            return "";

        StringBuilder sb = new StringBuilder(showCreateDatabase(ksName));
        try
        {
            Collection<CatalogTableMetadata> catalogTableMetadatas = schemaManagerService.getTableOrViewMetadatas(ksName);
            for (CatalogTableMetadata catalogTableMetadata : catalogTableMetadatas)
            {
                String tableName = catalogTableMetadata.getTableName();
                if (!excludeTables.contains(tableName) && (tables.size() == 0 || tables.contains(tableName)))
                    sb.append(showCreateTable(schemaManagerService.buildSparkSourceTable(catalogTableMetadata)));
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        return sb.toString();
    }

    private String showCreateDatabase(String ksName)
    {
        // don't show schema for any system keyspaces
        if (schemaManagerService.getSystemKeyspaces().contains(ksName))
            return "";

        return String.format("CREATE DATABASE IF NOT EXISTS %s;\n\n", ksName) + String.format("USE %s;\n\n", ksName);
    }

    private String showCreateTable(Table table)
    {
        return isSourceTable(table) ? showSparkCreateTable(table) : "";
    }

    private boolean isSourceTable(Table table)
    {
        return table.getParameters().containsKey("spark.sql.sources.provider");
    }

    private String showSparkCreateTable(Table table)
    {
        StringBuilder results = new StringBuilder();

        results.append("CREATE TABLE ");
        results.append(table.getTableName()).append(" \n");

        Map<String, String> tableparams = table.getParameters();
        String sourceProvider = tableparams.get("spark.sql.sources.provider");
        if (sourceProvider == null)
            throw new RuntimeException("spark.sql.sources.provider is missing in the table property");
        results.append("       USING ").append(sourceProvider).append(" \n");

        StorageDescriptor sd = table.getSd();
        SerDeInfo ser = sd.getSerdeInfo();
        if (ser.getParametersSize() > 0)
        {
            results.append("       OPTIONS ( \n");
            List<String> options = new ArrayList<>();
            for (Map.Entry<String,String> entry: ser.getParameters().entrySet() )
                options.add(String.format("                %s \"%s\"", entry.getKey(), entry.getValue()));
            results.append(join(options, ",\n"));
            results.append(");\n\n");
        }
        else
        {
            throw new RuntimeException("source table option settings are missing");
        }
        return results.toString();
    }

    private String join(List<? extends CharSequence> s, String delimiter) {
        int capacity = 0;
        int delimLength = delimiter.length();
        Iterator<? extends CharSequence> iter = s.iterator();
        if (iter.hasNext()) {
            capacity += iter.next().length() + delimLength;
        }

        StringBuilder buffer = new StringBuilder(capacity);
        iter = s.iterator();
        if (iter.hasNext()) {
            buffer.append(iter.next());
            while (iter.hasNext()) {
                buffer.append(delimiter);
                buffer.append(iter.next());
            }
        }
        return buffer.toString();
    }
}
