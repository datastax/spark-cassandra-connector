/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package com.datastax.bdp.hadoop.hive.metastore;


import org.apache.hadoop.hive.metastore.api.SerDeInfo;


public abstract class AbstractGraphMetadata implements CatalogTableMetadata
{
    private String graphName;

    public AbstractGraphMetadata(String graphName)
    {
        this.graphName = graphName;
    }

    @Override
    public String getDbName() {
        return SchemaManagerService.DSE_GRAPH_DATABASE_NAME;
    }

    public String getGraphName() {
        return graphName;
    }

    @Override
    public SerDeInfo getSerDeInfo() {
        SerDeInfo serde = new SerDeInfo();
        //Set source table options as serde parameters
        serde.putToParameters("graph", getGraphName());
        serde.putToParameters("pushdown", "true");
        return serde;
    }
}
