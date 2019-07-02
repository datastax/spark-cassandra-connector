/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package com.datastax.bdp.hadoop.hive.metastore;

public class GraphVertexTableMetadata extends AbstractGraphMetadata
{
    public static final String POSTFIX="_vertices";

    public GraphVertexTableMetadata(String graphName)
    {
        super(graphName);
    }

    @Override
    public String getTableName()
    {
        return getGraphName() + POSTFIX;
    }

    @Override
    public String getSourceProvider()
    {
        return "com.datastax.bdp.graph.spark.sql.vertex";
    }
}
