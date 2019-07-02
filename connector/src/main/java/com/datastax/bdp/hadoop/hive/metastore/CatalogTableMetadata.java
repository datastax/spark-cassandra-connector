/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package com.datastax.bdp.hadoop.hive.metastore;

import org.apache.hadoop.hive.metastore.api.SerDeInfo;

/**
 * Common interface for all type source tables: C* table, MV, graph vertex table, graph edge table
 */
public interface CatalogTableMetadata
{
    String getTableName();

    String getDbName();

    String getSourceProvider();

    SerDeInfo getSerDeInfo();
}
