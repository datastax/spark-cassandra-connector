/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.bdp.hadoop.hive.metastore;

import com.datastax.oss.driver.api.core.metadata.schema.RelationMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.ViewMetadata;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;

public class TableOrViewMetadata implements CatalogTableMetadata
{

    private RelationMetadata metadata;

    public TableOrViewMetadata(TableMetadata metadata)
    {
        this.metadata = metadata;
    }

    public TableOrViewMetadata(ViewMetadata metadata)
    {
        this.metadata = metadata;
    }

    @Override
    public String getTableName() {
        return metadata.getName().asInternal();
    }

    @Override
    public String getDbName() {
        return metadata.getKeyspace().asInternal();
    }

    public String getKeyspace() {
        return metadata.getKeyspace().asInternal();
    }

    @Override
    public String getSourceProvider() {
        return "org.apache.spark.sql.cassandra";
    }

    @Override
    public SerDeInfo getSerDeInfo() {
        SerDeInfo serde = new SerDeInfo();
        //Set source table options as serde parameters
        serde.putToParameters("table", getTableName());
        serde.putToParameters("keyspace", getDbName());
        serde.putToParameters("pushdown", "true");
        return serde;
    }
}
