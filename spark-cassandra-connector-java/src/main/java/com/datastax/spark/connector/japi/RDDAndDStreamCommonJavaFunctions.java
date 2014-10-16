package com.datastax.spark.connector.japi;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.spark.connector.BatchSize;
import com.datastax.spark.connector.ColumnSelector;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.writer.RowWriterFactory;
import com.datastax.spark.connector.writer.WriteConf;
import org.apache.spark.SparkConf;

import java.io.Serializable;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.allColumns;

/**
 * Java API for either {@code RDD} or {@code DStream}.
 */
@SuppressWarnings("UnusedDeclaration")
public abstract class RDDAndDStreamCommonJavaFunctions<T> {

    protected abstract CassandraConnector defaultConnector();

    protected abstract SparkConf getConf();

    private WriteConf defaultWriteConf() {
        return WriteConf.fromSparkConf(getConf());
    }

    public abstract void saveToCassandra(String keyspace, String table, RowWriterFactory<T> rowWriterFactory,
                                         ColumnSelector columnNames, WriteConf conf, CassandraConnector connector);

    public void saveToCassandra(String keyspace, String table, RowWriterFactory<T> rowWriterFactory) {
        new SaveToCassandraOptions<>(this, keyspace, table, rowWriterFactory, allColumns, defaultConnector(), defaultWriteConf()).save();
    }

    public SaveToCassandraOptions<T> toCassandra(String keyspaceName, String tableName, RowWriterFactory<T> rowWriterFactory) {
        return new SaveToCassandraOptions<>(this, keyspaceName, tableName, rowWriterFactory, allColumns, defaultConnector(), defaultWriteConf());
    }

    static class SaveToCassandraOptions<T> implements Serializable {
        public final RDDAndDStreamCommonJavaFunctions<T> functions;
        public final String keyspaceName;
        public final String tableName;
        public final RowWriterFactory<T> rowWriterFactory;
        public final ColumnSelector columnSelector;
        public final CassandraConnector connector;
        public final WriteConf writeConf;

        public SaveToCassandraOptions(RDDAndDStreamCommonJavaFunctions<T> functions, String keyspaceName, String tableName,
                                      RowWriterFactory<T> rowWriterFactory, ColumnSelector columnSelector, CassandraConnector connector, WriteConf writeConf) {
            this.functions = functions;
            this.keyspaceName = keyspaceName;
            this.tableName = tableName;
            this.rowWriterFactory = rowWriterFactory;
            this.columnSelector = columnSelector;
            this.connector = connector;
            this.writeConf = writeConf;
        }

        public SaveToCassandraOptions<T> withConnector(CassandraConnector connector) {
            return new SaveToCassandraOptions<>(functions, keyspaceName, tableName, rowWriterFactory, columnSelector, connector, writeConf);
        }

        public SaveToCassandraOptions<T> withWriteConf(WriteConf conf) {
            return new SaveToCassandraOptions<>(functions, keyspaceName, tableName, rowWriterFactory, columnSelector, connector, conf);
        }

        public SaveToCassandraOptions<T> withRowWriterFactory(RowWriterFactory<T> factory) {
            return new SaveToCassandraOptions<>(functions, keyspaceName, tableName, factory, columnSelector, connector, writeConf);
        }

        public SaveToCassandraOptions<T> withColumnSelector(ColumnSelector columnSelector) {
            return new SaveToCassandraOptions<>(functions, keyspaceName, tableName, rowWriterFactory, columnSelector, connector, writeConf);
        }

        public SaveToCassandraOptions<T> withBatchSize(BatchSize batchSize) {
            return new SaveToCassandraOptions<>(functions, keyspaceName, tableName, rowWriterFactory, columnSelector, connector,
                    new WriteConf(batchSize, writeConf.consistencyLevel(), writeConf.parallelismLevel())
            );
        }

        public SaveToCassandraOptions<T> withConsistencyLevel(ConsistencyLevel consistencyLevel) {
            return new SaveToCassandraOptions<>(functions, keyspaceName, tableName, rowWriterFactory, columnSelector, connector,
                    new WriteConf(writeConf.batchSize(), consistencyLevel, writeConf.parallelismLevel())
            );
        }

        public SaveToCassandraOptions<T> withParallelismLevel(int parallelismLevel) {
            return new SaveToCassandraOptions<>(functions, keyspaceName, tableName, rowWriterFactory, columnSelector, connector,
                    new WriteConf(writeConf.batchSize(), writeConf.consistencyLevel(), parallelismLevel)
            );
        }

        public void save() {
            functions.saveToCassandra(keyspaceName, tableName, rowWriterFactory, columnSelector, writeConf, connector);
        }

    }
}
