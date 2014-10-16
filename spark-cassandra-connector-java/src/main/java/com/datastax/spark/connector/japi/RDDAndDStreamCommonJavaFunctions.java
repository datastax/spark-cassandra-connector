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

    public abstract CassandraConnector defaultConnector();

    protected abstract SparkConf getConf();

    public WriteConf defaultWriteConf() {
        return WriteConf.fromSparkConf(getConf());
    }

    protected abstract void saveToCassandra(String keyspace, String table, RowWriterFactory<T> rowWriterFactory,
                                         ColumnSelector columnNames, WriteConf conf, CassandraConnector connector);

    /**
     * @deprecated this method will be removed in future release.
     */
    @Deprecated
    public void saveToCassandra(String keyspace, String table, RowWriterFactory<T> rowWriterFactory, ColumnSelector columnNames) {
        new WriterBuilder<>(this, keyspace, table, rowWriterFactory, columnNames, defaultConnector(), defaultWriteConf()).saveToCassandra();
    }

    public WriterBuilder<T> writerBuilder(String keyspaceName, String tableName, RowWriterFactory<T> rowWriterFactory) {
        return new WriterBuilder<>(this, keyspaceName, tableName, rowWriterFactory, allColumns, defaultConnector(), defaultWriteConf());
    }

    public static class WriterBuilder<T> implements Serializable {
        public final RDDAndDStreamCommonJavaFunctions<T> functions;
        public final String keyspaceName;
        public final String tableName;
        public final RowWriterFactory<T> rowWriterFactory;
        public final ColumnSelector columnSelector;
        public final CassandraConnector connector;
        public final WriteConf writeConf;

        public WriterBuilder(RDDAndDStreamCommonJavaFunctions<T> functions, String keyspaceName, String tableName,
                             RowWriterFactory<T> rowWriterFactory, ColumnSelector columnSelector, CassandraConnector connector, WriteConf writeConf) {
            this.functions = functions;
            this.keyspaceName = keyspaceName;
            this.tableName = tableName;
            this.rowWriterFactory = rowWriterFactory;
            this.columnSelector = columnSelector;
            this.connector = connector;
            this.writeConf = writeConf;
        }

        public WriterBuilder<T> withConnector(CassandraConnector connector) {
            return new WriterBuilder<>(functions, keyspaceName, tableName, rowWriterFactory, columnSelector, connector, writeConf);
        }

        public WriterBuilder<T> withWriteConf(WriteConf conf) {
            return new WriterBuilder<>(functions, keyspaceName, tableName, rowWriterFactory, columnSelector, connector, conf);
        }

        public WriterBuilder<T> withRowWriterFactory(RowWriterFactory<T> factory) {
            return new WriterBuilder<>(functions, keyspaceName, tableName, factory, columnSelector, connector, writeConf);
        }

        public WriterBuilder<T> withColumnSelector(ColumnSelector columnSelector) {
            return new WriterBuilder<>(functions, keyspaceName, tableName, rowWriterFactory, columnSelector, connector, writeConf);
        }

        public WriterBuilder<T> withBatchSize(BatchSize batchSize) {
            return new WriterBuilder<>(functions, keyspaceName, tableName, rowWriterFactory, columnSelector, connector,
                    new WriteConf(batchSize, writeConf.consistencyLevel(), writeConf.parallelismLevel())
            );
        }

        public WriterBuilder<T> withConsistencyLevel(ConsistencyLevel consistencyLevel) {
            return new WriterBuilder<>(functions, keyspaceName, tableName, rowWriterFactory, columnSelector, connector,
                    new WriteConf(writeConf.batchSize(), consistencyLevel, writeConf.parallelismLevel())
            );
        }

        public WriterBuilder<T> withParallelismLevel(int parallelismLevel) {
            return new WriterBuilder<>(functions, keyspaceName, tableName, rowWriterFactory, columnSelector, connector,
                    new WriteConf(writeConf.batchSize(), writeConf.consistencyLevel(), parallelismLevel)
            );
        }

        public void saveToCassandra() {
            functions.saveToCassandra(keyspaceName, tableName, rowWriterFactory, columnSelector, writeConf, connector);
        }

    }
}
