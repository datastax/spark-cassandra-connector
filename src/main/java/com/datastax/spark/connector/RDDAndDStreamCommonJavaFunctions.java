package com.datastax.spark.connector;

import com.datastax.spark.connector.mapper.ColumnMapper;
import com.datastax.spark.connector.writer.RowWriterFactory;

import java.util.Map;

import static com.datastax.spark.connector.util.JavaApiHelper.defaultRowWriterFactory;
import static com.datastax.spark.connector.util.JavaApiHelper.javaBeanColumnMapper;

@SuppressWarnings("UnusedDeclaration")
abstract class RDDAndDStreamCommonJavaFunctions<T> {
    private final Class<T> targetClass;

    RDDAndDStreamCommonJavaFunctions(Class<T> targetClass) {
        this.targetClass = targetClass;
    }

    public abstract void saveToCassandra(String keyspace, String table, RowWriterFactory<T> rowWriterFactory);

    public abstract void saveToCassandra(String keyspace, String table, String[] columnNames, RowWriterFactory<T> rowWriterFactory);

    public abstract void saveToCassandra(String keyspace, String table, String[] columnNames, int batchSize, RowWriterFactory<T> rowWriterFactory);

    public void saveToCassandra(String keyspace, String table, ColumnMapper<T> columnMapper) {
        RowWriterFactory<T> rwf = defaultRowWriterFactory(targetClass, columnMapper);
        saveToCassandra(keyspace, table, rwf);
    }

    public void saveToCassandra(String keyspace, String table, String[] columnNames, ColumnMapper<T> columnMapper) {
        RowWriterFactory<T> rwf = defaultRowWriterFactory(targetClass, columnMapper);
        saveToCassandra(keyspace, table, columnNames, rwf);
    }

    public void saveToCassandra(String keyspace, String table, String[] columnNames, int batchSize, ColumnMapper<T> columnMapper) {
        RowWriterFactory<T> rwf = defaultRowWriterFactory(targetClass, columnMapper);
        saveToCassandra(keyspace, table, columnNames, batchSize, rwf);
    }

    public void saveToCassandra(String keyspace, String table, Map<String, String> columnNameOverride) {
        saveToCassandra(keyspace, table, javaBeanColumnMapper(targetClass, columnNameOverride));
    }

    public void saveToCassandra(String keyspace, String table, String[] columnNames, Map<String, String> columnNameOverride) {
        saveToCassandra(keyspace, table, columnNames, javaBeanColumnMapper(targetClass, columnNameOverride));
    }

    public void saveToCassandra(String keyspace, String table, String[] columnNames, int batchSize, Map<String, String> columnNameOverride) {
        saveToCassandra(keyspace, table, columnNames, batchSize, javaBeanColumnMapper(targetClass, columnNameOverride));
    }

    public void saveToCassandra(String keyspace, String table) {
        saveToCassandra(keyspace, table, CassandraJavaUtil.NO_OVERRIDE);
    }

    public void saveToCassandra(String keyspace, String table, String[] columnNames) {
        saveToCassandra(keyspace, table, columnNames, CassandraJavaUtil.NO_OVERRIDE);
    }

    public void saveToCassandra(String keyspace, String table, String[] columnNames, int batchSize) {
        saveToCassandra(keyspace, table, columnNames, batchSize, CassandraJavaUtil.NO_OVERRIDE);
    }
}
