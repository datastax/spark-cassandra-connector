package com.datastax.spark.connector;

import com.datastax.spark.connector.mapper.ColumnMapper;
import com.datastax.spark.connector.rdd.CassandraRDD;
import com.datastax.spark.connector.rdd.reader.ClassBasedRowReaderFactory;
import com.datastax.spark.connector.rdd.reader.RowReaderFactory;
import org.apache.spark.SparkContext;
import scala.reflect.ClassTag;
import scala.reflect.api.TypeTags;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static com.datastax.spark.connector.util.JavaApiHelper.*;

public class SparkContextJavaFunctions {
    public final SparkContext sparkContext;
    private final SparkContextFunctions scf;

    SparkContextJavaFunctions(SparkContext sparkContext) {
        this.sparkContext = sparkContext;
        this.scf = new SparkContextFunctions(sparkContext);
    }

    public <T extends Serializable> CassandraRDD<T> cassandraTable(String keyspace, String table, RowReaderFactory<T> rowReaderFactory, Class<T> targetClass) {
        ClassTag<T> ct = getClassTag(targetClass);

        return scf.cassandraTable(keyspace, table, ct, rowReaderFactory);
    }

    public CassandraRDD<CassandraRow> cassandraTable(String keyspace, String table) {
        RowReaderFactory<CassandraRow> rtf = genericRowReaderFactory();

        return cassandraTable(keyspace, table, rtf, CassandraRow.class);
    }

    public <T extends Serializable> CassandraRDD<T> cassandraTable(String keyspace, String table, ColumnMapper<T> columnMapper, Class<T> targetClass) {
        TypeTags.TypeTag<T> tt = getTypeTag(targetClass);
        RowReaderFactory<T> rtf = new ClassBasedRowReaderFactory<>(tt, columnMapper);

        return cassandraTable(keyspace, table, rtf, targetClass);
    }

    public <T extends Serializable> CassandraRDD<T> cassandraTable(String keyspace, String table, Class<T> targetClass) {
        ClassTag<T> ct = getClassTag(targetClass);
        ColumnMapper<T> cm = javaBeanColumnMapper(targetClass, new HashMap<String, String>());

        return cassandraTable(keyspace, table, cm, targetClass);
    }

    public <T extends Serializable> CassandraRDD<T> cassandraTable(String keyspace, String table, Class<T> targetClass, Map<String, String> columnNameOverride) {
        ClassTag<T> ct = getClassTag(targetClass);
        ColumnMapper<T> cm = javaBeanColumnMapper(targetClass, columnNameOverride);

        return cassandraTable(keyspace, table, cm, targetClass);
    }

}
