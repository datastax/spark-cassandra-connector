package com.datastax.spark.connector;

import com.datastax.spark.connector.mapper.ColumnMapper;
import com.datastax.spark.connector.rdd.CassandraRDD;
import com.datastax.spark.connector.rdd.reader.ClassBasedRowReaderFactory;
import com.datastax.spark.connector.rdd.reader.RowReaderFactory;
import com.datastax.spark.connector.util.JavaApiHelper;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import scala.reflect.ClassTag;
import scala.reflect.api.TypeTags;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static com.datastax.spark.connector.util.JavaApiHelper.*;

@SuppressWarnings("UnusedDeclaration")
public class CassandraJavaUtil {

    public final static Map<String, String> NO_OVERRIDE = new java.util.HashMap<>();

    private CassandraJavaUtil() {
        assert false;
    }

    public static SparkContextJavaFunctions javaFunctions(SparkContext sparkContext) {
        return new SparkContextJavaFunctions(sparkContext);
    }

    public static SparkContextJavaFunctions javaFunctions(JavaSparkContext sparkContext) {
        return new SparkContextJavaFunctions(JavaSparkContext.toSparkContext(sparkContext));
    }

    public static <T> RDDJavaFunctions javaFunctions(RDD<T> rdd, Class<T> targetClass) {
        return new RDDJavaFunctions<>(rdd, targetClass);
    }

    public static <T> RDDJavaFunctions javaFunctions(JavaRDD<T> rdd, Class<T> targetClass) {
        return new RDDJavaFunctions<>(JavaRDD.toRDD(rdd), targetClass);
    }

    public static class SparkContextJavaFunctions {
        public final SparkContext sparkContext;
        private final SparkContextFunctions scf;

        private SparkContextJavaFunctions(SparkContext sparkContext) {
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
            TypeTags.TypeTag<T> tt = JavaApiHelper.getTypeTag(targetClass);
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

}
