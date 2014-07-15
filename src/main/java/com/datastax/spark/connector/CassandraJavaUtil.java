package com.datastax.spark.connector;

import com.datastax.spark.connector.CassandraRow;
import com.datastax.spark.connector.RDDFunctions;
import com.datastax.spark.connector.SparkContextFunctions;
import com.datastax.spark.connector.mapper.ColumnMapper;
import com.datastax.spark.connector.rdd.CassandraRDD;
import com.datastax.spark.connector.rdd.reader.ClassBasedRowReaderFactory;
import com.datastax.spark.connector.rdd.reader.RowReaderFactory;
import com.datastax.spark.connector.util.JavaApiHelper;
import com.datastax.spark.connector.writer.RowWriterFactory;
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

    public static class RDDJavaFunctions<T> {
        public final RDD<T> rdd;
        private final RDDFunctions<T> rddf;
        private final Class<T> targetClass;

        private RDDJavaFunctions(RDD<T> rdd, Class<T> targetClass) {
            this.rdd = rdd;
            this.targetClass = targetClass;
            this.rddf = new RDDFunctions<>(rdd, getClassTag(targetClass));
        }

        public void saveToCassandra(String keyspace, String table, RowWriterFactory<T> rowWriterFactory) {
            rddf.saveToCassandra(keyspace, table, rowWriterFactory);
        }

        public void saveToCassandra(String keyspace, String table, String[] columnNames, RowWriterFactory<T> rowWriterFactory) {
            //noinspection RedundantTypeArguments
            rddf.saveToCassandra(keyspace, table, JavaApiHelper.<String>toScalaSeq(columnNames), rowWriterFactory);
        }

        public void saveToCassandra(String keyspace, String table, String[] columnNames, int batchSize, RowWriterFactory<T> rowWriterFactory) {
            //noinspection RedundantTypeArguments
            rddf.saveToCassandra(keyspace, table, JavaApiHelper.<String>toScalaSeq(columnNames), batchSize, rowWriterFactory);
        }

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
            saveToCassandra(keyspace, table, NO_OVERRIDE);
        }

        public void saveToCassandra(String keyspace, String table, String[] columnNames) {
            saveToCassandra(keyspace, table, columnNames, NO_OVERRIDE);
        }

        public void saveToCassandra(String keyspace, String table, String[] columnNames, int batchSize) {
            saveToCassandra(keyspace, table, columnNames, batchSize, NO_OVERRIDE);
        }
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
