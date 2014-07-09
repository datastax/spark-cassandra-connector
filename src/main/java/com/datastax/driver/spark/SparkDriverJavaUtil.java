package com.datastax.driver.spark;

import com.datastax.driver.spark.mapper.ColumnMapper;
import com.datastax.driver.spark.mapper.JavaBeanColumnMapper;
import com.datastax.driver.spark.rdd.CassandraRDD;
import com.datastax.driver.spark.rdd.reader.CassandraRow;
import com.datastax.driver.spark.rdd.reader.ClassBasedRowReaderFactory;
import com.datastax.driver.spark.rdd.reader.RowReaderFactory;
import com.datastax.driver.spark.util.JavaApiHelper;
import com.datastax.driver.spark.writer.RowWriterFactory;
import com.datastax.driver.spark.writer.RowWriterFactory$;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import scala.collection.JavaConversions;
import scala.collection.immutable.HashMap;
import scala.collection.mutable.WrappedArray$;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;
import scala.reflect.api.TypeTags;

import java.io.Serializable;
import java.util.Map;

@SuppressWarnings("UnusedDeclaration")
public class SparkDriverJavaUtil {

    public final static Map<String, String> NO_OVERRIDE = new java.util.HashMap<String, String>();

    private SparkDriverJavaUtil() {
        assert false;
    }

    public static SparkContextJavaFunctions javaFunctions(SparkContext sparkContext) {
        return new SparkContextJavaFunctions(sparkContext);
    }

    public static SparkContextJavaFunctions javaFunctions(JavaSparkContext sparkContext) {
        return new SparkContextJavaFunctions(JavaSparkContext.toSparkContext(sparkContext));
    }

    public static <T> RDDJavaFunctions javaFunctions(RDD<T> rdd, Class<T> targetClass) {
        return new RDDJavaFunctions<T>(rdd, targetClass);
    }

    public static <T> RDDJavaFunctions javaFunctions(JavaRDD<T> rdd, Class<T> targetClass) {
        return new RDDJavaFunctions<T>(JavaRDD.toRDD(rdd), targetClass);
    }

    private static <K, V> scala.collection.immutable.Map<K, V> toScalaMap(Map<K, V> map) {
        return new HashMap<K, V>().<V>$plus$plus(JavaConversions.mapAsScalaMap(map).toSeq());
    }

    private static <T> ColumnMapper<T> javaColumnMapper(Map<String, String> columnNameOverride, ClassTag<T> ct) {
        return new JavaBeanColumnMapper<T>(toScalaMap(columnNameOverride), ct);
    }

    public static class RDDJavaFunctions<T> {
        public final RDD<T> rdd;
        private final RDDFunctions<T> rddf;
        private final ClassTag<T> ct;

        private RDDJavaFunctions(RDD<T> rdd, Class<T> targetClass) {
            this.rdd = rdd;
            this.ct = ClassTag$.MODULE$.apply(targetClass);
            this.rddf = new RDDFunctions<T>(rdd, ct);
        }

        public void saveToCassandra(String keyspace, String table, RowWriterFactory<T> rowWriterFactory) {
            rddf.saveToCassandra(keyspace, table, rowWriterFactory);
        }

        public void saveToCassandra(String keyspace, String table, String[] columnNames, RowWriterFactory<T> rowWriterFactory) {
            rddf.saveToCassandra(keyspace, table, WrappedArray$.MODULE$.<String>make(columnNames).toSeq(), rowWriterFactory);
        }

        public void saveToCassandra(String keyspace, String table, String[] columnNames, int batchSize, RowWriterFactory<T> rowWriterFactory) {
            rddf.saveToCassandra(keyspace, table, WrappedArray$.MODULE$.<String>make(columnNames).toSeq(), batchSize, rowWriterFactory);
        }

        public void saveToCassandra(String keyspace, String table, ColumnMapper<T> columnMapper) {
            RowWriterFactory<T> rwf = RowWriterFactory$.MODULE$.defaultRowWriterFactory(ct, columnMapper);
            saveToCassandra(keyspace, table, rwf);
        }

        public void saveToCassandra(String keyspace, String table, String[] columnNames, ColumnMapper<T> columnMapper) {
            RowWriterFactory<T> rwf = RowWriterFactory$.MODULE$.defaultRowWriterFactory(ct, columnMapper);
            saveToCassandra(keyspace, table, columnNames, rwf);
        }

        public void saveToCassandra(String keyspace, String table, String[] columnNames, int batchSize, ColumnMapper<T> columnMapper) {
            RowWriterFactory<T> rwf = RowWriterFactory$.MODULE$.defaultRowWriterFactory(ct, columnMapper);
            saveToCassandra(keyspace, table, columnNames, batchSize, rwf);
        }

        public void saveToCassandra(String keyspace, String table, Map<String, String> columnNameOverride) {
            saveToCassandra(keyspace, table, javaColumnMapper(columnNameOverride, ct));
        }

        public void saveToCassandra(String keyspace, String table, String[] columnNames, Map<String, String> columnNameOverride) {
            saveToCassandra(keyspace, table, columnNames, javaColumnMapper(columnNameOverride, ct));
        }

        public void saveToCassandra(String keyspace, String table, String[] columnNames, int batchSize, Map<String, String> columnNameOverride) {
            saveToCassandra(keyspace, table, columnNames, batchSize, javaColumnMapper(columnNameOverride, ct));
        }
    }

    public static class SparkContextJavaFunctions {
        public final SparkContext sparkContext;
        private final SparkContextFunctions scf;

        private SparkContextJavaFunctions(SparkContext sparkContext) {
            this.sparkContext = sparkContext;
            this.scf = package$.MODULE$.toSparkContextFunctions(sparkContext);
        }

        public <T extends Serializable> CassandraRDD<T> cassandraTable(String keyspace, String table, RowReaderFactory<T> rowReaderFactory, Class<T> targetClass) {
            ClassTag<T> ct = ClassTag$.MODULE$.apply(targetClass);

            return scf.cassandraTable(keyspace, table, ct, rowReaderFactory);
        }

        public CassandraRDD<CassandraRow> cassandraTable(String keyspace, String table) {
            RowReaderFactory<CassandraRow> rtf = RowReaderFactory.GenericRowReader$$.MODULE$;

            return cassandraTable(keyspace, table, rtf, CassandraRow.class);
        }

        public <T extends Serializable> CassandraRDD<T> cassandraTable(String keyspace, String table, ColumnMapper<T> columnMapper, Class<T> targetClass) {
            TypeTags.TypeTag<T> tt = JavaApiHelper.getTypeTag(targetClass);
            RowReaderFactory<T> rtf = new ClassBasedRowReaderFactory<T>(tt, columnMapper);

            return cassandraTable(keyspace, table, rtf, targetClass);
        }

        public <T extends Serializable> CassandraRDD<T> cassandraTable(String keyspace, String table, Class<T> targetClass) {
            ClassTag<T> ct = ClassTag$.MODULE$.apply(targetClass);
            ColumnMapper<T> cm = new JavaBeanColumnMapper<T>(new HashMap<String, String>(), ct);

            return cassandraTable(keyspace, table, cm, targetClass);
        }

        public <T extends Serializable> CassandraRDD<T> cassandraTable(String keyspace, String table, Class<T> targetClass, Map<String, String> columnNameOverride) {
            ClassTag<T> ct = ClassTag$.MODULE$.apply(targetClass);
            ColumnMapper<T> cm = new JavaBeanColumnMapper<T>(toScalaMap(columnNameOverride), ct);

            return cassandraTable(keyspace, table, cm, targetClass);
        }

    }

}
