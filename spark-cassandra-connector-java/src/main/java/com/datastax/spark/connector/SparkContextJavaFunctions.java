package com.datastax.spark.connector;

import com.datastax.spark.connector.mapper.ColumnMapper;
import com.datastax.spark.connector.rdd.CassandraJavaRDD;
import com.datastax.spark.connector.rdd.reader.ClassBasedRowReaderFactory;
import com.datastax.spark.connector.rdd.reader.RowReaderFactory;
import org.apache.spark.SparkContext;
import scala.reflect.ClassTag;
import scala.reflect.api.TypeTags;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static com.datastax.spark.connector.util.JavaApiHelper.*;

@SuppressWarnings("UnusedDeclaration")
public class SparkContextJavaFunctions {
    public final SparkContext sparkContext;
    private final SparkContextFunctions scf;

    SparkContextJavaFunctions(SparkContext sparkContext) {
        this.sparkContext = sparkContext;
        this.scf = new SparkContextFunctions(sparkContext);
    }

    /**
     * Returns a view of a Cassandra table as a {@code CassandraJavaRDD}. With this method, a
     * {@link com.datastax.spark.connector.rdd.reader.RowReader} created by the provided
     * {@link com.datastax.spark.connector.rdd.reader.RowReaderFactory} is used to produce
     * object of {@code targetClass} for each fetched row.
     *
     * @see #cassandraTable(String, String)
     */
    public <T extends Serializable> CassandraJavaRDD<T> cassandraTable(String keyspace, String table,
            RowReaderFactory<T> rowReaderFactory, Class<T> targetClass) {
        ClassTag<T> ct = getClassTag(targetClass);

        return scf.cassandraTable(keyspace, table, ct, rowReaderFactory).toJavaRDD();
    }

    /**
     * Returns a view of a Cassandra table as a {@code CassandraJavaRDD}. With this method, each row
     * is converted to a {@code CassandraRow} object.
     * <p/>
     * Example:
     * <pre>
     * CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };
     * CREATE TABLE test.words (word text PRIMARY KEY, count int);
     * INSERT INTO test.words (word, count) VALUES ('foo', 20);
     * INSERT INTO test.words (word, count) VALUES ('bar', 20);
     * ...
     *
     * // Obtaining RDD of CassandraRow objects:
     * CassandraJavaRDD<CassandraRow> rdd = CassandraJavaUtil.javaFunctions(jsc).cassandraTable("test", "words");
     * rdd.first().getString("word");     // foo
     * rdd.first().getInt("count");       // 20
     * </pre>
     */
    public CassandraJavaRDD<CassandraRow> cassandraTable(String keyspace, String table) {
        RowReaderFactory<CassandraRow> rtf = genericRowReaderFactory();

        return cassandraTable(keyspace, table, rtf, CassandraRow.class);
    }

    /**
     * Returns a view of a Cassandra table as a {@code CassandraJavaRDD}. With this method, each row
     * is converted into the instance of {@code targetClass} with use of the provided custom
     * {@link com.datastax.spark.connector.mapper.ColumnMapper}. By default,
     * {@link com.datastax.spark.connector.mapper.JavaBeanColumnMapper} is used to map object
     * properties into column names.
     *
     * @see #cassandraTable(String, String, Class)
     */
    public <T extends Serializable> CassandraJavaRDD<T> cassandraTable(String keyspace, String table,
            ColumnMapper<T> columnMapper, Class<T> targetClass) {

        TypeTags.TypeTag<T> tt = getTypeTag(targetClass);
        RowReaderFactory<T> rtf = new ClassBasedRowReaderFactory<>(tt, columnMapper);

        return cassandraTable(keyspace, table, rtf, targetClass);
    }

    /**
     * Returns a view of a Cassandra table as a {@code CassandraJavaRDD}. With this method, each row
     * is converted into the instance of {@code targetClass}.
     * <p/>
     * Example:
     * <pre>
     * CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };
     * CREATE TABLE test.words (word text PRIMARY KEY, count int);
     * INSERT INTO test.words (word, count) VALUES ('foo', 20);
     * INSERT INTO test.words (word, count) VALUES ('bar', 20);
     *
     * // all the Java classes which you want to use with Spark should be serializable
     * public class WordCount implements Serializable {
     *     private String word;
     *     private Integer count;
     *     private String other;
     *
     *     // it is very important to implement no-args constructor in a Java class which is to be used
     *     // with the Connector
     *     public WordCount() {
     *     }
     *
     *     // other constructors, getters, etc.
     *
     *     public void setWord(String word) {
     *         this.word = word;
     *     }
     *
     *     public void setCount(Integer count) {
     *         this.count = count;
     *     }
     *
     *     public void setOther(String other) {
     *         this.other = other;
     *     }
     * }
     *
     * // Obtaining RDD of {@code targetClass} objects:
     * CassandraJavaRDD<WordCount> rdd = CassandraJavaUtil.javaFunctions(jsc)
     *      .cassandraTable("test", "words", WordCount.class);
     * rdd.first().getWord();     // foo
     * rdd.first().getCount();    // 20
     * </pre>
     */
    public <T extends Serializable> CassandraJavaRDD<T> cassandraTable(String keyspace, String table, Class<T> targetClass) {
        ClassTag<T> ct = getClassTag(targetClass);
        ColumnMapper<T> cm = javaBeanColumnMapper(ct, new HashMap<String, String>());

        return cassandraTable(keyspace, table, cm, targetClass);
    }

    /**
     * Returns a view of a Cassandra table as a {@code CassandraJavaRDD}. With this method, each row
     * is converted into the instance of {@code targetClass}. It works just like
     * {@link #cassandraTable(String, String, Class)} but it additionally allows the specification of
     * a custom property to column name mappings.
     *
     * @see #cassandraTable(String, String, Class, java.util.Map)
     */
    public <T extends Serializable> CassandraJavaRDD<T> cassandraTable(String keyspace, String table,
            Class<T> targetClass, Map<String, String> columnNameOverride) {

        ClassTag<T> ct = getClassTag(targetClass);
        ColumnMapper<T> cm = javaBeanColumnMapper(ct, columnNameOverride);

        return cassandraTable(keyspace, table, cm, targetClass);
    }

}
