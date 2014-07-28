package com.datastax.spark.connector;

import com.datastax.spark.connector.mapper.ColumnMapper;
import com.datastax.spark.connector.writer.RowWriterFactory;

import java.util.Map;

import static com.datastax.spark.connector.util.JavaApiHelper.defaultRowWriterFactory;
import static com.datastax.spark.connector.util.JavaApiHelper.javaBeanColumnMapper;

/**
 * Java API for either {@code RDD} or {@code DStream}.
 */
@SuppressWarnings("UnusedDeclaration")
public abstract class RDDAndDStreamCommonJavaFunctions<T> {
    private final Class<T> targetClass;

    RDDAndDStreamCommonJavaFunctions(Class<T> targetClass) {
        this.targetClass = targetClass;
    }

    /**
     * Saves the data from the underlying {@code RDD} or {@code DStream} to a Cassandra table.
     * This method works just like {@link #saveToCassandra(String, String)}.
     * It additionally allows the specification of a factory of custom {@link com.datastax.spark.connector.writer.RowWriter}.
     * By default, a factory of {@link com.datastax.spark.connector.writer.DefaultRowWriter} is used together with
     * an underlying {@link com.datastax.spark.connector.mapper.JavaBeanColumnMapper}.
     * <p/>
     * If the underlying data source is a {@code DStream}, all generated RDDs will be saved
     * to Cassandra as if this method was called on each of them.
     *
     * @see #saveToCassandra(String, String)
     */
    public abstract void saveToCassandra(String keyspace, String table, RowWriterFactory<T> rowWriterFactory);

    /**
     * Saves the data from the underlying {@code RDD} or {@code DStream} to a Cassandra table.
     * This method works just like {@link #saveToCassandra(String, String, String[])}.
     * It additionally allows the specification of a factory of custom {@link com.datastax.spark.connector.writer.RowWriter}.
     * By default, a factory of {@link com.datastax.spark.connector.writer.DefaultRowWriter} is used together with
     * an underlying {@link com.datastax.spark.connector.mapper.JavaBeanColumnMapper}.
     * <p/>
     * If the underlying data source is a {@code DStream}, all generated RDDs will be saved
     * to Cassandra as if this method was called on each of them.
     *
     * @see #saveToCassandra(String, String, String[])
     */
    public abstract void saveToCassandra(String keyspace, String table, String[] columnNames, RowWriterFactory<T> rowWriterFactory);

    /**
     * Saves the data from the underlying {@code RDD} or {@code DStream} to a Cassandra table in batches of given size.
     * This method works just like {@link #saveToCassandra(String, String, String[], int)}.
     * It additionally allows the specification of a factory of custom {@link com.datastax.spark.connector.writer.RowWriter}.
     * By default, a factory of {@link com.datastax.spark.connector.writer.DefaultRowWriter} is used together with
     * an underlying {@link com.datastax.spark.connector.mapper.JavaBeanColumnMapper}.
     * <p/>
     * If the underlying data source is a {@code DStream}, all generated RDDs will be saved
     * to Cassandra as if this method was called on each of them.
     *
     * @see #saveToCassandra(String, String, String[], int)
     */
    public abstract void saveToCassandra(String keyspace, String table, String[] columnNames, int batchSize, RowWriterFactory<T> rowWriterFactory);

    /**
     * Saves the data from the underlying {@code RDD} or {@code DStream} to a Cassandra table.
     * This method works just like {@link #saveToCassandra(String, String)}.
     * It additionally allows the specification of a custom column mapper. By default,
     * {@link com.datastax.spark.connector.mapper.JavaBeanColumnMapper} is used, which works
     * perfectly with Java bean-like classes.
     * <p/>
     * If the underlying data source is a {@code DStream}, all generated RDDs will be saved
     * to Cassandra as if this method was called on each of them.
     */
    public void saveToCassandra(String keyspace, String table, ColumnMapper<T> columnMapper) {
        RowWriterFactory<T> rwf = defaultRowWriterFactory(targetClass, columnMapper);
        saveToCassandra(keyspace, table, rwf);
    }

    /**
     * Saves the data from the underlying {@code RDD} or {@code DStream} to a Cassandra table.
     * This method works just like {@link #saveToCassandra(String, String, String[])}.
     * It additionally allows the specification of a custom column mapper. By default,
     * {@link com.datastax.spark.connector.mapper.JavaBeanColumnMapper} is used, which works
     * perfectly with Java bean-like classes.
     * <p/>
     * If the underlying data source is a {@code DStream}, all generated RDDs will be saved
     * to Cassandra as if this method was called on each of them.
     */
    public void saveToCassandra(String keyspace, String table, String[] columnNames, ColumnMapper<T> columnMapper) {
        RowWriterFactory<T> rwf = defaultRowWriterFactory(targetClass, columnMapper);
        saveToCassandra(keyspace, table, columnNames, rwf);
    }

    /**
     * Saves the data from the underlying {@code RDD} or {@code DStream} to a Cassandra table in batches of given size.
     * This method works just like {@link #saveToCassandra(String, String, String[], int)}.
     * It additionally allows the specification of a custom column mapper. By default,
     * {@link com.datastax.spark.connector.mapper.JavaBeanColumnMapper} is used, which works
     * perfectly with Java bean-like classes.
     * <p/>
     * If the underlying data source is a {@code DStream}, all generated RDDs will be saved
     * to Cassandra as if this method was called on each of them.
     */
    public void saveToCassandra(String keyspace, String table, String[] columnNames, int batchSize, ColumnMapper<T> columnMapper) {
        RowWriterFactory<T> rwf = defaultRowWriterFactory(targetClass, columnMapper);
        saveToCassandra(keyspace, table, columnNames, batchSize, rwf);
    }

    /**
     * Saves the data from the underlying {@code RDD} or {@code DStream} to a Cassandra table.
     * This method works just like {@link #saveToCassandra(String, String)}.
     * It additionally allows the specification of a custom RDD object property mapping to columns in Cassandra table.
     * <p/>
     * If the underlying data source is a {@code DStream}, all generated RDDs will be saved
     * to Cassandra as if this method was called on each of them.
     * <p/>
     * Example:
     * <pre>
     * CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };
     * CREATE TABLE test.words(word VARCHAR PRIMARY KEY, count INT, other VARCHAR);
     *
     * // all the Java classes which you want to use with Spark should be serializable
     * public class WordCount implements Serializable {
     *     private String w;
     *     private Integer c;
     *     private String o;
     *
     *     // constructors, setters, etc.
     *
     *     public String getW() {
     *         return w;
     *     }
     *
     *     public Integer getC() {
     *         return c;
     *     }
     *
     *     public String getO() {
     *         return o;
     *     }
     * }
     *
     * JavaSparkContext jsc = ...
     * JavaRDD<WordCount> rdd = jsc.parallelize(Arrays.asList(new WordCount("foo", 5, "bar")));
     *
     * Map<String, String> mapping = new HashMap<String, String>(3);
     * mapping.put("w", "word");
     * mapping.put("c", "count");
     * mapping.put("o", "other");
     *
     * CassandraJavaUtil.javaFunctions(rdd, WordCount.class).saveToCassandra("test", "words", mapping);
     * </pre>
     */
    public void saveToCassandra(String keyspace, String table, Map<String, String> columnNameOverride) {
        saveToCassandra(keyspace, table, javaBeanColumnMapper(targetClass, columnNameOverride));
    }

    /**
     * Saves the data from the underlying {@code RDD} or {@code DStream} to a Cassandra table.
     * This method works just like {@link #saveToCassandra(String, String, String[])}.
     * It additionally allows the specification of a custom RDD/DStream object property mapping to columns in Cassandra table.
     * <p/>
     * If the underlying data source is a {@code DStream}, all generated RDDs will be saved
     * to Cassandra as if this method was called on each of them.
     *
     * @see #saveToCassandra(String, String, java.util.Map)
     */
    public void saveToCassandra(String keyspace, String table, String[] columnNames, Map<String, String> columnNameOverride) {
        saveToCassandra(keyspace, table, columnNames, javaBeanColumnMapper(targetClass, columnNameOverride));
    }

    /**
     * Saves the data from the underlying {@code RDD} or {@code DStream} to a Cassandra table in batches of given size.
     * This method works just like {@link #saveToCassandra(String, String, String[], int)}.
     * It additionally allows the specification of a custom RDD/DStream object property mapping to columns in Cassandra table.
     * <p/>
     * If the underlying data source is a {@code DStream}, all generated RDDs will be saved
     * to Cassandra as if this method was called on each of them.
     *
     * @see #saveToCassandra(String, String, java.util.Map)
     */
    public void saveToCassandra(String keyspace, String table, String[] columnNames, int batchSize, Map<String, String> columnNameOverride) {
        saveToCassandra(keyspace, table, columnNames, batchSize, javaBeanColumnMapper(targetClass, columnNameOverride));
    }

    /**
     * Saves the data from the underlying {@code RDD} or {@code DStream} to a Cassandra table.
     * Saves all properties that have corresponding Cassandra columns.
     * The underlying RDD class must provide data for all columns.
     * <p/>
     * By default, writes are performed at {@code ConsistencyLevel.ONE} in order to leverage data-locality
     * and minimize network traffic. This write consistency level is controlled by the following property:
     * - {@code spark.cassandra.output.consistency.level}: consistency level for RDD writes, string matching
     * the {@link com.datastax.driver.core.ConsistencyLevel} enum name.
     * <p/>
     * If the underlying data source is a {@code DStream}, all generated RDDs will be saved
     * to Cassandra as if this method was called on each of them.
     * <p/>
     * Example:
     * <pre>
     * CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };
     * CREATE TABLE test.words(word VARCHAR PRIMARY KEY, count INT, other VARCHAR);
     *
     * // all the Java classes which you want to use with Spark should be serializable
     * public class WordCount implements Serializable {
     *     private String word;
     *     private Integer count;
     *     private String other;
     *
     *     // constructors, setters, etc.
     *
     *     public String getWord() {
     *         return word;
     *     }
     *
     *     public Integer getCount() {
     *         return count;
     *     }
     *
     *     public String getOther() {
     *         return other;
     *     }
     * }
     *
     * JavaSparkContext jsc = ...
     * JavaRDD<WordCount> rdd = jsc.parallelize(Arrays.asList(new WordCount("foo", 5, "bar")));
     * CassandraJavaUtil.javaFunctions(rdd, WordCount.class).saveToCassandra("test", "words");
     * </pre>
     */
    public void saveToCassandra(String keyspace, String table) {
        saveToCassandra(keyspace, table, CassandraJavaUtil.NO_OVERRIDE);
    }

    /**
     * Saves the data from the underlying {@code RDD} or {@code DStream} to a Cassandra table.
     * The {@code RDD} object properties must match Cassandra table column names.
     * Non-selected property/column names are left unchanged in Cassandra.
     * All primary key columns must be selected.
     * <p/>
     * By default, writes are performed at {@code ConsistencyLevel.ONE} in order to leverage data-locality
     * and minimize network traffic. This write consistency level is controlled by the following property:
     * - {@code spark.cassandra.output.consistency.level}: consistency level for RDD writes, string matching
     * the {@link com.datastax.driver.core.ConsistencyLevel} enum name.
     * <p/>
     * If the underlying data source is a {@code DStream}, all generated RDDs will be saved
     * to Cassandra as if this method was called on each of them.
     * <p/>
     * Example:
     * <pre>
     * CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };
     * CREATE TABLE test.words(word VARCHAR PRIMARY KEY, count INT, other VARCHAR);
     *
     * // all the Java classes which you want to use with Spark should be serializable
     * public class WordCount implements Serializable {
     *     private String word;
     *     private Integer count;
     *     private String other;
     *
     *     // constructors, setters, etc.
     *
     *     public String getWord() {
     *         return word;
     *     }
     *
     *     public Integer getCount() {
     *         return count;
     *     }
     *
     *     public String getOther() {
     *         return other;
     *     }
     * }
     *
     * JavaSparkContext jsc = ...
     * JavaRDD<WordCount> rdd = jsc.parallelize(Arrays.asList(new WordCount("foo", 5, "bar")));
     * CassandraJavaUtil.javaFunctions(rdd, WordCount.class)
     *      .saveToCassandra("test", "words", new String[] {"word", "count"}); // will not save the "other" column
     * </pre>
     */
    public void saveToCassandra(String keyspace, String table, String[] columnNames) {
        saveToCassandra(keyspace, table, columnNames, CassandraJavaUtil.NO_OVERRIDE);
    }

    /**
     * Saves the data from the underlying {@code RDD} or {@code DStream} to a Cassandra table in batches of the given size.
     * Use this overload only if you find automatically tuned batch size doesn't result in optimal performance.
     * <p/>
     * Larger batches raise memory use by temporary buffers and may incur
     * larger GC pressure on the server. Small batches would result in more roundtrips
     * and worse throughput. Typically sending a few kilobytes of data per every batch
     * is enough to achieve good performance.
     * <p/>
     * By default, writes are performed at ConsistencyLevel.ONE in order to leverage data-locality and minimize
     * network traffic. This write consistency level is controlled by the following property:
     * - {@code spark.cassandra.output.consistency.level}: consistency level for RDD writes, string matching the
     * {@link com.datastax.driver.core.ConsistencyLevel} enum name.
     * <p/>
     * If the underlying data source is a {@code DStream}, all generated RDDs will be saved
     * to Cassandra as if this method was called on each of them.
     *
     * @see #saveToCassandra(String, String)
     * @see #saveToCassandra(String, String, String[])
     */
    public void saveToCassandra(String keyspace, String table, String[] columnNames, int batchSize) {
        saveToCassandra(keyspace, table, columnNames, batchSize, CassandraJavaUtil.NO_OVERRIDE);
    }
}
