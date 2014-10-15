package com.datastax.spark.connector;

import com.datastax.spark.connector.rdd.CassandraJavaPairRDD;
import com.datastax.spark.connector.rdd.CassandraJavaRDD;
import com.datastax.spark.connector.rdd.CassandraRDD;
import com.datastax.spark.connector.rdd.CassandraRDD$;
import com.datastax.spark.connector.rdd.reader.KeyValueRowReaderFactory;
import com.datastax.spark.connector.rdd.reader.RowReaderFactory;
import org.apache.spark.SparkContext;
import scala.Tuple2;

import static com.datastax.spark.connector.util.JavaApiHelper.getClassTag;

@SuppressWarnings("UnusedDeclaration")
public class SparkContextJavaFunctions {
    public final SparkContext sparkContext;
    private final SparkContextFunctions scf;

    SparkContextJavaFunctions(SparkContext sparkContext) {
        this.sparkContext = sparkContext;
        this.scf = new SparkContextFunctions(sparkContext);
    }

    /**
     * Converts {@code CassandraRDD} into {@code CassandraJavaRDD}.
     */
    public <T> CassandraJavaRDD<T> toJavaRDD(CassandraRDD<T> rdd, Class<T> targetClass) {
        return new CassandraJavaRDD<>(rdd, targetClass);
    }

    /**
     * Converts {@code CassandraRDD} of {@code Tuple2} into {@code CassandraJavaPairRDD}.
     */
    public <K, V> CassandraJavaPairRDD<K, V> toJavaPairRDD(CassandraRDD<Tuple2<K, V>> rdd,
                                                           Class<K> keyClass, Class<V> valueClass) {
        return new CassandraJavaPairRDD<>(rdd, keyClass, valueClass);
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
     *
     * @since 1.0.0
     */
    public CassandraJavaRDD<CassandraJavaRow> cassandraTable(String keyspace, String table) {
        RowReaderFactory<CassandraJavaRow> rtf = GenericJavaRowReaderFactory.instance;
        return cassandraTable(keyspace, table, rtf);
    }

    /**
     * @param keyspace Cassandra keyspace
     * @param table    Cassandra table
     * @param rrf      a row reader factory to convert Cassandra rows into values of type {@code T}
     * @param <T>      target value type
     * @return {@link com.datastax.spark.connector.rdd.CassandraJavaRDD} of objects of type {@code T}
     * @since 1.1.0
     */
    public <T> CassandraJavaRDD<T> cassandraTable(String keyspace, String table, RowReaderFactory<T> rrf) {
        CassandraRDD<T> rdd = CassandraRDD$.MODULE$.apply(sparkContext, keyspace, table, getClassTag(rrf.targetClass()), rrf);
        return new CassandraJavaRDD<>(rdd, rrf.targetClass());
    }

    /**
     * @param keyspace Cassandra keyspace
     * @param table    Cassandra table
     * @param keyRRF   a row reader factory to convert Cassandra rows into keys of type {@code K}
     * @param valueRRF a row reader factory to convert Cassandra rows into values of type {@code V}
     * @param <K>      key type
     * @param <V>      value type
     * @return {@link com.datastax.spark.connector.rdd.CassandraJavaPairRDD} of {@code K}, {@code V} pairs
     * @since 1.1.0
     */
    public <K, V> CassandraJavaPairRDD<K, V> cassandraTable(String keyspace, String table, RowReaderFactory<K> keyRRF, RowReaderFactory<V> valueRRF) {
        KeyValueRowReaderFactory<K, V> rrf = new KeyValueRowReaderFactory<>(keyRRF, valueRRF);

        CassandraRDD<Tuple2<K, V>> rdd = CassandraRDD$.MODULE$.apply(sparkContext, keyspace, table,
                getClassTag(keyRRF.targetClass()), getClassTag(valueRRF.targetClass()), rrf);

        return new CassandraJavaPairRDD<>(rdd, keyRRF.targetClass(), valueRRF.targetClass());
    }


}
