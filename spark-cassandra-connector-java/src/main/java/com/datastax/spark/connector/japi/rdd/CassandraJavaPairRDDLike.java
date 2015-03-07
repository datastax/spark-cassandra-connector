package com.datastax.spark.connector.japi.rdd;

import com.datastax.spark.connector.rdd.CassandraRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
import scala.reflect.ClassTag;

/**
 * A Java API wrapper over {@link com.datastax.spark.connector.rdd.CassandraRDD} of tuples to provide Spark Cassandra
 * Connector functionality in Java.
 *
 * <p>The wrapper can be obtained by one of the methods of {@link com.datastax.spark.connector.japi.SparkContextJavaFunctions}
 * or {@link com.datastax.spark.connector.japi.StreamingContextJavaFunctions}.</p>
 */
public interface CassandraJavaPairRDDLike<K, V, ThisType extends CassandraJavaPairRDDLike<K, V, ThisType>>
        extends CassandraCommonJavaRDDLike<ThisType> {

    ClassTag<K> kClassTag();

    ClassTag<V> vClassTag();

    CassandraRDD<Tuple2<K, V>> rdd();

    /**
     * Applies a function to each item, and groups consecutive items having the same value together.
     * Contrary to `groupBy`, items from the same group must be already next to each other in the
     * original collection. Works locally on each partition, so items from different
     * partitions will never be placed in the same group.
     */
    <K2> JavaPairRDD<K2, Iterable<Tuple2<K, V>>> spanBy(Function<Tuple2<K, V>, K2> f, Class<K2> keyClass);

    /**
     * Groups items with the same key, assuming the items with the same key are next to each other
     * in the collection. It does not perform shuffle, therefore it is much faster than using
     * much more universal Spark RDD `groupByKey`. For this method to be useful with Cassandra tables,
     * the key must represent a prefix of the primary key, containing at least the partition key of the
     * Cassandra table.
     */
    JavaPairRDD<K, Iterable<V>> spanByKey();

}
