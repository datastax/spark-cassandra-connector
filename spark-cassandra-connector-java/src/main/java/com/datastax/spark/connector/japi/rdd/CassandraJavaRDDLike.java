package com.datastax.spark.connector.japi.rdd;

import com.datastax.spark.connector.rdd.CassandraRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import scala.reflect.ClassTag;

/**
 * A Java API wrapper over {@link com.datastax.spark.connector.rdd.CassandraRDD} to provide Spark Cassandra
 * Connector functionality in Java.
 *
 * <p>The wrapper can be obtained by one of the methods of {@link com.datastax.spark.connector.japi.SparkContextJavaFunctions}
 * or {@link com.datastax.spark.connector.japi.StreamingContextJavaFunctions}.</p>
 */
public interface CassandraJavaRDDLike<R, ThisType extends CassandraJavaRDDLike<R, ThisType>> 
        extends CassandraCommonJavaRDDLike<ThisType> {

    ClassTag<R> classTag();

    CassandraRDD<R> rdd();

    /**
     * Applies a function to each item, and groups consecutive items having the same value together.
     * Contrary to `groupBy`, items from the same group must be already next to each other in the
     * original collection. Works locally on each partition, so items from different
     * partitions will never be placed in the same group.
     */
    <K> JavaPairRDD<K, Iterable<R>> spanBy(Function<R, K> f, Class<K> keyClass);
}
