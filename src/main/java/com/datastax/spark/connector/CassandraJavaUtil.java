package com.datastax.spark.connector;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.DStream;

import java.util.Map;

@SuppressWarnings("UnusedDeclaration")
public class CassandraJavaUtil {

    public final static Map<String, String> NO_OVERRIDE = new java.util.HashMap<>();

    private CassandraJavaUtil() {
        assert false;
    }

    /**
     * A static factory method to create a {@link SparkContextJavaFunctions}
     * basing on an existing {@link SparkContext}.
     */
    public static SparkContextJavaFunctions javaFunctions(SparkContext sparkContext) {
        return new SparkContextJavaFunctions(sparkContext);
    }

    /**
     * A static factory method to create a {@link SparkContextJavaFunctions}
     * basing on an existing {@link JavaSparkContext}.
     */
    public static SparkContextJavaFunctions javaFunctions(JavaSparkContext sparkContext) {
        return new SparkContextJavaFunctions(JavaSparkContext.toSparkContext(sparkContext));
    }

    /**
     * A static factory method to create a {@link StreamingContextJavaFunctions}
     * basing on an existing {@link StreamingContext}.
     */
    public static StreamingContextJavaFunctions javaFunctions(StreamingContext streamingContext) {
        return new StreamingContextJavaFunctions(streamingContext);
    }

    /**
     * A static factory method to create a {@link StreamingContextJavaFunctions}
     * basing on an existing {@link JavaStreamingContext}.
     */
    public static StreamingContextJavaFunctions javaFunctions(JavaStreamingContext streamingContext) {
        return new StreamingContextJavaFunctions(streamingContext.ssc());
    }

    /**
     * A static factory method to create a {@link RDDJavaFunctions} basing on
     * an existing {@link RDD}.
     *
     * @param targetClass a class of elements in the provided <code>RDD</code>
     */
    public static <T> RDDJavaFunctions javaFunctions(RDD<T> rdd, Class<T> targetClass) {
        return new RDDJavaFunctions<>(rdd, targetClass);
    }

    /**
     * A static factory method to create a {@link RDDJavaFunctions} basing on
     * an existing {@link JavaRDD}.
     *
     * @param targetClass a class of elements in the provided <code>RDD</code>
     */
    public static <T> RDDJavaFunctions javaFunctions(JavaRDD<T> rdd, Class<T> targetClass) {
        return new RDDJavaFunctions<>(JavaRDD.toRDD(rdd), targetClass);
    }

    /**
     * A static factory method to create a {@link DStreamJavaFunctions} basing
     * on an existing {@link DStream}.
     *
     * @param targetClass a class of elements in the provided <code>DStream</code>
     */
    public static <T> DStreamJavaFunctions javaFunctions(DStream<T> dStream, Class<T> targetClass) {
        return new DStreamJavaFunctions<>(dStream, targetClass);
    }

    /**
     * A static factory method to create a {@link DStreamJavaFunctions} basing
     * on an existing {@link JavaDStream}.
     *
     * @param targetClass a class of elements in the provided <code>DStream</code>
     */
    public static <T> DStreamJavaFunctions javaFunctions(JavaDStream<T> dStream, Class<T> targetClass) {
        return new DStreamJavaFunctions<>(dStream.dstream(), targetClass);
    }


}
