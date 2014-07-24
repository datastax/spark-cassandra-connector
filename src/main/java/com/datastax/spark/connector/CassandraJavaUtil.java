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

    public static SparkContextJavaFunctions javaFunctions(SparkContext sparkContext) {
        return new SparkContextJavaFunctions(sparkContext);
    }

    public static SparkContextJavaFunctions javaFunctions(JavaSparkContext sparkContext) {
        return new SparkContextJavaFunctions(JavaSparkContext.toSparkContext(sparkContext));
    }

    public static StreamingContextJavaFunctions javaFunctions(StreamingContext streamingContext) {
        return new StreamingContextJavaFunctions(streamingContext);
    }

    public static StreamingContextJavaFunctions javaFunctions(JavaStreamingContext streamingContext) {
        return new StreamingContextJavaFunctions(streamingContext.ssc());
    }

    public static <T> RDDJavaFunctions javaFunctions(RDD<T> rdd, Class<T> targetClass) {
        return new RDDJavaFunctions<>(rdd, targetClass);
    }

    public static <T> RDDJavaFunctions javaFunctions(JavaRDD<T> rdd, Class<T> targetClass) {
        return new RDDJavaFunctions<>(JavaRDD.toRDD(rdd), targetClass);
    }

    public static <T> DStreamJavaFunctions javaFunctions(DStream<T> dStream, Class<T> targetClass) {
        return new DStreamJavaFunctions<>(dStream, targetClass);
    }

    public static <T> DStreamJavaFunctions javaFunctions(JavaDStream<T> dStream, Class<T> targetClass) {
        return new DStreamJavaFunctions<>(dStream.dstream(), targetClass);
    }


}
