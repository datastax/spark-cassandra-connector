package com.datastax.spark.connector.japi;

import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.DStream;

/**
 * The main entry point to Spark Cassandra Connector Java API for Spark Streaming.
 *
 * <p>There are several helpful static factory methods which build useful wrappers around Streaming
 * Context and DStream.</p>
 */
@SuppressWarnings("UnusedDeclaration")
public class CassandraStreamingJavaUtil
{

    private CassandraStreamingJavaUtil() {
        assert false;
    }

    // -------------------------------------------------------------------------
    //              Java API wrappers factory methods
    // -------------------------------------------------------------------------

    /**
     * A static factory method to create a {@link StreamingContextJavaFunctions} based on an existing
     * {@link StreamingContext} instance.
     */
    public static StreamingContextJavaFunctions javaFunctions(StreamingContext streamingContext) {
        return new StreamingContextJavaFunctions(streamingContext);
    }

    /**
     * A static factory method to create a {@link StreamingContextJavaFunctions} based on an existing
     * {@link JavaStreamingContext} instance.
     */
    public static StreamingContextJavaFunctions javaFunctions(JavaStreamingContext streamingContext) {
        return new StreamingContextJavaFunctions(streamingContext.ssc());
    }

    /**
     * A static factory method to create a {@link DStreamJavaFunctions} based on an existing
     * {@link DStream} instance.
     */
    public static <T> DStreamJavaFunctions<T> javaFunctions(DStream<T> dStream) {
        return new DStreamJavaFunctions<>(dStream);
    }

    /**
     * A static factory method to create a {@link DStreamJavaFunctions} based on an existing
     * {@link JavaDStream} instance.
     */
    public static <T> DStreamJavaFunctions<T> javaFunctions(JavaDStream<T> dStream) {
        return new DStreamJavaFunctions<>(dStream.dstream());
    }

}
