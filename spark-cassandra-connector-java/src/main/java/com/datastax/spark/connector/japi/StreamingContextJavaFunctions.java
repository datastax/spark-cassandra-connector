package com.datastax.spark.connector.japi;

import org.apache.spark.streaming.StreamingContext;

/**
 * Java API wrapper over {@link org.apache.spark.streaming.StreamingContext} to provide Spark Cassandra Connector
 * functionality.
 *
 * <p>To obtain an instance of this wrapper, use one of the factory methods in {@link
 * com.datastax.spark.connector.japi.CassandraJavaUtil} class.</p>
 */
@SuppressWarnings("UnusedDeclaration")
public class StreamingContextJavaFunctions extends SparkContextJavaFunctions {
    public final StreamingContext ssc;

    StreamingContextJavaFunctions(StreamingContext ssc) {
        super(ssc.sparkContext());
        this.ssc = ssc;
    }
}
