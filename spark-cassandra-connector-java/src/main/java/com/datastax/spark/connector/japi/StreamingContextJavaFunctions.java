package com.datastax.spark.connector.japi;

import org.apache.spark.streaming.StreamingContext;

@SuppressWarnings("UnusedDeclaration")
public class StreamingContextJavaFunctions extends SparkContextJavaFunctions {
    public final StreamingContext ssc;

    StreamingContextJavaFunctions(StreamingContext ssc) {
        super(ssc.sparkContext());
        this.ssc = ssc;
    }
}
