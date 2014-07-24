package com.datastax.spark.connector;

import org.apache.spark.streaming.StreamingContext;

public class StreamingContextJavaFunctions extends SparkContextJavaFunctions {
    public final StreamingContext ssc;

    StreamingContextJavaFunctions(StreamingContext ssc) {
        super(ssc.sparkContext());
        this.ssc = ssc;
    }
}
