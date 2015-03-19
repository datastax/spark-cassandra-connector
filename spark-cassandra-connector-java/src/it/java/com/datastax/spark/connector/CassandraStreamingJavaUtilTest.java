package com.datastax.spark.connector;

import com.datastax.spark.connector.japi.DStreamJavaFunctions;
import com.datastax.spark.connector.japi.StreamingContextJavaFunctions;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.DStream;
import org.junit.Test;

import static com.datastax.spark.connector.japi.CassandraStreamingJavaUtil.*;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class CassandraStreamingJavaUtilTest
{

    @Test
    public void testJavaFunctions2() throws Exception {
        StreamingContext ssc = mock(StreamingContext.class);
        StreamingContextJavaFunctions scjf = javaFunctions(ssc);
        assertThat(scjf.ssc, is(ssc));
    }

    @Test
    public void testJavaFunctions3() throws Exception {
        JavaStreamingContext jsc = mock(JavaStreamingContext.class);
        StreamingContext ssc = mock(StreamingContext.class);
        when(jsc.ssc()).thenReturn(ssc);
        StreamingContextJavaFunctions scjf = javaFunctions(jsc);
        assertThat(scjf.ssc, is(ssc));
    }

    @Test
    public void testJavaFunctions6() throws Exception {
        DStream ds = mock(DStream.class);
        DStreamJavaFunctions dsjf = javaFunctions(ds);
        assertThat(dsjf.dstream, is(ds));
    }

    @Test
    public void testJavaFunctions7() throws Exception {
        JavaDStream jds = mock(JavaDStream.class);
        DStream dstream = mock(DStream.class);
        when(jds.dstream()).thenReturn(dstream);
        DStreamJavaFunctions dsjf = javaFunctions(jds);
        assertThat(dsjf.dstream, is(jds.dstream()));
    }
}