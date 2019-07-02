package com.datastax.spark.connector.japi.rdd;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.someColumns;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.Test;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.spark.connector.rdd.CassandraJoinRDD;

@SuppressWarnings({"unchecked", "RedundantTypeArguments"})
public class CassandraJoinJavaRDDTest {

    @Test
    public void testOn() {
        CassandraJoinRDD<String, Integer> rdd = mock(CassandraJoinRDD.class);
        CassandraJoinRDD<String, Integer> rdd2 = mock(CassandraJoinRDD.class);
        when(rdd.on(someColumns("a", "b"))).thenReturn(rdd2);
        CassandraJoinJavaRDD<String, Integer> jrdd = new CassandraJoinJavaRDD<>(rdd, String.class, Integer.class);
        assertThat(jrdd.on(someColumns("a", "b")).rdd(), is(rdd2));
    }

}
