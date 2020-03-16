package com.datastax.spark.connector.japi;

import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.spark.connector.rdd.ReadConf;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

public class SparkContextJavaFunctionsTest
{
    @Test
    public void testReadConfPopulating() {
        SparkConf conf = new SparkConf();
        conf.set("spark.cassandra.input.fetch.sizeInRows", "1234");
        conf.set("spark.cassandra.input.split.sizeInMB", "4321");
        conf.set("spark.cassandra.input.consistency.level", "THREE");

        SparkContext sc = Mockito.mock(SparkContext.class);
        when(sc.getConf()).thenReturn(conf);

        ReadConf readConf = CassandraJavaUtil.javaFunctions(sc).cassandraTable("a", "b").rdd().readConf();

        assertEquals(readConf.fetchSizeInRows(), 1234);
        assertEquals(readConf.splitSizeInMB(), 4321);
        assertEquals(readConf.consistencyLevel(), DefaultConsistencyLevel.THREE);
    }
}
