package com.datastax.spark.connector.japi;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.junit.Test;
import org.mockito.Mockito;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.spark.connector.rdd.ReadConf;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

public class SparkContextJavaFunctionsTest
{
    @Test
    public void testReadConfPopulating() {
        SparkConf conf = new SparkConf();
        conf.set("spark.cassandra.input.fetch.size_in_rows", "1234");
        conf.set("spark.cassandra.input.split.size_in_mb", "4321");
        conf.set("spark.cassandra.input.consistency.level", "THREE");

        SparkContext sc = Mockito.mock(SparkContext.class);
        when(sc.getConf()).thenReturn(conf);

        ReadConf readConf = CassandraJavaUtil.javaFunctions(sc).cassandraTable("a", "b").rdd().readConf();

        assertEquals(readConf.fetchSizeInRows(), 1234);
        assertEquals(readConf.splitSizeInMB(), 4321);
        assertEquals(readConf.consistencyLevel(), ConsistencyLevel.THREE);
    }
}
