package com.datastax.spark.connector.japi.rdd;

import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.NamedColumnRef;
import com.datastax.spark.connector.rdd.CassandraRDD;
import com.datastax.spark.connector.rdd.ReadConf;
import scala.Tuple2;
import scala.reflect.ClassTag;

import static com.datastax.spark.connector.util.JavaApiHelper.toScalaSeq;

/**
 * A Java API wrapper over {@link com.datastax.spark.connector.rdd.CassandraRDD} of tuples to provide Spark Cassandra
 * Connector functionality in Java.
 *
 * <p>The wrapper can be obtained by one of the methods of {@link com.datastax.spark.connector.japi.SparkContextJavaFunctions}
 * or {@link com.datastax.spark.connector.japi.StreamingContextJavaFunctions}.</p>
 */
public interface CassandraJavaPairRDDLike<K, V, ThisType extends CassandraJavaPairRDDLike<K, V, ThisType>> {

    public abstract ClassTag<K> kClassTag();
    public abstract ClassTag<V> vClassTag();
    public abstract CassandraRDD<Tuple2<K, V>> rdd();

    public abstract ThisType select(String... columnNames);
    public abstract ThisType selectRefs(NamedColumnRef... selectionColumns);
    public abstract ThisType where(String cqlWhereClause, Object... args);
    public abstract String[] selectedColumnNames();
    public abstract ThisType withCassandraConnector(CassandraConnector connector);
    public abstract ThisType withReadConf(ReadConf config);
}
