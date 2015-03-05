package com.datastax.spark.connector.japi.rdd;

import scala.Tuple2;
import scala.reflect.ClassTag;

import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.NamedColumnRef;
import com.datastax.spark.connector.rdd.CassandraRDD;
import com.datastax.spark.connector.rdd.ReadConf;

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

    /**
     * Narrows down the selected set of columns.
     *
     * <p>Use this for better performance, when you don't need all the columns in the result RDD. When called multiple
     * times, it selects the subset of the already selected columns, so after a column was removed by the previous
     * {@code select} call, it is not possible to add it back.</p>
     */
    public abstract ThisType select(String... columnNames);

    public abstract ThisType selectRefs(NamedColumnRef... selectionColumns);

    /**
     * Adds a CQL {@code WHERE} predicate(s) to the query.
     *
     * <p>Useful for leveraging secondary indexes in Cassandra. Implicitly adds an {@code ALLOW FILTERING} clause to the
     * {@code WHERE} clause, however beware that some predicates might be rejected by Cassandra, particularly in cases
     * when they filter on an unindexed, non-clustering column.</p>
     */
    public abstract ThisType where(String cqlWhereClause, Object... args);

    /**
     * Forces the rows within a selected Cassandra partition to be returned in ascending order
     * (if possible).
     */
    public abstract ThisType withAscOrder();

    /**
     * Forces the rows within a selected Cassandra partition to be returned in descending order
     * (if possible).
     */
    public abstract ThisType withDescOrder();

    /**
     * Adds the limit clause to CQL select statement. The limit will be applied for each created
     * Spark partition. In other words, unless the data are fetched from a single Cassandra partition
     * the number of results is unpredictable.
     * <p/>
     * The main purpose of passing limit clause is to fetch top n rows from a single Cassandra
     * partition when the table is designed so that it uses clustering keys and a partition key
     * predicate is passed to the where clause.
     */
    public abstract ThisType limit(long rowsNumber);

    /**
     * Returns the names of columns to be selected from the table.
     */
    public abstract String[] selectedColumnNames();

    /**
     * Returns a copy of this RDD with connector changed to the specified one.
     */
    public abstract ThisType withCassandraConnector(CassandraConnector connector);

    /**
     * Returns a copy of this RDD with read configuration changed to the specified one.
     */
    public abstract ThisType withReadConf(ReadConf config);

    /**
     * Produces the empty CassandraRDD which has the same signature and properties, but it does not
     * perform any validation and it does not even try to return any rows.
     */
    public abstract ThisType toEmptyCassandraRDD();
}
