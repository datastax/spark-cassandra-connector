package com.datastax.spark.connector.japi.rdd;

import com.datastax.spark.connector.NamedColumnRef;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.rdd.ReadConf;

public interface CassandraCommonJavaRDDLike<ThisType> {
    /**
     * Narrows down the selected set of columns.
     *
     * <p>Use this for better performance, when you don't need all the columns in the result RDD. When called multiple
     * times, it selects the subset of the already selected columns, so after a column was removed by the previous
     * {@code select} call, it is not possible to add it back.</p>
     */
    ThisType select(String columnName1, String columnName2, String... columnNames);

    /**
     * Narrows down the selected set of columns.
     * @see #select(String, String, String...)
     */
    ThisType select(String columnName);

    /**
     * Narrows down the selected set of columns.
     * @see #select(String, String, String...)
     */
    ThisType selectRefs(NamedColumnRef columnRef1, NamedColumnRef columnRef2, NamedColumnRef... columnRefs);

    /**
     * Narrows down the selected set of columns.
     * @see #select(String, String, String...)
     */
    ThisType selectRefs(NamedColumnRef columnRef);

    /**
     * Adds a CQL {@code WHERE} predicate(s) to the query.
     *
     * <p>Useful for leveraging secondary indexes in Cassandra. Implicitly adds an {@code ALLOW FILTERING} clause to the
     * {@code WHERE} clause, however beware that some predicates might be rejected by Cassandra, particularly in cases
     * when they filter on an unindexed, non-clustering column.</p>
     */
    ThisType where(String cqlWhereClause, Object arg1, Object arg2, Object... args);

    /**
     * Adds a CQL {@code WHERE} predicate(s) to the query.
     * @see #where(String, Object, Object, Object...)
     */
    ThisType where(String cqlWhereClause, Object arg);

    /**
     * Adds a CQL {@code WHERE} predicate(s) to the query.
     * @see #where(String, Object, Object, Object...)
     */
    ThisType where(String cqlWhereClause);

    /**
     * Forces the rows within a selected Cassandra partition to be returned in ascending order
     * (if possible).
     */
    ThisType withAscOrder();

    /**
     * Forces the rows within a selected Cassandra partition to be returned in descending order
     * (if possible).
     */
    ThisType withDescOrder();

    /**
     * Adds the limit clause to CQL select statement. The limit will be applied for each created
     * Spark partition. In other words, unless the data are fetched from a single Cassandra partition
     * the number of results is unpredictable.
     * <p/>
     * The main purpose of passing limit clause is to fetch top n rows from a single Cassandra
     * partition when the table is designed so that it uses clustering keys and a partition key
     * predicate is passed to the where clause.
     */
    ThisType limit(long rowsNumber);

    /**
     * Returns the names of columns to be selected from the table.
     */
    String[] selectedColumnNames();

    /**
     * Returns the references of columns to be selected from the table.
     */
    NamedColumnRef[] selectedColumnRefs();

    /**
     * Returns a copy of this RDD with connector changed to the specified one.
     */
    ThisType withConnector(CassandraConnector connector);

    /**
     * Returns a copy of this RDD with read configuration changed to the specified one.
     */
    ThisType withReadConf(ReadConf config);

    /**
     * Produces the empty CassandraRDD which has the same signature and properties, but it does not
     * perform any validation and it does not even try to return any rows.
     */
    ThisType toEmptyCassandraRDD();

}
