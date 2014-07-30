package com.datastax.spark.connector.rdd;

import com.datastax.spark.connector.util.JavaApiHelper;
import org.apache.spark.api.java.JavaRDD;
import scala.reflect.ClassTag;

import static com.datastax.spark.connector.util.JavaApiHelper.getClassTag;
import static com.datastax.spark.connector.util.JavaApiHelper.toScalaSeq;

/**
 * A Java wrapper for {@link com.datastax.spark.connector.rdd.CassandraRDD}. Makes the invocation of
 * Cassandra related methods easy in Java.
 */
public class CassandraJavaRDD<R> extends JavaRDD<R> {

    private final Class<R> clazz;

    public CassandraJavaRDD(CassandraRDD<R> rdd, Class<R> clazz) {
        super(rdd, getClassTag(clazz));
        this.clazz = clazz;
    }

    @Override
    public ClassTag<R> classTag() {
        return getClassTag(clazz);
    }

    @Override
    public CassandraRDD<R> rdd() {
        return (CassandraRDD<R>) super.rdd();
    }

    /**
     * Narrows down the selected set of columns.
     * Use this for better performance, when you don't need all the columns in the result RDD.
     * When called multiple times, it selects the subset of the already selected columns, so
     * after a column was removed by the previous {@code select} call, it is not possible to
     * add it back.
     */
    public CassandraJavaRDD<R> select(String... columnNames) {
        // explicit type argument is intentional and required here
        //noinspection RedundantTypeArguments
        CassandraRDD<R> newRDD = rdd().select(JavaApiHelper.<String>toScalaSeq(columnNames));
        return new CassandraJavaRDD<>(newRDD, clazz);
    }

    /**
     * Adds a CQL {@code WHERE} predicate(s) to the query.
     * Useful for leveraging secondary indexes in Cassandra.
     * Implicitly adds an {@code ALLOW FILTERING} clause to the {@code WHERE} clause, however beware that some predicates
     * might be rejected by Cassandra, particularly in cases when they filter on an unindexed, non-clustering column.
     */
    public CassandraJavaRDD<R> where(String cqlWhereClause, Object... args) {
        CassandraRDD<R> newRDD = rdd().where(cqlWhereClause, toScalaSeq(args));
        return new CassandraJavaRDD<>(newRDD, clazz);
    }

    /**
     * Returns the names of columns to be selected from the table.
     */
    public String[] selectedColumnNames() {
        // explicit type cast is intentional and required here
        //noinspection RedundantCast
        return (String []) rdd().selectedColumnNames().toArray(getClassTag(String.class));
    }

}
