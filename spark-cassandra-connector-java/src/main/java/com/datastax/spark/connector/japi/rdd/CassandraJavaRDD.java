package com.datastax.spark.connector.japi.rdd;

import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.mapper.IndexedByNameColumnRef;
import com.datastax.spark.connector.rdd.CassandraRDD;
import com.datastax.spark.connector.rdd.ReadConf;
import com.datastax.spark.connector.util.JavaApiHelper;
import org.apache.spark.api.java.JavaRDD;
import scala.reflect.ClassTag;

import static com.datastax.spark.connector.util.JavaApiHelper.getClassTag;
import static com.datastax.spark.connector.util.JavaApiHelper.toScalaSeq;

/**
 * A Java API wrapper over {@link com.datastax.spark.connector.rdd.CassandraRDD} to provide Spark Cassandra Connector
 * functionality in Java.
 *
 * <p>The wrapper can be obtained by one of the methods of {@link com.datastax.spark.connector.japi.SparkContextJavaFunctions}
 * or {@link com.datastax.spark.connector.japi.StreamingContextJavaFunctions}.</p>
 */
public class CassandraJavaRDD<R> extends JavaRDD<R> {

    public CassandraJavaRDD(CassandraRDD<R> rdd, Class<R> clazz) {
        super(rdd, getClassTag(clazz));
    }

    public CassandraJavaRDD(CassandraRDD<R> rdd, ClassTag<R> classTag) {
        super(rdd, classTag);
    }

    @Override
    public CassandraRDD<R> rdd() {
        return (CassandraRDD<R>) super.rdd();
    }

    /**
     * Narrows down the selected set of columns.
     *
     * <p>Use this for better performance, when you don't need all the columns in the result RDD. When called multiple
     * times, it selects the subset of the already selected columns, so after a column was removed by the previous
     * {@code select} call, it is not possible to add it back.</p>
     */
    public CassandraJavaRDD<R> select(String... columnNames) {
        // explicit type argument is intentional and required here
        //noinspection RedundantTypeArguments
        CassandraRDD<R> newRDD = rdd().select(JavaApiHelper.<IndexedByNameColumnRef>toScalaSeq(CassandraJavaUtil.convert(columnNames)));
        return new CassandraJavaRDD<>(newRDD, classTag());
    }

    /**
     * Narrows down the selected set of columns.
     *
     * <p>Use this for better performance, when you don't need all the columns in the result RDD. When called multiple
     * times, it selects the subset of the already selected columns, so after a column was removed by the previous
     * {@code select} call, it is not possible to add it back.</p>
     */
    public CassandraJavaRDD<R> select(IndexedByNameColumnRef... selectionColumns) {
        // explicit type argument is intentional and required here
        //noinspection RedundantTypeArguments
        CassandraRDD<R> newRDD = rdd().select(JavaApiHelper.<IndexedByNameColumnRef>toScalaSeq(selectionColumns));
        return new CassandraJavaRDD<>(newRDD, classTag());
    }

    /**
     * Adds a CQL {@code WHERE} predicate(s) to the query.
     *
     * <p>Useful for leveraging secondary indexes in Cassandra. Implicitly adds an {@code ALLOW FILTERING} clause to the
     * {@code WHERE} clause, however beware that some predicates might be rejected by Cassandra, particularly in cases
     * when they filter on an unindexed, non-clustering column.</p>
     */
    public CassandraJavaRDD<R> where(String cqlWhereClause, Object... args) {
        CassandraRDD<R> newRDD = rdd().where(cqlWhereClause, toScalaSeq(args));
        return new CassandraJavaRDD<>(newRDD, classTag());
    }

    /**
     * Returns the columns to be selected from the table.
     */
    public IndexedByNameColumnRef[] selectedColumnRefs() {
        // explicit type cast is intentional and required here
        //noinspection RedundantCast
        return (IndexedByNameColumnRef[]) rdd().selectedColumnRefs().<IndexedByNameColumnRef>toArray(getClassTag(IndexedByNameColumnRef.class));
    }

    /**
     * Returns the names of columns to be selected from the table.
     */
    public String[] selectedColumnNames() {
        // explicit type cast is intentional and required here
        //noinspection RedundantCast
        return (String[]) rdd().selectedColumnNames().<String>toArray(getClassTag(String.class));
    }

    /**
     * Returns a copy of this RDD with connector changed to the specified one.
     */
    public CassandraJavaRDD<R> withConnector(CassandraConnector connector) {
        CassandraRDD<R> newRDD = rdd().withConnector(connector);
        return new CassandraJavaRDD<>(newRDD, classTag());
    }

    /**
     * Returns a copy of this RDD with read configuration changed to the specified one.
     */
    public CassandraJavaRDD<R> withReadConf(ReadConf config) {
        CassandraRDD<R> newRDD = rdd().withReadConf(config);
        return new CassandraJavaRDD<>(newRDD, classTag());
    }
}
