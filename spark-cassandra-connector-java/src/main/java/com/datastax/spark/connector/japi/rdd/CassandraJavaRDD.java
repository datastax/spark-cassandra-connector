package com.datastax.spark.connector.japi.rdd;

import scala.collection.Seq;
import scala.reflect.ClassTag;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.toSelectableColumnRefs;
import static com.datastax.spark.connector.util.JavaApiHelper.getClassTag;
import static com.datastax.spark.connector.util.JavaApiHelper.toScalaSeq;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import com.datastax.spark.connector.ColumnRef;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.SparkContextJavaFunctions;
import com.datastax.spark.connector.japi.StreamingContextJavaFunctions;
import com.datastax.spark.connector.rdd.CassandraRDD;
import com.datastax.spark.connector.rdd.ReadConf;
import com.datastax.spark.connector.util.JavaApiHelper;

/**
 * A Java API wrapper over {@link CassandraRDD} to provide Spark Cassandra Connector functionality in
 * Java.
 * <p/>
 * The wrapper can be obtained by one of the methods of {@link SparkContextJavaFunctions} or {@link
 * StreamingContextJavaFunctions}.
 */
@SuppressWarnings({"unchecked", "UnusedDeclaration"})
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

    private CassandraJavaRDD<R> wrap(CassandraRDD<R> newRDD) {
        return new CassandraJavaRDD<>(newRDD, classTag());
    }

    /**
     * Narrows down the selected set of columns.
     * <p/>
     * Use this for better performance, when you don't need all the columns in the result RDD. When
     * called multiple times, it selects the subset of the already selected columns, so after a column
     * was removed by the previous {@code select} call, it is not possible to add it back.
     */
    public CassandraJavaRDD<R> select(String... columnNames) {
        Seq<ColumnRef> columnRefs = toScalaSeq(toSelectableColumnRefs(columnNames));
        CassandraRDD<R> newRDD = rdd().select(columnRefs);
        return wrap(newRDD);
    }

    /**
     * Narrows down the selected set of columns.
     * <p/>
     * Use this for better performance, when you don't need all the columns in the result RDD. When
     * called multiple times, it selects the subset of the already selected columns, so after a column
     * was removed by the previous {@code select} call, it is not possible to add it back.</p>
     */
    public CassandraJavaRDD<R> select(ColumnRef... columns) {
        Seq<ColumnRef> columnRefs = JavaApiHelper.toScalaSeq(columns);
        CassandraRDD<R> newRDD = rdd().select(columnRefs);
        return wrap(newRDD);
    }

    /**
     * Adds a CQL {@code WHERE} predicate(s) to the query.
     * <p/>
     * Useful for leveraging secondary indexes in Cassandra. Implicitly adds an {@code ALLOW FILTERING}
     * clause to the {@code WHERE} clause, however beware that some predicates might be rejected by
     * Cassandra, particularly in cases when they filter on an unindexed, non-clustering column.</p>
     */
    public CassandraJavaRDD<R> where(String cqlWhereClause, Object... args) {
        CassandraRDD<R> newRDD = rdd().where(cqlWhereClause, toScalaSeq(args));
        return wrap(newRDD);
    }

    /**
     * Forces the rows within a selected Cassandra partition to be returned in ascending order (if
     * possible).
     */
    public CassandraJavaRDD<R> withAscOrder() {
        CassandraRDD<R> newRDD = rdd().withAscOrder();
        return wrap(newRDD);
    }

    /**
     * Forces the rows within a selected Cassandra partition to be returned in descending order (if
     * possible).
     */
    public CassandraJavaRDD<R> withDescOrder() {
        CassandraRDD<R> newRDD = rdd().withDescOrder();
        return wrap(newRDD);
    }

    /**
     * Returns the columns to be selected from the table.
     */
    @SuppressWarnings("RedundantCast")
    public ColumnRef[] selectedColumnRefs() {
        ClassTag<ColumnRef> classTag = getClassTag(ColumnRef.class);
        return (ColumnRef[]) rdd().selectedColumnRefs().toArray(classTag);
    }

    /**
     * Returns the names of columns to be selected from the table.
     */
    @SuppressWarnings("RedundantCast")
    public String[] selectedColumnNames() {
        ClassTag<String> classTag = getClassTag(String.class);
        return (String[]) rdd().selectedColumnNames().toArray(classTag);
    }

    /**
     * Returns a copy of this RDD with connector changed to the specified one.
     */
    public CassandraJavaRDD<R> withConnector(CassandraConnector connector) {
        CassandraRDD<R> newRDD = rdd().withConnector(connector);
        return wrap(newRDD);
    }

    /**
     * Returns a copy of this RDD with read configuration changed to the specified one.
     */
    public CassandraJavaRDD<R> withReadConf(ReadConf config) {
        CassandraRDD<R> newRDD = rdd().withReadConf(config);
        return wrap(newRDD);
    }

    /**
     * Adds the limit clause to CQL select statement. The limit will be applied for each created Spark
     * partition. In other words, unless the data are fetched from a single Cassandra partition the
     * number of results is unpredictable.
     * <p/>
     * The main purpose of passing limit clause is to fetch top n rows from a single Cassandra partition
     * when the table is designed so that it uses clustering keys and a partition key predicate is passed
     * to the where clause.
     */
    public CassandraJavaRDD<R> limit(Long rowsNumber) {
        CassandraRDD<R> newRDD = rdd().limit(rowsNumber);
        return wrap(newRDD);
    }

    /**
     * Applies a function to each item, and groups consecutive items having the same value together.
     * Contrary to `groupBy`, items from the same group must be already next to each other in the
     * original collection. Works locally on each partition, so items from different partitions will
     * never be placed in the same group.
     */
    public <K> JavaPairRDD<K, Iterable<R>> spanBy(Function<R, K> f, ClassTag<K> keyClassTag) {
        return CassandraJavaUtil.javaFunctions(rdd()).spanBy(f, keyClassTag);
    }

    /**
     * @see {@link #spanBy(Function, ClassTag)}
     */
    public <K> JavaPairRDD<K, Iterable<R>> spanBy(Function<R, K> f, Class<K> keyClass) {
        return CassandraJavaUtil.javaFunctions(rdd()).spanBy(f, getClassTag(keyClass));
    }

    /**
     * Produces the empty CassandraRDD which has the same signature and properties, but it does not
     * perform any validation and it does not even try to return any rows.
     */
    public CassandraJavaRDD<R> toEmptyCassandraRDD() {
        CassandraRDD<R> newRDD = rdd().toEmptyCassandraRDD();
        return wrap(newRDD);
    }
}
