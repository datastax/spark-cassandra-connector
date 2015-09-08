package com.datastax.spark.connector.japi.rdd;

import scala.Tuple2;
import scala.collection.Seq;
import scala.reflect.ClassTag;

import org.apache.spark.rdd.RDD;

import com.datastax.spark.connector.ColumnRef;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.rdd.CassandraRDD;
import com.datastax.spark.connector.rdd.CassandraTableScanRDD;
import com.datastax.spark.connector.rdd.ReadConf;
import com.datastax.spark.connector.rdd.reader.RowReaderFactory;
import com.datastax.spark.connector.util.JavaApiHelper;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.toSelectableColumnRefs;

public class CassandraTableScanJavaRDD<R> extends CassandraJavaRDD<R> {

    public CassandraTableScanJavaRDD(CassandraTableScanRDD<R> rdd, Class<R> clazz) {
        super(rdd, clazz);
    }

    public CassandraTableScanJavaRDD(CassandraTableScanRDD<R> rdd, ClassTag<R> classTag) {
        super(rdd, classTag);
    }

    @Override
    public CassandraTableScanRDD<R> rdd() {
        return (CassandraTableScanRDD<R>) super.rdd();
    }

    @Override
    protected CassandraTableScanJavaRDD<R> wrap(CassandraRDD<R> newRDD) {
        return new CassandraTableScanJavaRDD<>((CassandraTableScanRDD<R>) newRDD, classTag());
    }

    @Override
    public CassandraTableScanJavaRDD<R> select(String... columnNames) {
        return (CassandraTableScanJavaRDD<R>) super.select(columnNames);
    }

    @Override
    public CassandraTableScanJavaRDD<R> select(ColumnRef... columns) {
        return (CassandraTableScanJavaRDD<R>) super.select(columns);
    }

    @Override
    public CassandraTableScanJavaRDD<R> where(String cqlWhereClause, Object... args) {
        return (CassandraTableScanJavaRDD<R>) super.where(cqlWhereClause, args);
    }

    @Override
    public CassandraTableScanJavaRDD<R> withAscOrder() {
        return (CassandraTableScanJavaRDD<R>) super.withAscOrder();
    }

    @Override
    public CassandraTableScanJavaRDD<R> withDescOrder() {
        return (CassandraTableScanJavaRDD<R>) super.withDescOrder();
    }

    @Override
    public CassandraTableScanJavaRDD<R> withConnector(CassandraConnector connector) {
        return (CassandraTableScanJavaRDD<R>) super.withConnector(connector);
    }

    @Override
    public CassandraTableScanJavaRDD<R> withReadConf(ReadConf config) {
        return (CassandraTableScanJavaRDD<R>) super.withReadConf(config);
    }

    @Override
    public CassandraTableScanJavaRDD<R> limit(Long rowsNumber) {
        return (CassandraTableScanJavaRDD<R>) super.limit(rowsNumber);
    }

    /**
     * Selects a subset of columns mapped to the key of a JavaPairRDD.
     * The selected columns must be available in the CassandraRDD.
     * If no selected columns are given, all available columns are selected.
     *
     * @param rrf row reader factory to convert the key to desired type K
     * @param keyClassTag class tag of K, required to construct the result JavaPairRDD
     * @param columns list of columns passed to the rrf to create the row reader,
     *                useful when the key is mapped to a tuple or a single value
     */
    public <K> CassandraJavaPairRDD<K, R> keyBy(
        RowReaderFactory<K> rrf, ClassTag<K> keyClassTag, ColumnRef... columns) {
        Seq<ColumnRef> columnRefs = JavaApiHelper.toScalaSeq(columns);
        CassandraRDD<Tuple2<K, R>> resultRDD =
                columns.length == 0
                        ? rdd().keyBy(rrf)
                        : rdd().keyBy(columnRefs, rrf);
        return new CassandraJavaPairRDD<>(resultRDD, keyClassTag, classTag());
    }

    /**
     * @see {@link #keyBy(RowReaderFactory, ClassTag, ColumnRef...)}
     */
    public <K> CassandraJavaPairRDD<K, R> keyBy(
            RowReaderFactory<K> rrf, Class<K> keyClass, ColumnRef... columns) {
        return keyBy(rrf, JavaApiHelper.getClassTag(keyClass), columns);
    }

    /**
     * @see {@link #keyBy(RowReaderFactory, ClassTag, ColumnRef...)}
     */
    public <K> CassandraJavaPairRDD<K, R> keyBy(
            RowReaderFactory<K> rrf, Class<K> keyClass, String... columnNames) {
        ColumnRef[] columnRefs = toSelectableColumnRefs(columnNames);
        return keyBy(rrf, JavaApiHelper.getClassTag(keyClass), columnRefs);
    }


    /**
     * @see {@link #keyBy(RowReaderFactory, ClassTag, ColumnRef...)}
     */
    public <K> CassandraJavaPairRDD<K, R> keyBy(RowReaderFactory<K> rrf, Class<K> keyClass) {
        return keyBy(rrf, JavaApiHelper.getClassTag(keyClass));
    }

}
