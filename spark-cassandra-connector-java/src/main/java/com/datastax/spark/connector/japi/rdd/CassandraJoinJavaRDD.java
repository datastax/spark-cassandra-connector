package com.datastax.spark.connector.japi.rdd;

import scala.reflect.ClassTag;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.safeClassTag;

import com.datastax.spark.connector.ColumnSelector;
import com.datastax.spark.connector.rdd.CassandraJoinRDD;

public class CassandraJoinJavaRDD<K, V> extends CassandraJavaPairRDD<K, V> {

    public CassandraJoinJavaRDD(CassandraJoinRDD<K, V> rdd,
                                ClassTag<K> keyClassTag, ClassTag<V> valueClassTag) {

        super(rdd, keyClassTag, valueClassTag);
    }

    public CassandraJoinJavaRDD(CassandraJoinRDD<K, V> rdd,
                                Class<K> keyClass, Class<V> valueClass) {

        super(rdd, safeClassTag(keyClass), safeClassTag(valueClass));
    }

    private CassandraJoinJavaRDD<K, V> wrap(CassandraJoinRDD<K, V> newRDD) {
        return new CassandraJoinJavaRDD<>(newRDD, kClassTag(), vClassTag());
    }


    public CassandraJoinJavaRDD<K, V> on(ColumnSelector joinColumns) {
        CassandraJoinRDD<K, V> newRDD = rdd().on(joinColumns);
        return wrap(newRDD);
    }

    @Override
    public CassandraJoinRDD<K, V> rdd() {
        return (CassandraJoinRDD<K, V>) super.rdd();
    }
}
