package com.datastax.spark.connector.japi;

import com.datastax.spark.connector.PairRDDFunctions;
import com.datastax.spark.connector.util.JavaApiHelper;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;
import scala.collection.Seq;
import scala.reflect.ClassTag;

import java.util.Collection;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.classTag;

public class PairRDDJavaFunctions<K, V> extends RDDJavaFunctions<Tuple2<K, V>> {

    public final PairRDDFunctions<K, V> pairRDDFunctions;

    public PairRDDJavaFunctions(RDD<Tuple2<K, V>> rdd) {
        super(rdd);
        pairRDDFunctions = new PairRDDFunctions<>(rdd);
    }

    /**
     * Groups items with the same key, assuming the items with the same key are next to each other in the
     * collection. It does not perform shuffle, therefore it is much faster than using much more
     * universal Spark RDD `groupByKey`. For this method to be useful with Cassandra tables, the key must
     * represent a prefix of the primary key, containing at least the partition key of the Cassandra
     * table.
     */
    public JavaPairRDD<K, Collection<V>> spanByKey(ClassTag<K> keyClassTag) {
        ClassTag<Tuple2<K, Collection<V>>> tupleClassTag = classTag(Tuple2.class);
        ClassTag<Collection<V>> vClassTag = classTag(Collection.class);
        RDD<Tuple2<K, Collection<V>>> newRDD = pairRDDFunctions.spanByKey()
                .map(JavaApiHelper.<K, V, Seq<V>>valuesAsJavaCollection(), tupleClassTag);

        return new JavaPairRDD<>(newRDD, keyClassTag, vClassTag);
    }
}
