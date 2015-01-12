package com.datastax.spark.connector.japi;

import com.datastax.spark.connector.ColumnSelector;
import com.datastax.spark.connector.RDDFunctions;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.writer.RowWriterFactory;
import com.datastax.spark.connector.writer.WriteConf;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;

/**
 * A Java API wrapper over {@link org.apache.spark.rdd.RDD} to provide Spark Cassandra Connector functionality.
 *
 * <p>To obtain an instance of this wrapper, use one of the factory methods in {@link
 * com.datastax.spark.connector.japi.CassandraJavaUtil} class.</p>
 */
@SuppressWarnings("UnusedDeclaration")
public class RDDJavaFunctions<T> extends RDDAndDStreamCommonJavaFunctions<T> {
    public final RDD<T> rdd;
    private final RDDFunctions<T> rddf;

    RDDJavaFunctions(RDD<T> rdd) {
        this.rdd = rdd;
        this.rddf = new RDDFunctions<>(rdd);
    }

    @Override
    public CassandraConnector defaultConnector() {
        return rddf.connector();
    }

    @Override
    protected SparkConf getConf() {
        return rdd.conf();
    }

    @Override
    protected void saveToCassandra(String keyspace, String table, RowWriterFactory<T> rowWriterFactory,
                                   ColumnSelector columnNames, WriteConf conf, CassandraConnector connector) {
        rddf.saveToCassandra(keyspace, table, columnNames, conf, connector, rowWriterFactory);
    }

    // TODO: Add spanBy. To do this we need to first refactor RDDJavaFunctions (probably translate to Scala),
    // because currently ClassTag information is missing and we can't call RDD#map here nor construct a
    // Scala lambda to pass to Scala API.
}
