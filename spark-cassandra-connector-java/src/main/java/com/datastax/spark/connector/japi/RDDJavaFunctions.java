package com.datastax.spark.connector.japi;

import com.datastax.spark.connector.ColumnSelector;
import com.datastax.spark.connector.RDDFunctions;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.writer.RowWriterFactory;
import com.datastax.spark.connector.writer.WriteConf;
import org.apache.spark.SparkConf;
import org.apache.spark.rdd.RDD;

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

}
