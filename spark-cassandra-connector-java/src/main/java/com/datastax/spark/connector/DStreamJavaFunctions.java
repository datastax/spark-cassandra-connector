package com.datastax.spark.connector;

import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.streaming.DStreamFunctions;
import com.datastax.spark.connector.writer.RowWriterFactory;
import com.datastax.spark.connector.writer.WriteConf;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.dstream.DStream;

@SuppressWarnings("UnusedDeclaration")
public class DStreamJavaFunctions<T> extends RDDAndDStreamCommonJavaFunctions<T> {
    public final DStream<T> dstream;
    private final DStreamFunctions<T> dsf;

    DStreamJavaFunctions(DStream<T> dStream) {
        this.dstream = dStream;
        this.dsf = new DStreamFunctions<>(dStream);
    }

    @Override
    protected CassandraConnector defaultConnector() {
        return dsf.connector();
    }

    @Override
    protected SparkConf getConf() {
        return dstream.ssc().conf();
    }

    @Override
    public void saveToCassandra(String keyspace, String table, RowWriterFactory<T> rowWriterFactory,
                                ColumnSelector columnNames, WriteConf conf, CassandraConnector connector) {
        dsf.saveToCassandra(keyspace, table, columnNames, conf, connector, rowWriterFactory);
    }
}
