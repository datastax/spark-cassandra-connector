package com.datastax.spark.connector;

import com.datastax.spark.connector.streaming.DStreamFunctions;
import com.datastax.spark.connector.util.JavaApiHelper;
import com.datastax.spark.connector.writer.RowWriterFactory;
import org.apache.spark.streaming.dstream.DStream;

import static com.datastax.spark.connector.util.JavaApiHelper.getClassTag;

@SuppressWarnings("UnusedDeclaration")
public class DStreamJavaFunctions<T> extends RDDAndDStreamCommonJavaFunctions<T> {
    public final DStream<T> rdd;
    private final DStreamFunctions<T> dsf;

    DStreamJavaFunctions(DStream<T> dStream, Class<T> targetClass) {
        super(targetClass);
        this.rdd = dStream;
        this.dsf = new DStreamFunctions<>(dStream, getClassTag(targetClass));
    }

    @Override
    public void saveToCassandra(String keyspace, String table, RowWriterFactory<T> rowWriterFactory) {
        dsf.saveToCassandra(keyspace, table, rowWriterFactory);
    }

    @Override
    public void saveToCassandra(String keyspace, String table, String[] columnNames, RowWriterFactory<T> rowWriterFactory) {
        // explicit type argument is intentional and required here
        //noinspection RedundantTypeArguments
        dsf.saveToCassandra(keyspace, table, JavaApiHelper.<String>toScalaSeq(columnNames), rowWriterFactory);
    }

    @Override
    public void saveToCassandra(String keyspace, String table, String[] columnNames, int batchSize, RowWriterFactory<T> rowWriterFactory) {
        // explicit type argument is intentional and required here
        //noinspection RedundantTypeArguments
        dsf.saveToCassandra(keyspace, table, JavaApiHelper.<String>toScalaSeq(columnNames), batchSize, rowWriterFactory);
    }
}
