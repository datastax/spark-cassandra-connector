package com.datastax.spark.connector;

import com.datastax.spark.connector.util.JavaApiHelper;
import com.datastax.spark.connector.writer.RowWriterFactory;
import org.apache.spark.rdd.RDD;

import static com.datastax.spark.connector.util.JavaApiHelper.getClassTag;

@SuppressWarnings("UnusedDeclaration")
public class RDDJavaFunctions<T> extends RDDAndDStreamCommonJavaFunctions<T> {
    public final RDD<T> rdd;
    private final RDDFunctions<T> rddf;

    RDDJavaFunctions(RDD<T> rdd, Class<T> targetClass) {
        super(targetClass);
        this.rdd = rdd;
        this.rddf = new RDDFunctions<>(rdd, getClassTag(targetClass));
    }

    @Override
    public void saveToCassandra(String keyspace, String table, RowWriterFactory<T> rowWriterFactory) {
        rddf.saveToCassandra(keyspace, table, rowWriterFactory);
    }

    @Override
    public void saveToCassandra(String keyspace, String table, String[] columnNames, RowWriterFactory<T> rowWriterFactory) {
        // explicit type argument is intentional and required here
        //noinspection RedundantTypeArguments
        rddf.saveToCassandra(keyspace, table, JavaApiHelper.<String>toScalaSeq(columnNames), rowWriterFactory);
    }

    @Override
    public void saveToCassandra(String keyspace, String table, String[] columnNames, int batchSize, RowWriterFactory<T> rowWriterFactory) {
        // explicit type argument is intentional and required here
        //noinspection RedundantTypeArguments
        rddf.saveToCassandra(keyspace, table, JavaApiHelper.<String>toScalaSeq(columnNames), batchSize, rowWriterFactory);
    }
}
