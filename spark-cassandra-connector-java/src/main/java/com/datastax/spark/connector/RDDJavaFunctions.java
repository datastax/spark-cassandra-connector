package com.datastax.spark.connector;

import com.datastax.spark.connector.util.JavaApiHelper;
import com.datastax.spark.connector.writer.RowWriterFactory;
import org.apache.spark.rdd.RDD;
import scala.reflect.ClassTag;

@SuppressWarnings("UnusedDeclaration")
public class RDDJavaFunctions<T> extends RDDAndDStreamCommonJavaFunctions<T> {
    public final RDD<T> rdd;
    private final RDDFunctions<T> rddf;

    RDDJavaFunctions(RDD<T> rdd, ClassTag<T> classTag) {
        super(classTag);
        this.rdd = rdd;
        this.rddf = new RDDFunctions<>(rdd, classTag);
    }

    /**
     * @inheritDoc
     */
    @Override
    public void saveToCassandra(String keyspace, String table, RowWriterFactory<T> rowWriterFactory) {
        // explicit type argument is intentional and required here
        //noinspection RedundantTypeArguments
        rddf.saveToCassandra(keyspace, table, rowWriterFactory);
    }

    /**
     * @inheritDoc
     */
    @Override
    public void saveToCassandra(String keyspace, String table, String[] columnNames, RowWriterFactory<T> rowWriterFactory) {
        // explicit type argument is intentional and required here
        //noinspection RedundantTypeArguments
        rddf.saveToCassandra(keyspace, table, JavaApiHelper.<String>toColumns(columnNames), rowWriterFactory);
    }

    /**
     * @inheritDoc
     */
    @Override
    public void saveToCassandra(String keyspace, String table, String[] columnNames, int batchSize, RowWriterFactory<T> rowWriterFactory) {
        // explicit type argument is intentional and required here
        //noinspection RedundantTypeArguments
        rddf.saveToCassandra(keyspace, table, JavaApiHelper.<String>toColumns(columnNames), batchSize, rowWriterFactory);
    }
}
