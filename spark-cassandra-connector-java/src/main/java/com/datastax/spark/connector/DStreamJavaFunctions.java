package com.datastax.spark.connector;

import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.streaming.DStreamFunctions;
import com.datastax.spark.connector.util.JavaApiHelper;
import com.datastax.spark.connector.writer.RowWriterFactory;
import com.datastax.spark.connector.writer.WriteConf;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.dstream.DStream;
import scala.reflect.ClassTag;

@SuppressWarnings("UnusedDeclaration")
public class DStreamJavaFunctions<T> extends RDDAndDStreamCommonJavaFunctions<T> {
    public final DStream<T> rdd;
    private final DStreamFunctions<T> dsf;

    DStreamJavaFunctions(DStream<T> dStream, ClassTag<T> classTag) {
        super(classTag);
        this.rdd = dStream;
        this.dsf = new DStreamFunctions<>(dStream, classTag);
    }


    private SparkConf getConf() {
        return rdd.context().conf();
    }

    private WriteConf getDefaultWriteConf() {
        return WriteConf.fromSparkConf(getConf());
    }

    private CassandraConnector getConnector() {
        return CassandraConnector.apply(getConf());
    }
    /**
     * @inheritDoc
     */
    @Override
    public void saveToCassandra(String keyspace, String table, RowWriterFactory<T> rowWriterFactory) {
        // explicit type argument is intentional and required here
        //noinspection RedundantTypeArguments
        dsf.saveToCassandra(keyspace, table,
                JavaApiHelper.allColumns(), getDefaultWriteConf(), getConnector(), rowWriterFactory);
    }

    /**
     * @inheritDoc
     */
    @Override
    public void saveToCassandra(String keyspace, String table, String[] columnNames, RowWriterFactory<T> rowWriterFactory) {
        // explicit type argument is intentional and required here
        //noinspection RedundantTypeArguments
        dsf.saveToCassandra(keyspace, table,
                JavaApiHelper.<String>toColumns(columnNames), getDefaultWriteConf(), getConnector(), rowWriterFactory);
    }

    /**
     * @inheritDoc
     */
    @Override
    public void saveToCassandra(String keyspace, String table, String[] columnNames, WriteConf writeConf, RowWriterFactory<T> rowWriterFactory) {
        // explicit type argument is intentional and required here
        //noinspection RedundantTypeArguments
        dsf.saveToCassandra(keyspace, table,
                JavaApiHelper.<String>toColumns(columnNames), writeConf, getConnector(), rowWriterFactory);
    }
}
