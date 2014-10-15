package com.datastax.spark.connector;

import com.datastax.driver.core.Row;
import com.datastax.spark.connector.cql.TableDef;
import com.datastax.spark.connector.rdd.reader.RowReader;
import com.datastax.spark.connector.rdd.reader.RowReaderFactory;
import com.datastax.spark.connector.rdd.reader.RowReaderOptions;
import scala.Option;
import scala.collection.Seq;

/**
 * @author Jacek Lewandowski
 */
public class GenericJavaRowReaderFactory {
    public final static RowReaderFactory<CassandraJavaRow> instance = new RowReaderFactory<CassandraJavaRow>() {
        @Override
        public RowReader<CassandraJavaRow> rowReader(TableDef table, RowReaderOptions options) {
            return JavaRowReader.instance;
        }

        @Override
        public RowReaderOptions rowReader$default$2() {
            return new RowReaderOptions(RowReaderOptions.apply$default$1());
        }

        @Override
        public Class<CassandraJavaRow> targetClass() {
            return CassandraJavaRow.class;
        }
    };


    public static class JavaRowReader implements RowReader<CassandraJavaRow> {
        public final static JavaRowReader instance = new JavaRowReader();

        private JavaRowReader() {
        }

        @Override
        public CassandraJavaRow read(Row row, String[] columnNames) {
            return RowReaderFactory.GenericRowReader$$.MODULE$.read(row, columnNames);
        }

        @Override
        public Option<Seq<String>> columnNames() {
            return RowReaderFactory.GenericRowReader$$.MODULE$.columnNames();
        }

        @Override
        public Option<Object> columnCount() {
            return RowReaderFactory.GenericRowReader$$.MODULE$.columnCount();
        }

        @Override
        public Option<Object> consecutiveColumns() {
            return RowReaderFactory.GenericRowReader$$.MODULE$.consecutiveColumns();
        }
    }

}
