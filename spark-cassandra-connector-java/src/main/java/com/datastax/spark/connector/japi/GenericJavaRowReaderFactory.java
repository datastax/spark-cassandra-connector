package com.datastax.spark.connector.japi;

import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Row;
import com.datastax.spark.connector.cql.TableDef;
import com.datastax.spark.connector.rdd.reader.RowReader;
import com.datastax.spark.connector.rdd.reader.RowReaderFactory;
import com.datastax.spark.connector.rdd.reader.RowReaderOptions;
import com.datastax.spark.connector.util.JavaApiHelper;
import scala.Option;
import scala.collection.Seq;
import scala.reflect.ClassTag;

public class GenericJavaRowReaderFactory {
    public final static RowReaderFactory<CassandraRow> instance = new RowReaderFactory<CassandraRow>() {
        @Override
        public ClassTag<CassandraRow> classTag() {
            return JavaApiHelper.getClassTag(CassandraRow.class);
        }

        @Override
        public RowReader<CassandraRow> rowReader(TableDef table, RowReaderOptions options) {
            return JavaRowReader.instance;
        }

        @Override
        public RowReaderOptions rowReader$default$2() {
            return new RowReaderOptions(RowReaderOptions.apply$default$1(), RowReaderOptions.apply$default$2());
        }


    };


    public static class JavaRowReader implements RowReader<CassandraRow> {
        public final static JavaRowReader instance = new JavaRowReader();

        private JavaRowReader() {
        }

        @Override
        public CassandraRow read(Row row, String[] columnNames, ProtocolVersion protocolVersion) {
            assert row.getColumnDefinitions().size() == columnNames.length :
                    "Number of columns in a row must match the number of columns in the table metadata";
            return CassandraRow$.MODULE$.fromJavaDriverRow(row, columnNames, protocolVersion);
        }

        @Override
        public Option<Seq<String>> columnNames() {
            return Option.empty();
        }

        @Override
        public Option<Object> requiredColumns() {
            return Option.empty();
        }

        @Override
        public Option<Object> consumedColumns() {
            return Option.empty();
        }
    }

}
