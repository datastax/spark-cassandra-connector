package com.datastax.spark.connector.japi;

import akka.japi.JAPI;
import com.datastax.spark.connector.*;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.mapper.ColumnMapper;
import com.datastax.spark.connector.rdd.reader.ClassBasedRowReaderFactory;
import com.datastax.spark.connector.rdd.reader.RowReaderFactory;
import com.datastax.spark.connector.rdd.reader.ValueRowReaderFactory;
import com.datastax.spark.connector.types.TypeConverter;
import com.datastax.spark.connector.types.TypeConverter$;
import com.datastax.spark.connector.util.JavaApiHelper;
import com.datastax.spark.connector.writer.RowWriterFactory;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.DStream;
import scala.Option;
import scala.reflect.api.TypeTags;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.datastax.spark.connector.util.JavaApiHelper.*;

@SuppressWarnings("UnusedDeclaration")
public class CassandraJavaUtil {
    public final static Map<String, String> NO_OVERRIDE = new java.util.HashMap<>();
    public static final ColumnSelector allColumns = AllColumns$.MODULE$;
    public static final BatchSize automaticBatchSize = BatchSize$.MODULE$.Automatic();

    private CassandraJavaUtil() {
        assert false;
    }

    /**
     * A static factory method to create a {@link SparkContextJavaFunctions}
     * based on an existing {@link SparkContext}.
     */
    public static SparkContextJavaFunctions javaFunctions(SparkContext sparkContext) {
        return new SparkContextJavaFunctions(sparkContext);
    }

    /**
     * A static factory method to create a {@link SparkContextJavaFunctions}
     * based on an existing {@link JavaSparkContext}.
     */
    public static SparkContextJavaFunctions javaFunctions(JavaSparkContext sparkContext) {
        return new SparkContextJavaFunctions(JavaSparkContext.toSparkContext(sparkContext));
    }

    /**
     * A static factory method to create a {@link StreamingContextJavaFunctions}
     * based on an existing {@link StreamingContext}.
     */
    public static StreamingContextJavaFunctions javaFunctions(StreamingContext streamingContext) {
        return new StreamingContextJavaFunctions(streamingContext);
    }

    /**
     * A static factory method to create a {@link StreamingContextJavaFunctions}
     * based on an existing {@link JavaStreamingContext}.
     */
    public static StreamingContextJavaFunctions javaFunctions(JavaStreamingContext streamingContext) {
        return new StreamingContextJavaFunctions(streamingContext.ssc());
    }

    /**
     * A static factory method to create a {@link RDDJavaFunctions} based on
     * an existing {@link RDD}.
     *
     * @param targetClass a class of elements in the provided <code>RDD</code>
     */
    public static <T> RDDJavaFunctions<T> javaFunctions(RDD<T> rdd) {
        return new RDDJavaFunctions<>(rdd);
    }

    /**
     * A static factory method to create a {@link RDDJavaFunctions} based on
     * an existing {@link JavaRDD}.
     */
    public static <T> RDDJavaFunctions<T> javaFunctions(JavaRDD<T> rdd) {
        return new RDDJavaFunctions<>(rdd.rdd());
    }

    // TypeTag factory methods

    /**
     * A static factory method to create a {@link DStreamJavaFunctions} based
     * on an existing {@link DStream}.
     *
     * @param targetClass a class of elements in the provided <code>DStream</code>
     */
    public static <T> DStreamJavaFunctions<T> javaFunctions(DStream<T> dStream) {
        return new DStreamJavaFunctions<>(dStream);
    }

    /**
     * A static factory method to create a {@link DStreamJavaFunctions} based
     * on an existing {@link JavaDStream}.
     */
    public static <T> DStreamJavaFunctions<T> javaFunctions(JavaDStream<T> dStream) {
        return new DStreamJavaFunctions<>(dStream.dstream());
    }

    public static <T> TypeTags.TypeTag<T> typeTag(Class<T> main) {
        return JavaApiHelper.getTypeTag(main);
    }

    // TypeConverter factory methods

    public static <T> TypeTags.TypeTag<T> typeTag(Class<T> main, TypeTags.TypeTag<?> typeParam1, TypeTags.TypeTag<?>... typeParams) {
        return JavaApiHelper.getTypeTag(main, JAPI.seq(ArrayUtils.add(typeParams, 0, typeParam1)));
    }

    public static <T> TypeTags.TypeTag<T> typeTag(Class<T> main, Class<?> typeParam1, Class<?>... typeParams) {
        TypeTags.TypeTag<?>[] typeParamsTags = new TypeTags.TypeTag[typeParams.length + 1];
        typeParamsTags[0] = typeTag(typeParam1);
        for (int i = 0; i < typeParams.length; i++) {
            typeParamsTags[i + 1] = typeTag(typeParams[i]);
        }
        return JavaApiHelper.getTypeTag(main, JAPI.seq(typeParamsTags));
    }

    @SuppressWarnings("unchecked")
    public static <T> TypeConverter<T> typeConverter(TypeTags.TypeTag typeTag) {
        return TypeConverter$.MODULE$.forType(typeTag);
    }

    @SuppressWarnings("unchecked")
    public static <T> TypeConverter<T> typeConverter(Class main) {
        return TypeConverter$.MODULE$.forType(typeTag(main));
    }

    // RowReaderFactory factory methods

    @SuppressWarnings("unchecked")
    public static <T> TypeConverter<T> typeConverter(Class main, Class typeParam1, Class... typeParams) {
        return TypeConverter$.MODULE$.forType(typeTag(main, typeParam1, typeParams));
    }

    @SuppressWarnings("unchecked")
    public static <T> TypeConverter<T> typeConverter(Class main, TypeTags.TypeTag typeParam1, TypeTags.TypeTag... typeParams) {
        return TypeConverter$.MODULE$.forType(typeTag(main, typeParam1, typeParams));
    }

    public static <T> RowReaderFactory<T> mapColumnTo(Class<T> targetClass) {
        return new ValueRowReaderFactory<>(CassandraJavaUtil.<T>typeConverter(targetClass));
    }

    public static <T> RowReaderFactory<List<T>> mapColumnToListOf(Class<T> targetClass) {
        return new ValueRowReaderFactory<>(
                CassandraJavaUtil.<List<T>>typeConverter(List.class, targetClass));
    }

    public static <T> RowReaderFactory<Set<T>> mapColumnToSetOf(Class<T> targetClass) {
        return new ValueRowReaderFactory<>(
                CassandraJavaUtil.<Set<T>>typeConverter(Set.class, targetClass));
    }

    public static <K, V> RowReaderFactory<Map<K, V>> mapColumnToMapOf(Class<K> targetKeyClass, Class<V> targetValueClass) {
        return new ValueRowReaderFactory<>(
                CassandraJavaUtil.<Map<K, V>>typeConverter(Map.class, targetKeyClass, targetValueClass));
    }

    public static <T> RowReaderFactory<T> mapColumnTo(Class targetClass, Class typeParam1, Class... typeParams) {
        return new ValueRowReaderFactory<>(
                CassandraJavaUtil.<T>typeConverter(targetClass, typeParam1, typeParams));
    }

    public static <T> RowReaderFactory<T> mapColumnTo(TypeConverter<T> typeConverter) {
        return new ValueRowReaderFactory<>(typeConverter);
    }

    public static <T> RowReaderFactory<T> mapRowTo(Class<T> targetClass) {
        TypeTags.TypeTag<T> tt = typeTag(targetClass);
        ColumnMapper<T> mapper = javaBeanColumnMapper(getClassTag(targetClass), new HashMap<String, String>());
        return new ClassBasedRowReaderFactory<>(tt, mapper);
    }

    public static <T> RowReaderFactory<T> mapRowTo(Class<T> targetClass, Map<String, String> columnMapping) {
        TypeTags.TypeTag<T> tt = typeTag(targetClass);
        ColumnMapper<T> mapper = javaBeanColumnMapper(getClassTag(targetClass), columnMapping);
        return new ClassBasedRowReaderFactory<>(tt, mapper);
    }

    @SafeVarargs
    public static <T> RowReaderFactory<T> mapRowTo(Class<T> targetClass, Pair<String, String>... columnMappings) {
        TypeTags.TypeTag<T> tt = typeTag(targetClass);
        ColumnMapper<T> mapper = javaBeanColumnMapper(getClassTag(targetClass), convertToMap(columnMappings));
        return new ClassBasedRowReaderFactory<>(tt, mapper);
    }

    public static <T> RowReaderFactory<T> mapRowTo(ColumnMapper<T> columnMapper) {
        TypeTags.TypeTag<T> tt = typeTag(getRuntimeClass(columnMapper.classTag()));
        return new ClassBasedRowReaderFactory<>(tt, columnMapper);
    }

    public static Map<String, String> convertToMap(Pair<String, String>[] pairs) {
        Map<String, String> map = new HashMap<>();
        for (Pair<String, String> pair : pairs)
            map.put(pair.getKey(), pair.getValue());

        return map;
    }

    public static BatchSize rowsInBatch(int batchSizeInRows) {
        return RowsInBatch$.MODULE$.apply(batchSizeInRows);
    }

    public static BatchSize bytesInBatch(int batchSizeInBytes) {
        return BytesInBatch$.MODULE$.apply(batchSizeInBytes);
    }

    public static ColumnSelector someColumns(String... columnNames) {
        return SomeColumns$.MODULE$.apply(JAPI.seq(columnNames));
    }

    public static Option<CassandraConnector> connector(CassandraConnector connector) {
        return Option.apply(connector);
    }

    public static <T> RowWriterFactory<T> mapToRows(ColumnMapper<T> columnMapper) {
        return defaultRowWriterFactory(columnMapper);
    }

    public static <T> RowWriterFactory<T> mapToRows(Class<T> targetClass, Map<String, String> columnNameMappings) {
        ColumnMapper<T> mapper = javaBeanColumnMapper(getClassTag(targetClass), columnNameMappings);
        return mapToRows(mapper);
    }

    @SafeVarargs
    public static <T> RowWriterFactory<T> mapToRows(Class<T> targetClass, Pair<String, String> ... columnNameMappings) {
        return mapToRows(targetClass, convertToMap(columnNameMappings));
    }
}
