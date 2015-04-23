package com.datastax.spark.connector.japi;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import scala.Option;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;
import scala.reflect.api.TypeTags;

import akka.japi.JAPI;
import static com.datastax.spark.connector.util.JavaApiHelper.*;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;

import com.datastax.spark.connector.*;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.mapper.ColumnMapper;
import com.datastax.spark.connector.mapper.JavaBeanColumnMapper;
import com.datastax.spark.connector.rdd.reader.ClassBasedRowReaderFactory;
import com.datastax.spark.connector.rdd.reader.RowReaderFactory;
import com.datastax.spark.connector.rdd.reader.ValueRowReaderFactory;
import com.datastax.spark.connector.types.TypeConverter;
import com.datastax.spark.connector.types.TypeConverter$;
import com.datastax.spark.connector.util.JavaApiHelper;
import com.datastax.spark.connector.util.JavaApiHelper$;
import com.datastax.spark.connector.writer.BatchGroupingKey;
import com.datastax.spark.connector.writer.RowWriterFactory;

/**
 * The main entry point to Spark Cassandra Connector Java API.
 * <p/>
 * There are several helpful static factory methods which build useful wrappers around Spark Context, and
 * RDD. There are also helper methods to build reader and writer factories which configure their
 * parameters.
 */
@SuppressWarnings("UnusedDeclaration")
public class CassandraJavaUtil {

    private CassandraJavaUtil() {
        assert false;
    }

    // -------------------------------------------------------------------------
    //              Java API wrappers factory methods 
    // -------------------------------------------------------------------------

    /**
     * A static factory method to create a {@link SparkContextJavaFunctions} based on an existing {@link
     * SparkContext} instance.
     */
    public static SparkContextJavaFunctions javaFunctions(SparkContext sparkContext) {
        return new SparkContextJavaFunctions(sparkContext);
    }

    /**
     * A static factory method to create a {@link SparkContextJavaFunctions} based on an existing {@link
     * JavaSparkContext} instance.
     */
    public static SparkContextJavaFunctions javaFunctions(JavaSparkContext sparkContext) {
        return new SparkContextJavaFunctions(JavaSparkContext.toSparkContext(sparkContext));
    }

    /**
     * A static factory method to create a {@link RDDJavaFunctions} based on an existing {@link RDD}
     * instance.
     */
    public static <T> RDDJavaFunctions<T> javaFunctions(RDD<T> rdd) {
        return new RDDJavaFunctions<>(rdd);
    }

    /**
     * A static factory method to create a {@link RDDJavaFunctions} based on an existing {@link JavaRDD}
     * instance.
     */
    public static <T> RDDJavaFunctions<T> javaFunctions(JavaRDD<T> rdd) {
        return new RDDJavaFunctions<>(rdd.rdd());
    }

    /**
     * A static factory method to create a {@link PairRDDJavaFunctions} based on an existing {@link
     * JavaPairRDD} instance.
     */
    public static <K, V> PairRDDJavaFunctions<K, V> javaFunctions(JavaPairRDD<K, V> rdd) {
        return new PairRDDJavaFunctions<>(rdd.rdd());
    }

    // -------------------------------------------------------------------------
    //              Type tag resolvers 
    // -------------------------------------------------------------------------

    public static <T> ClassTag<T> safeClassTag(Class<T> cls) {
        return JavaApiHelper$.MODULE$.getClassTag(cls);
    }

    public static <T> ClassTag<T> classTag(Class<?> cls) {
        return JavaApiHelper$.MODULE$.getClassTag2(cls);
    }

    @SuppressWarnings("unchecked")
    public static <T> ClassTag<T> anyClassTag() {
        return (ClassTag<T>) ClassTag$.MODULE$.AnyRef();
    }

    /**
     * Creates a type tag representing a given class type without any type parameters.
     */
    public static <T> TypeTags.TypeTag<T> safeTypeTag(Class<T> main) {
        return JavaApiHelper.getTypeTag(main);
    }

    /**
     * Creates a type tag representing a given class type without any type parameters.
     */
    @SuppressWarnings("unchecked")
    public static <T> TypeTags.TypeTag<T> typeTag(Class<?> main) {
        return JavaApiHelper.getTypeTag((Class<T>) main);
    }

    /**
     * Creates a type tag representing a given class type with given type parameters.
     * <p/>
     * This is the most generic method to create type tags. One can use it to create type tags of
     * arbitrary class types with any number of type parameters and any depth of type parameter nesting.
     * <p/>
     * For example, the following code: {@code typeTag(Map.class, typeTag(String.class),
     * typeTag(List.class, typeTag(String.class))} returns the type tag of {@code Map<String,
     * List<String>>}
     */
    public static <T> TypeTags.TypeTag<T> typeTag(
            Class<?> main, TypeTags.TypeTag<?> typeParam1, TypeTags.TypeTag<?>... typeParams) {
        return JavaApiHelper.getTypeTag(main, JAPI.seq(ArrayUtils.add(typeParams, 0, typeParam1)));
    }

    /**
     * Creates a type tag representing a given class type with given type parameters.
     * <p/>
     * With this factory method you can create type tags of class types with any number of type
     * parameters. However, the type parameters are class types without their own type parameters.
     * <p/>
     * For example, the following code: {@code typeTag(List.class, String.class)} returns a type tag of
     * {@code List<String>}, and the following code: {@code typeTag(Map.class, String.class,
     *Integer.class)} returns a type tag of {@code Map<String, Integer>}.
     * <p/>
     * This is a short-hand method for calling {@link #typeTag(Class, TypeTags.TypeTag,
     * TypeTags.TypeTag[])} and providing type parameters with {@link #safeTypeTag(Class)}.
     */
    public static <T> TypeTags.TypeTag<T> typeTag(Class<?> main, Class<?> typeParam1, Class<?>... typeParams) {
        TypeTags.TypeTag<?>[] typeParamsTags = new TypeTags.TypeTag[typeParams.length + 1];
        typeParamsTags[0] = typeTag(typeParam1);
        for (int i = 0; i < typeParams.length; i++) {
            typeParamsTags[i + 1] = typeTag(typeParams[i]);
        }
        return JavaApiHelper.getTypeTag(main, JAPI.seq(typeParamsTags));
    }

    // -------------------------------------------------------------------------
    //              Type converter resolvers
    // -------------------------------------------------------------------------

    /**
     * Retrieves a type converter for the given class type.
     * <p/>
     * This is a short-hand method for calling {@link #typeConverter(TypeTags.TypeTag)} with an argument
     * obtained by {@link #safeTypeTag(Class)}.
     */
    public static <T> TypeConverter<T> safeTypeConverter(Class<T> main) {
        return TypeConverter$.MODULE$.forType(safeTypeTag(main));
    }

    /**
     * Retrieves a type converter for a type represented by a given type tag.
     */
    @SuppressWarnings("unchecked")
    public static <T> TypeConverter<T> typeConverter(TypeTags.TypeTag<?> typeTag) {
        return (TypeConverter<T>) TypeConverter$.MODULE$.forType(typeTag);
    }

    /**
     * Retrieves a type converter for the given class type.
     * <p/>
     * This is a short-hand method for calling {@link #typeConverter(TypeTags.TypeTag)} with an argument
     * obtained by {@link #safeTypeTag(Class)}.
     */
    @SuppressWarnings("unchecked")
    public static <T> TypeConverter<T> typeConverter(Class<?> main) {
        return TypeConverter$.MODULE$.forType(CassandraJavaUtil.<T>typeTag(main));
    }

    /**
     * Retrieves a type converter for the given class type with the given type parameters.
     * <p/>
     * This is a short-hand method for calling {@link #typeConverter(TypeTags.TypeTag)} with an argument
     * obtained by {@link #typeTag(Class, Class, Class[])}.
     */
    @SuppressWarnings("unchecked")
    public static <T> TypeConverter<T> typeConverter(Class<?> main, Class<?> typeParam1, Class<?>... typeParams) {
        return TypeConverter$.MODULE$.forType(CassandraJavaUtil.<T>typeTag(main, typeParam1, typeParams));
    }

    /**
     * Retrieves a type converter for the given class type with the given type parameters.
     * <p/>
     * This is a short-hand method for calling {@link #typeConverter(TypeTags.TypeTag)} with an argument
     * obtained by {@link #typeTag(Class, TypeTags.TypeTag, TypeTags.TypeTag[])}.
     */
    @SuppressWarnings("unchecked")
    public static <T> TypeConverter<T> typeConverter(
            Class<?> main, TypeTags.TypeTag<?> typeParam1, TypeTags.TypeTag<?>... typeParams) {
        return TypeConverter$.MODULE$.forType(CassandraJavaUtil.<T>typeTag(main, typeParam1, typeParams));
    }

    // -------------------------------------------------------------------------
    //              Single column mappers 
    // -------------------------------------------------------------------------

    /**
     * Constructs a row reader factory which maps a single column from a row to the instance of a given
     * class.
     * <p/>
     * It is a short-hand method for calling {@link #mapColumnTo(TypeConverter)} with the type converter
     * resolved by {@link #typeConverter(Class)} method.
     */
    public static <T> RowReaderFactory<T> mapColumnTo(Class<T> targetClass) {
        return new ValueRowReaderFactory<>(CassandraJavaUtil.<T>typeConverter(targetClass));
    }

    /**
     * Constructs a row reader factory which maps a single column from a row to the list of objects of a
     * specified type.
     * <p/>
     * The method should be used when we have to deal with a column of some collection type (either a set
     * or a list).
     * <p/>
     * For example the following code: {@code javaFunctions(sc).cassandraTable("ks", "tab",
     *mapColumnToListOf(String.class))} returns an RDD of {@code List<String>}.
     * <p/>
     * It is a short-hand method for calling {@link #mapColumnTo(Class, Class, Class[])} with the main
     * type being {@link java.util.List} and the type parameter being the given target class.
     */
    public static <T> RowReaderFactory<List<T>> mapColumnToListOf(Class<T> targetClass) {
        return new ValueRowReaderFactory<>(
                CassandraJavaUtil.<List<T>>typeConverter(List.class, targetClass));
    }

    /**
     * Constructs a row reader factory which maps a single column from a row to the set of objects of a
     * specified type.
     * <p/>
     * The method should be used when we have to deal with a column of some collection type (either a set
     * or a list).
     * <p/>
     * For example the following code: {@code javaFunctions(sc).cassandraTable("ks", "tab",
     *mapColumnToSetOf(String.class))} returns an RDD of {@code Set<String>}.
     * <p/>
     * It is a short-hand method for calling {@link #mapColumnTo(Class, Class, Class[])} with the main
     * type being {@link java.util.Set} and the type parameter being the given target class.
     */
    public static <T> RowReaderFactory<Set<T>> mapColumnToSetOf(Class<T> targetClass) {
        return new ValueRowReaderFactory<>(
                CassandraJavaUtil.<Set<T>>typeConverter(Set.class, targetClass));
    }

    /**
     * Constructs a row reader factory which maps a single column from a row to the map of pairs of
     * specified key and value types.
     * <p/>
     * The method should be used when we have to deal with a column of a map type.
     * <p/>
     * For example the following code: {@code javaFunctions(sc).cassandraTable("ks", "tab",
     *mapColumnToMapOf(String.class, Integer.class))} returns an RDD of {@code Map<String, Integer>}.
     * <p/>
     * It is a short-hand method for calling {@link #mapColumnTo(Class, Class, Class[])} with the main
     * type being {@link java.util.Map} and the type parameters being the given target key and target
     * value classes.
     */
    public static <K, V> RowReaderFactory<Map<K, V>> mapColumnToMapOf(
            Class<K> targetKeyClass, Class<V> targetValueClass) {
        return new ValueRowReaderFactory<>(
                CassandraJavaUtil.<Map<K, V>>typeConverter(Map.class, targetKeyClass, targetValueClass));
    }

    /**
     * Constructs a row reader factory which maps a single column from a row to the objects of a
     * specified type with given type parameters.
     * <p/>
     * It is a short-hand method for calling {@link #mapColumnTo(TypeConverter)} with a type converter
     * resolved by calling {@link #typeConverter(Class, Class, Class[])}.
     */
    @SuppressWarnings("unchecked")
    public static <T> RowReaderFactory<T> mapColumnTo(
            Class targetClass, Class typeParam1, Class... typeParams) {
        return new ValueRowReaderFactory<>(
                CassandraJavaUtil.<T>typeConverter(targetClass, typeParam1, typeParams));
    }

    /**
     * Constructs a row reader factory which maps a single column from a row using a specified type
     * converter.
     * <p/>
     * Unlike {@code mapRowTo(...)} methods, this method converts the first available column from the
     * result set. Therefore, it is strongly recommended to explicitly specify the projection on the RDD
     * in order to decrease the amount of data to be fetched and to make it explicit which column is to
     * be mapped.
     */
    public static <T> RowReaderFactory<T> mapColumnTo(TypeConverter<T> typeConverter) {
        return new ValueRowReaderFactory<>(typeConverter);
    }

    // -------------------------------------------------------------------------
    //              Entire row mappers 
    // -------------------------------------------------------------------------

    /**
     * Constructs a row reader factory which maps an entire row to an object of a specified type.
     * <p/>
     * The given target class is considered to follow the Java bean style convention. It should have a
     * public, no-args constructor, as well as a set of getters and setters. The names of the attributes
     * must follow the names of columns in the table (however, different conventions are allowed - see
     * the guide for more info). The default mapping of attributes to column names can be overridden by
     * providing a custom map of attribute-column mappings for the pairs which do not follow the general
     * convention.
     * <p/>
     * Another version of this method {@link #mapRowTo(Class, Pair[])} can be considered much more handy
     * for inline invocations.
     * <p/>
     * The method uses {@link JavaBeanColumnMapper} as the column mapper. If another column mapper has to
     * be used, see {@link #mapRowTo(Class, ColumnMapper)} method.
     */
    public static <T> RowReaderFactory<T> mapRowTo(
            Class<T> targetClass, Map<String, String> columnMapping) {
        TypeTags.TypeTag<T> tt = typeTag(targetClass);
        ColumnMapper<T> mapper = javaBeanColumnMapper(safeClassTag(targetClass), columnMapping);
        return new ClassBasedRowReaderFactory<>(tt, mapper);
    }

    /**
     * Constructs a row reader factory which maps an entire row to an object of a specified type.
     * <p/>
     * The given target class is considered to follow the Java bean style convention. It should have a
     * public, no-args constructor, as well as a set of getters and setters. The names of the attributes
     * must follow the names of columns in the table (however, different conventions are allowed - see
     * the guide for more info). The default mapping of attributes to column names can be changed by
     * providing a custom map of attribute-column mappings for the pairs which do not follow the general
     * convention.
     * <p/>
     * The method uses {@link JavaBeanColumnMapper} as the column mapper. If another column mapper has to
     * be used, see {@link #mapRowTo(Class, ColumnMapper)} method.
     */
    public static <T> RowReaderFactory<T> mapRowTo(Class<T> targetClass, Pair... columnMappings) {
        TypeTags.TypeTag<T> tt = typeTag(targetClass);
        ColumnMapper<T> mapper = javaBeanColumnMapper(
                safeClassTag(targetClass), convertToMap(columnMappings));
        return new ClassBasedRowReaderFactory<>(tt, mapper);
    }

    /**
     * Constructs a row reader factory which maps an entire row using a specified column mapper. Follow
     * the guide to find out more about column mappers.
     */
    public static <T> RowReaderFactory<T> mapRowTo(Class<T> targetClass, ColumnMapper<T> columnMapper) {
        TypeTags.TypeTag<T> tt = typeTag(targetClass);
        return new ClassBasedRowReaderFactory<>(tt, columnMapper);
    }

    // -------------------------------------------------------------------------
    //              Row writer factory creation methods 
    // -------------------------------------------------------------------------

    /**
     * Creates a row writer factory which can map objects supported by a given column mapper to columns
     * in a table.
     */
    public static <T> RowWriterFactory<T> mapToRow(Class<T> targetClass, ColumnMapper<T> columnMapper) {
        return defaultRowWriterFactory(columnMapper, safeClassTag(targetClass));
    }

    /**
     * Creates a row writer factory which maps objects of a given class type to columns in table.
     * <p/>
     * The given source class is considered to follow the Java bean style convention. It should have a
     * public, no-args constructor, as well as a set of getters and setters. The names of the attributes
     * must follow the names of columns in the table (however, different conventions are allowed - see
     * the guide for more info). The default mapping of attributes to column names can be changed by
     * providing a custom map of attribute-column mappings for the pairs which do not follow the general
     * convention.
     * <p/>
     * Another version of this method {@link #mapToRow(Class, Pair[])} can be considered much more handy
     * for inline invocations.
     * <p/>
     * The method uses {@link JavaBeanColumnMapper} as the column mapper. If another column mapper has to
     * be used, see {@link #mapToRow(Class, ColumnMapper)} method.
     */
    public static <T> RowWriterFactory<T> mapToRow(
            Class<T> sourceClass, Map<String, String> columnNameMappings) {
        ColumnMapper<T> mapper = javaBeanColumnMapper(safeClassTag(sourceClass), columnNameMappings);
        return mapToRow(sourceClass, mapper);
    }

    /**
     * Creates a row writer factory which maps objects of a given class type to columns in table.
     * <p/>
     * The given source class is considered to follow the Java bean style convention. It should have a
     * public, no-args constructor, as well as a set of getters and setters. The names of the attributes
     * must follow the names of columns in the table (however, different conventions are allowed - see
     * the guide for more info). The default mapping of attributes to column names can be changed by
     * providing a custom map of attribute-column mappings for the pairs which do not follow the general
     * convention.
     * <p/>
     * The method uses {@link JavaBeanColumnMapper} as the column mapper. If another column mapper has to
     * be used, see {@link #mapToRow(Class, ColumnMapper)} method.
     */
    public static <T> RowWriterFactory<T> mapToRow(Class<T> sourceClass, Pair... columnNameMappings) {
        return mapToRow(sourceClass, convertToMap(columnNameMappings));
    }

    // -------------------------------------------------------------------------
    //              Batch size factory methods and constants 
    // -------------------------------------------------------------------------

    /**
     * The default automatic batch size.
     */
    public static final BatchSize automaticBatchSize = BatchSize$.MODULE$.Automatic();

    /**
     * Creates a batch size which bases on the given number of rows - the total batch size in bytes
     * doesn't matter in this case.
     */
    public static BatchSize rowsInBatch(int batchSizeInRows) {
        return RowsInBatch$.MODULE$.apply(batchSizeInRows);
    }

    /**
     * Creates a batch size which bases on total size in bytes - the number of rows doesn't matter in
     * this case.
     */
    public static BatchSize bytesInBatch(int batchSizeInBytes) {
        return BytesInBatch$.MODULE$.apply(batchSizeInBytes);
    }

    public static final BatchGroupingKey BATCH_GROUPING_KEY_NONE = BatchGroupingKey.None$.MODULE$;
    public static final BatchGroupingKey BATCH_GROUPING_KEY_PARTITION = BatchGroupingKey.Partition$.MODULE$;
    public static final BatchGroupingKey BATCH_GROUPING_KEY_REPLICA_SET = BatchGroupingKey.ReplicaSet$.MODULE$;

    // -------------------------------------------------------------------------
    //              Column selector factory methods and constants 
    // -------------------------------------------------------------------------

    /**
     * Column selector to select all the columns from a table.
     */
    public static final ColumnSelector allColumns = AllColumns$.MODULE$;

    /**
     * Creates a column selector with a given columns projection.
     */
    public static ColumnSelector someColumns(String... columnNames) {
        NamedColumnRef[] columnsSelection = new NamedColumnRef[columnNames.length];
        for (int i = 0; i < columnNames.length; i++) {
            columnsSelection[i] = ColumnName$.MODULE$.apply(columnNames[i], Option.<String>empty());
        }

        return SomeColumns$.MODULE$.apply(JAPI.<SelectableColumnRef>seq(columnsSelection));
    }

    public static ColumnName column(String columnName) {
        return new ColumnName(columnName, Option.<String>empty());
    }

    public static TTL ttl(String columnName) {
        return new TTL(columnName, Option.<String>empty());
    }

    public static WriteTime writeTime(String columnName) {
        return new WriteTime(columnName, Option.<String>empty());
    }

    public static SelectableColumnRef[] toSelectableColumnRefs(String... columnNames) {
        ColumnName[] refs = new ColumnName[columnNames.length];
        for (int i = 0; i < columnNames.length; i++) {
            refs[i] = column(columnNames[i]);
        }

        return refs;
    }

    // -------------------------------------------------------------------------
    //              Utility methods 
    // -------------------------------------------------------------------------

    /**
     * A simple method that converts an array of pairs into a map.
     */
    public static Map<String, String> convertToMap(Pair[] pairs) {
        Map<String, String> map = new HashMap<>();
        for (Pair pair : pairs)
            map.put(String.valueOf(pair.getKey()), String.valueOf(pair.getValue()));

        return map;
    }

    /**
     * A simple method which wraps the given connector instance into Option.
     */
    public static Option<CassandraConnector> connector(CassandraConnector connector) {
        return Option.apply(connector);
    }


}
