package com.datastax.spark.connector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import scala.reflect.api.TypeTags;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.Test;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.spark.connector.japi.RDDJavaFunctions;
import com.datastax.spark.connector.japi.SparkContextJavaFunctions;
import com.datastax.spark.connector.mapper.ColumnMapper;
import com.datastax.spark.connector.rdd.reader.ClassBasedRowReaderFactory;
import com.datastax.spark.connector.rdd.reader.RowReaderFactory;
import com.datastax.spark.connector.rdd.reader.ValueRowReaderFactory;
import com.datastax.spark.connector.types.TypeConverter;

@SuppressWarnings("unchecked")
public class CassandraJavaUtilTest {

    @Test
    public void testTypeTag1() throws Exception {
        TypeTags.TypeTag<String> tt = typeTag(String.class);
        assertThat(tt.tpe().toString(), is(String.class.getName()));
    }

    @Test
    public void testTypeTag2() throws Exception {
        TypeTags.TypeTag<List> tt1 = typeTag(List.class, String.class);
        assertThat(tt1.tpe().toString(), is(String.format("%s[%s]",
                List.class.getName(), String.class.getName())));

        TypeTags.TypeTag<Map> tt2 = typeTag(Map.class, String.class, Integer.class);
        assertThat(tt2.tpe().toString(), is(String.format("%s[%s,%s]",
                Map.class.getName(), String.class.getName(), Integer.class.getName())));
    }

    @Test
    public void testTypeTag3() throws Exception {
        TypeTags.TypeTag<List> tt = typeTag(List.class, typeTag(Set.class, typeTag(Map.class, typeTag(String.class), typeTag(Integer.class))));
        assertThat(tt.tpe().toString(), is(String.format("%s[%s[%s[%s,%s]]]",
                List.class.getName(), Set.class.getName(), Map.class.getName(), String.class.getName(), Integer.class.getName())));
    }

    @Test
    public void testTypeConverter1() throws Exception {
        TypeConverter<List<String>> tc = typeConverter(String.class);
        assertThat(tc.targetTypeName(), is(String.class.getSimpleName()));
    }

    @Test
    public void testTypeConverter2() throws Exception {
        TypeConverter<List<String>> tc1 = typeConverter(List.class, String.class);
        assertThat(tc1.targetTypeName(), is(String.format("%s[%s]",
                List.class.getName(), String.class.getSimpleName())));

        TypeConverter<Map<String, Integer>> tc2 = typeConverter(Map.class, String.class, Integer.class);
        assertThat(tc2.targetTypeName(), is(String.format("%s[%s,%s]",
                Map.class.getName(), String.class.getSimpleName(), Integer.class.getName())));

    }

    @Test
    public void testTypeConverter3() throws Exception {
        TypeConverter<List> tc = typeConverter(List.class, typeTag(Set.class, typeTag(Map.class, typeTag(String.class), typeTag(Integer.class))));
        assertThat(tc.targetTypeName(), is(String.format("%s[%s[%s[%s,%s]]]",
                List.class.getName(), Set.class.getName(), Map.class.getName(), String.class.getSimpleName(), Integer.class.getName())));
    }

    @Test
    public void testTypeConverter4() throws Exception {
        TypeTags.TypeTag<List> tt = typeTag(List.class, typeTag(Set.class, typeTag(Map.class, typeTag(String.class), typeTag(Integer.class))));
        TypeConverter<List> tc = typeConverter(tt);
        assertThat(tc.targetTypeName(), is(String.format("%s[%s[%s[%s,%s]]]",
                List.class.getName(), Set.class.getName(), Map.class.getName(), String.class.getSimpleName(), Integer.class.getName())));
    }

    @Test
    public void testMapColumnTo1() throws Exception {
        RowReaderFactory<Integer> rrf = mapColumnTo(Integer.class);
        assertThat(rrf, instanceOf(ValueRowReaderFactory.class));
        assertThat(rrf.targetClass().getName(), is(Integer.class.getName()));
    }

    @Test
    public void testMapColumnTo2() throws Exception {
        RowReaderFactory<Integer> rrf = mapColumnTo(TypeConverter.JavaIntConverter$.MODULE$);
        assertThat(rrf, instanceOf(ValueRowReaderFactory.class));
        assertThat(rrf.targetClass().getName(), is(Integer.class.getName()));
    }

    @Test
    public void testMapColumnTo3() throws Exception {
        RowReaderFactory<List<Integer>> rrf = mapColumnTo(List.class, Integer.class);
        assertThat(rrf, instanceOf(ValueRowReaderFactory.class));
        assertThat(rrf.targetClass().getName(), is(List.class.getName()));
    }

    @Test
    public void testMapColumnToListOf() throws Exception {
        RowReaderFactory<List<Integer>> rrf = mapColumnToListOf(Integer.class);
        assertThat(rrf, instanceOf(ValueRowReaderFactory.class));
        assertThat(rrf.targetClass().getName(), is(List.class.getName()));
    }

    @Test
    public void testMapColumnToSetOf() throws Exception {
        RowReaderFactory<Set<Integer>> rrf = mapColumnToSetOf(Integer.class);
        assertThat(rrf, instanceOf(ValueRowReaderFactory.class));
        assertThat(rrf.targetClass().getName(), is(Set.class.getName()));

    }

    @Test
    public void testMapColumnToMapOf() throws Exception {
        RowReaderFactory<Map<Integer, Double>> rrf = mapColumnToMapOf(Integer.class, Double.class);
        assertThat(rrf, instanceOf(ValueRowReaderFactory.class));
        assertThat(rrf.targetClass().getName(), is(Map.class.getName()));
    }


    @Test
    public void testMapRowTo1() throws Exception {
        RowReaderFactory<SampleJavaBean> rrf = mapRowTo(SampleJavaBean.class);
        assertThat(rrf, instanceOf(ClassBasedRowReaderFactory.class));
        assertThat(rrf.targetClass().getName(), is(SampleJavaBean.class.getName()));
    }

    @Test
    public void testMapRowTo2() throws Exception {
        Map<String, String> mappings = new HashMap<>();
        mappings.put("one", "1");
        mappings.put("two", "2");
        RowReaderFactory<SampleJavaBean> rrf = mapRowTo(SampleJavaBean.class, mappings);
        assertThat(rrf, instanceOf(ClassBasedRowReaderFactory.class));
        assertThat(rrf.targetClass().getName(), is(SampleJavaBean.class.getName()));
    }

    @Test
    public void testMapRowTo3() throws Exception {
        RowReaderFactory<SampleJavaBean> rrf = mapRowTo(SampleJavaBean.class, Pair.of("a", "b"), Pair.of("c", "d"));
        assertThat(rrf, instanceOf(ClassBasedRowReaderFactory.class));
        assertThat(rrf.targetClass().getName(), is(SampleJavaBean.class.getName()));
    }

    @Test
    public void testMapRowTo4() throws Exception {
        //noinspection unchecked
        ColumnMapper<SampleJavaBean> mapper = mock(ColumnMapper.class);
        RowReaderFactory<SampleJavaBean> rrf = mapRowTo(SampleJavaBean.class, mapper);
        assertThat(rrf, instanceOf(ClassBasedRowReaderFactory.class));
        assertThat(rrf.targetClass().getName(), is(SampleJavaBean.class.getName()));
    }

    @Test
    public void testConvertToMap() throws Exception {
        Map<String, String> map1 = convertToMap(new Pair[0]);
        assertThat(map1.size(), is(0));

        Map<String, String> map2 = convertToMap(new Pair[]{Pair.of("one", "1"), Pair.of("two", "2")});
        assertThat(map2.size(), is(2));
        assertThat(map2.get("one"), is("1"));
        assertThat(map2.get("two"), is("2"));
    }

    @Test
    public void testJavaFunctions() throws Exception {
        SparkContext sc = mock(SparkContext.class);
        SparkContextJavaFunctions scjf = javaFunctions(sc);
        assertThat(scjf.sparkContext, is(sc));
    }

    @Test
    public void testJavaFunctions1() throws Exception {
        SparkContext sc = mock(SparkContext.class);
        JavaSparkContext jsc = mock(JavaSparkContext.class);
        when(jsc.sc()).thenReturn(sc);
        SparkContextJavaFunctions scjf = javaFunctions(jsc);
        assertThat(scjf.sparkContext, is(jsc.sc()));
    }

    @Test
    public void testJavaFunctions4() throws Exception {
        RDD rdd = mock(RDD.class);
        RDDJavaFunctions rddjf = javaFunctions(rdd);
        assertThat(rddjf.rdd, is(rdd));
    }

    @Test
    public void testJavaFunctions5() throws Exception {
        RDD rdd = mock(RDD.class);
        JavaRDD jrdd = mock(JavaRDD.class);
        when(jrdd.rdd()).thenReturn(rdd);
        RDDJavaFunctions rddjf = javaFunctions(jrdd);
        assertThat(rddjf.rdd, is(rdd));
    }

}