package com.datastax.spark.connector.japi;

import akka.japi.JavaPartialFunction;
import com.datastax.spark.connector.japi.types.JavaTypeConverter;
import com.datastax.spark.connector.util.JavaApiHelper$;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CustomTypeConverterTest {

    public static enum SampleEnum {
        ONE, TWO, THREE
    }

    public final static JavaTypeConverter<SampleEnum> sampleEnumConverter =
            new JavaTypeConverter<SampleEnum>(JavaApiHelper$.MODULE$.getTypeTag(SampleEnum.class),
                    new JavaPartialFunction<Object, SampleEnum>() {
                        @Override
                        public SampleEnum apply(Object x, boolean isCheck) throws Exception {
                            if (x == null) {
                                return null;
                            } else if (x instanceof String) {
                                try {
                                    return SampleEnum.valueOf((String) x);
                                } catch (IllegalArgumentException ex) {
                                    throw noMatch();
                                }
                            } else if (x instanceof Number) {
                                switch (((Number) x).intValue()) {
                                    case 1:
                                        return SampleEnum.ONE;
                                    case 2:
                                        return SampleEnum.TWO;
                                    case 3:
                                        return SampleEnum.THREE;
                                }
                            }
                            throw noMatch();
                        }
                    });


    @Test
    public void test1() {
        assertEquals(SampleEnum.class.getName(), sampleEnumConverter.targetTypeName());

        assertEquals(true, sampleEnumConverter.convertPF().isDefinedAt(1));
        assertEquals(true, sampleEnumConverter.convertPF().isDefinedAt(2.5));
        assertEquals(true, sampleEnumConverter.convertPF().isDefinedAt("THREE"));
        assertEquals(false, sampleEnumConverter.convertPF().isDefinedAt("asdf"));
        assertEquals(false, sampleEnumConverter.convertPF().isDefinedAt(4));
        assertEquals(true, sampleEnumConverter.convertPF().isDefinedAt(null));

        assertEquals(SampleEnum.ONE, sampleEnumConverter.convertPF().apply(1));
        assertEquals(SampleEnum.TWO, sampleEnumConverter.convertPF().apply(2.5));
        assertEquals(SampleEnum.THREE, sampleEnumConverter.convertPF().apply("THREE"));
        assertEquals(null, sampleEnumConverter.convertPF().apply(null));
    }
}
