package com.datastax.spark.connector;

import java.io.Serializable;

/**
 * This is a sample JavaBean style class. In order to test JavaAPI correctly, we cannot implement this in Scala because
 * Scala adds some additional accessors and mutators.
 */
public class SampleJavaBean implements Serializable {
    private Integer key;
    private String value;

    public static SampleJavaBean newInstance(Integer key, String value) {
        SampleJavaBean bean = new SampleJavaBean();
        bean.setKey(key);
        bean.setValue(value);
        return bean;
    }

    public Integer getKey() {
        return key;
    }

    public void setKey(Integer key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
