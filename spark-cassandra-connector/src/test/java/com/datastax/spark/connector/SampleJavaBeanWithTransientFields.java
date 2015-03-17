package com.datastax.spark.connector;

import java.io.Serializable;

/**
 * This is a sample JavaBean style class. In order to test JavaAPI correctly, we cannot implement this in Scala because
 * Scala adds some additional accessors and mutators.
 */
public class SampleJavaBeanWithTransientFields implements Serializable {
    private Integer key;
    private String value;

    transient private String transientField;

    public static SampleJavaBeanWithTransientFields newInstance(Integer key, String value) {
        SampleJavaBeanWithTransientFields bean = new SampleJavaBeanWithTransientFields();
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

    public String getTransientField()
    {
        return transientField;
    }

    public void setTransientField(String transientField)
    {
        this.transientField = transientField;
    }
}
