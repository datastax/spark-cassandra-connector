package com.datastax.spark.connector;

import java.io.Serializable;

/**
 * This is a sample JavaBean style class. In order to test JavaAPI correctly, we cannot implement this in Scala because
 * Scala adds some additional accessors and mutators.
 */
public class SampleWeirdJavaBean implements Serializable {
    private Integer devil;
    private String cat;

    public static SampleWeirdJavaBean newInstance(Integer key, String value) {
        SampleWeirdJavaBean bean = new SampleWeirdJavaBean();
        bean.setDevil(key);
        bean.setCat(value);
        return bean;
    }

    public Integer getDevil() {
        return devil;
    }

    public void setDevil(Integer devil) {
        this.devil = devil;
    }

    public String getCat() {
        return cat;
    }

    public void setCat(String cat) {
        this.cat = cat;
    }
}
