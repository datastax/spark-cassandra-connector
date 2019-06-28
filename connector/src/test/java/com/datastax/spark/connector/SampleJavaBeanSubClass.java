package com.datastax.spark.connector;

/**
 * This is a sample JavaBean style class/subclass. In order to test JavaAPI correctly, we cannot
 * implement this in Scala because Scala adds some additional accessors and mutators.
 */
public class SampleJavaBeanSubClass extends SampleJavaBean
{
    private String subClassField;

    public static SampleJavaBeanSubClass newInstance(Integer key, String value, String subClassField) {
        SampleJavaBeanSubClass bean = new SampleJavaBeanSubClass();
        bean.setKey(key);
        bean.setValue(value);
        bean.setSubClassField(subClassField);
        return bean;
    }

    public String getSubClassField()
    {
        return subClassField;
    }

    public void setSubClassField(String subClassField)
    {
        this.subClassField = subClassField;
    }
}
