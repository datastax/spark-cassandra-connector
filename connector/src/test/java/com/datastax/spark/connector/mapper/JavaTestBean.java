package com.datastax.spark.connector.mapper;

import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;

import java.io.Serializable;

/**
 * This is a Java Bean style class with Java Driver style annotations built in.
 * This class also contains nested UDTs with its own mappings
 */
@Entity
public class JavaTestBean implements Serializable {

    @CqlName(value = "cassandra_property_1")
    public Integer property1;
    @CqlName(value = "cassandra_camel_case_property")
    public Integer camelCaseProperty;
    public JavaTestUDTBean nested;

    public int getProperty1() {
        return property1;
    }

    public void setProperty1(int property1) {
        this.property1 = property1;
    }

    public int getCamelCaseProperty() {
        return camelCaseProperty;
    }

    public void setCamelCaseProperty(int camelCaseProperty) {
        this.camelCaseProperty = camelCaseProperty;
    }

    public JavaTestUDTBean getNested() {
        return nested;
    }

    public void setNested(JavaTestUDTBean nested) {
        this.nested = nested;
    }

}
