package com.datastax.spark.connector.mapper;

import com.datastax.driver.mapping.annotations.Field;

import java.io.Serializable;

/**
 * This Java POJO represents an UDT in Cassandra
 *
 * Created by soumya on 9/15/16.
 */
public class JavaTestUDTBean implements Serializable {
    public Integer field;
    @Field(name = "cassandra_another_field")
    public Integer anotherField;
    @Field(name = "cassandra_yet_another_field")
    public Integer completelyUnrelatedField;

    public Integer getField() {
        return field;
    }

    public void setField(Integer field) {
        this.field = field;
    }

    public Integer getAnotherField() {
        return anotherField;
    }

    public void setAnotherField(Integer anotherField) {
        this.anotherField = anotherField;
    }

    public Integer getCompletelyUnrelatedField() {
        return completelyUnrelatedField;
    }

    public void setCompletelyUnrelatedField(Integer completelyUnrelatedField) {
        this.completelyUnrelatedField = completelyUnrelatedField;
    }
}
