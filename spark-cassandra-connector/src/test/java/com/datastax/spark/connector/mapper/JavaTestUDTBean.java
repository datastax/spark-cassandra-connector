package com.datastax.spark.connector.mapper;

import com.datastax.driver.mapping.annotations.Field;

/**
 * Created by soumya on 9/15/16.
 */
public class JavaTestUDTBean {
    public Integer field;
    @Field(name = "another_field")
    public Integer anotherField;
    @Field(name = "yet_another_field")
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
