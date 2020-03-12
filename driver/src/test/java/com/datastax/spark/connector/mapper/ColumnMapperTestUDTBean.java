package com.datastax.spark.connector.mapper;

import com.datastax.oss.driver.api.mapper.annotations.CqlName;

import java.io.Serializable;

public class ColumnMapperTestUDTBean implements Serializable {
    public Integer field;
    @CqlName(value = "cassandra_another_field")
    public Integer anotherField;
    @CqlName(value = "cassandra_yet_another_field")
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

