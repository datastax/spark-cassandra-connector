package com.datastax.spark.connector;

import java.io.Serializable;

/**
 * This is a sample JavaBean style class. In order to test JavaAPI correctly, we cannot implement this in Scala because
 * Scala adds some additional accessors and mutators.
 */
public class SampleWithNestedJavaBean implements Serializable {
    public class SampleNestedJavaBean implements Serializable {
        private Integer key;
        private String value;

        public SampleNestedJavaBean(Integer key) {
            this.key = key;
        }

        public SampleNestedJavaBean() {
        }

        public SampleNestedJavaBean(Integer key, String value) {
            this.key = key;
            this.value = value;
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
}
