package com.datastax.driver.spark.demo;

import com.datastax.driver.spark.CassandraRow;
import com.google.common.base.Objects;
import org.apache.hadoop.util.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static com.datastax.driver.spark.CassandraJavaUtil.NO_OVERRIDE;
import static com.datastax.driver.spark.CassandraJavaUtil.javaFunctions;

public class JavaApiDemo implements Serializable {

    public static class Person implements Serializable {
        private Integer id;
        private String name;
        private Date birthDate;

        public static Person newInstance(Integer id, String name, Date birthDate) {
            Person person = new Person();
            person.setId(id);
            person.setName(name);
            person.setBirthDate(birthDate);
            return person;
        }

        public Integer getId() {
            return id;
        }

        public void setId(Integer id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Date getBirthDate() {
            return birthDate;
        }

        public void setBirthDate(Date birthDate) {
            this.birthDate = birthDate;
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("id", id)
                    .add("name", name)
                    .add("birthDate", birthDate)
                    .toString();
        }
    }

    public JavaApiDemo() {
        DemoApp demoApp = DemoApp$.MODULE$.apply();
        JavaSparkContext sc = new JavaSparkContext(demoApp.sc());

        // Here we are going to save some data to Cassandra...
        List<Person> people = Arrays.asList(
                Person.newInstance(1, "John", new Date()),
                Person.newInstance(2, "Anna", new Date()),
                Person.newInstance(3, "Andrew", new Date())
        );
        JavaRDD<Person> rdd = sc.parallelize(people);
        javaFunctions(rdd, Person.class).saveToCassandra("test", "people");

        // then, we want to read that data as an RDD of CassandraRows and convert them to strings...
        JavaRDD<String> cassandraRowsRDD = javaFunctions(sc).cassandraTable("test", "people").toJavaRDD()
                .map(new Function<CassandraRow, String>() {
                    @Override
                    public String call(CassandraRow cassandraRow) throws Exception {
                        return cassandraRow.toString();
                    }
                });
        System.out.println("Data as CassandraRows: \n" + StringUtils.join("\n", cassandraRowsRDD.toArray()));

        // finally, we want to read that data as an RDD of Person beans and also convert them to strings...
        JavaRDD<String> rdd2 = javaFunctions(sc).cassandraTable("test", "people", Person.class).toJavaRDD()
                .map(new Function<Person, String>() {
                    @Override
                    public String call(Person person) throws Exception {
                        return person.toString();
                    }
                });
        System.out.println("Data as Person beans: \n" + StringUtils.join("\n", rdd2.toArray()));

        sc.stop();
    }

    public static void main(String[] args) {
        new JavaApiDemo();
    }

}
