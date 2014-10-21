# Documentation

## Using Connector in Java
This section describes how to access the functionality of Connector when you write your program in Java.
It is assumed that you already familiarized yourself with the previous sections and you understand how 
Connector works.

### Prerequisites 
On order to use Java API, you need to add the Java API module to the list of dependencies:

```scala
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector-java" % "1.0.0" withSources() withJavadoc()
```

The best way to use Connector Java API is to import statically all the methods in `CassandraJavaUtil`. 
This utility class is the main entry point for Connector Java API.

```java
import static com.datastax.spark.connector.CassandraJavaUtil.*;
```

### Accessing Cassandra tables in Java
`CassandraJavaRDD` is a `CassandraRDD` counterpart in Java. It allows to invoke easily Connector specific methods
in order to enforce selection or projection on the database side. However, conversely to `CassandraRDD`, it extends
`JavaRDD` which is much more suitable for the development of Spark applications in Java. 

In order to create `CassandraJavaRDD` you need to invoke one of the `cassandraTable` methods of a special 
wrapper around `SparkContext`. The wrapper can be easily obtained with use of one of the overloaded `javaFunctions`
method in `CassandraJavaUtil`. 

Example:

```java
JavaRDD<String> cassandraRowsRDD = javaFunctions(sc).cassandraTable("ks", "tab")
        .map(new Function<CassandraRow, String>() {
            @Override
            public String call(CassandraRow cassandraRow) throws Exception {
                return cassandraRow.toString();
            }
        });
System.out.println("Data as CassandraRows: \n" + StringUtils.join("\n", cassandraRowsRDD.toArray()));
```

In the above example, `cassandraTable` method has been used to create `CassandraJavaRDD` view of the data in `ks.tab`.
The elements of the returned *RDD* are of `CassandraRow` type. If you want to produce an *RDD* of custom beans, you may
use `cassandraTable` method, which accepts the class of *RDD*'s elements.
 
Example:

```java
// firstly, we define a bean class
public static class Person implements Serializable {
    private Integer id;
    private String name;
    private Date birthDate;

    // Remember to declare no-args constructor
    public Person() { }

    public Integer getId() { return id; }
    public void setId(Integer id) { this.id = id; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public Date getBirthDate() { return birthDate; }
    public void setBirthDate(Date birthDate) { this.birthDate = birthDate; }

    // other methods, constructors, etc.
}
```

```java
JavaRDD<String> rdd = javaFunctions(sc).cassandraTable("ks", "people", Person.class)
        .map(new Function<Person, String>() {
            @Override
            public String call(Person person) throws Exception {
                return person.toString();
            }
        });
System.out.println("Data as Person beans: \n" + StringUtils.join("\n", rdd.toArray()));
```

In this example, we created a `CassandraJavaRDD` of `Person` elements. While defining bean classes like 
`Person`, remember to define no-args constructor. Although, it is not required for it to be the only constructor 
of such a class.

### Using selection and projection on the database side
Once `CassandraJavaRDD` is created, you may apply selection and projection on that RDD by invoking `where` 
and `select` methods on it respectively. Their semantic is the same as the semantic of their counterparts
in `CassandraRDD`. 

Example:
```java
JavaRDD<String> rdd = javaFunctions(sc).cassandraTable("ks", "tab")
        .select("id").map(new Function<CassandraRow, String>() {
            @Override
            public String call(CassandraRow cassandraRow) throws Exception {
                return cassandraRow.toString();
            }
        });
System.out.println("Data with only 'id' column fetched: \n" + StringUtils.join("\n", rdd.toArray()));
```

Example:
```java
JavaRDD<String> rdd = javaFunctions(sc).cassandraTable("ks", "tab")
        .where("name=?", "Anna").map(new Function<CassandraRow, String>() {
            @Override
            public String call(CassandraRow cassandraRow) throws Exception {
                return cassandraRow.toString();
            }
        });
System.out.println("Data filtered by the where clause (name='Anna'): \n" + StringUtils.join("\n", rdd.toArray()));
```

### Saving data to Cassandra

`javaFunctions` method can be also applied to any *RDD* in order to provide `saveToCassandra` overloaded methods.
Because Java lacks default values for arguments and implicit conversions, and for user's convenience, `saveToCassandra` 
method is overloaded with a large amount of argument combinations. In the following example, a `JavaRDD` of `Person`
elements is saved to Cassandra table `ks.people`. 

Example: 

```java
List<Person> people = Arrays.asList(
        Person.newInstance(1, "John", new Date()),
        Person.newInstance(2, "Anna", new Date()),
        Person.newInstance(3, "Andrew", new Date())
);
JavaRDD<Person> rdd = sc.parallelize(people);
javaFunctions(rdd, Person.class).saveToCassandra("ks", "people");
```

### Extensions for Spark Streaming

The main entry point for Spark Streaming in Java is `JavaStreamingContext` object. Like for `JavaSparkContext`, we 
can use `javaFunctions` method to access Connector specific functionality. For example, we can create an ordinary 
`CassandraJavaRDD` by invoking the same `cassandraTable` method as we do for `SparkContext`. There is nothing specific
to streaming in this case - these methods are provided only for convenience and they use `SparkContext` wrapped by 
`StreamingContext` under the hood. 

You may also save the data from `JavaDStream` to Cassandra. Again, you need to use `javaFunctions` method to create 
a special wrapper around `JavaDStream` and then invoke one of the `saveToCassandra` methods. *DStream* is a sequence
of *RDDs* and when you invoke `saveToCassandra` on it, it will follow saving to Cassandra all the *RDDs* in that *DStream*.

[Next - Spark Streaming with Cassandra](8_streaming.md)
