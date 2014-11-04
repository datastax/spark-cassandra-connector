# Documentation

## Using Connector in Java
This section describes how to access the functionality of Connector when you write your program in Java.
It is assumed that you already familiarized yourself with the previous sections and you understand how 
Connector works.

With Spark Cassandra Connector 1.1.x, Java API comes with significant changes and enhancements.

### Prerequisites 
On order to use Java API, you need to add the Java API module to the list of dependencies:

```scala
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector-java" % "1.1.0" withSources() withJavadoc()
```

The best way to use Connector Java API is to import statically all the methods in `CassandraJavaUtil`. 
This utility class is the main entry point for Connector Java API.

```java
import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;
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
use `cassandraTable` method, which accepts a custom `RowReaderFactory` - (see
[Working with user-defined case classes and tuples](4_mapper.md) for more details).

#### Obtaining CassandraJavaRDD

Since version 1.1.x, Java API comes with several useful factory methods which can be used to create factories of row
readers of the two major kinds: type converter based and column mapper based.

The type converter based row reader uses a single `TypeConverter` to map a single column from a row to some type. It
doesn't matter how many columns are in projection because it always choose the first one. This kind of row reader is
useful when one wants to select a single column from a table and map it directly to an *RDD* of values of such types as
*String*, *Integer*, etc. For example, we may want to get an *RDD* of prices in order to calculate the average:

Example:

```java
JavaRDD<Double> pricesRDD = javaFunctions(sc).cassandraTable("ks", "tab", mapColumnTo(Double.class)).select("price");
```

In the above example we explicitly select a single column (see the next subsection for details) and map it directly to
*Double*.

There are other overloaded versions of `mapColumnTo` methods which allow to map a column to one of the collection types,
or with use of an explicitly specified type converter.

The column mapper based row reader takes all the selected columns and maps them to some object with use of a given
`ColumnMapper`. The corresponding factories can be easily obtained by series of `mapRowTo` overloaded methods.

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
JavaRDD<Person> rdd = javaFunctions(sc).cassandraTable("ks", "people", mapRowTo(Person.class));
```

In this example, we created a `CassandraJavaRDD` of `Person` elements. While defining bean classes like 
`Person`, remember to define no-args constructor. Although, it is not required for it to be the only constructor 
of such a class.

By default, `mapRowTo` methods use `JavaBeanColumnMapper` with a default column name mapping logic. The column name
translation can be customised by providing pairs for column name and attribute name which have to be overridden. There
is also one overloaded `mapRowTo` methods which allows to specify a custom `ColumnMapper`. More details about column
mapper can be found in [Working with user-defined case classes and tuples](4_mapper.md) and
[Customizing the mapping between Scala and Cassandra](6_advanced_mapper.md).

#### Obtaining CassandraJavaPairRDD

Since 1.1.0 one can directly obtain a *CassandraJavaPairRDD*, which is an extension of *JavaPairRDD*. This can be done
easily by specifying two row reader factories (vs one row reader factory in the previous examples). The corresponding
row readers are responsible for resolving key and value from each row. The same methods `mapRowTo` and `mapColumnTo` can
be used to obtain the proper factories. However, one should keep in mind the following nuances:

Key row reader | Value row reader | Remarks
---------------|------------------|-----------
mapColumnTo    | mapColumnTo      | 1st column mapped to key, 2nd column mapped to value
mapColumnTo    | mapRowTo         | 1st column mapped to key, whole row mapped to value
mapRowTo       | mapColumnTo      | whole row mapped to key, 1st column mapped to value
mapRowTo       | mapRowTo         | whole row mapped to key, whole row mapped to value

Examples:

```java
CassandraJavaPairRDD<Integer, String> rdd1 = javaFunctions(sc)
    .cassandraTable("ks", "people", mapColumnTo(Integer.class), mapColumnTo(String.class))
    .select("id", "name");

CassandraJavaPairRDD<Integer, Person> rdd2 = javaFunctions(sc)
    .cassandraTable("ks", "people", mapColumnTo(Integer.class), mapRowTo(Person.class))
    .select("id", "name", "birth_date");
```

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

`javaFunctions` method can be also applied to any *RDD* in order to provide `writerBuilder` factory method.
In Spark Cassandra Connector prior to 1.1.0 there are a number of overloaded `saveToCassandra` methods because
of a lack of default values support for arguments and implicit conversions. Starting from version 1.1.0 they were
replaced by a builder object `RDDAndDStreamCommonJavaFunctions.WriterBuilder`, which can be obtained by invoking
`writerBuilder` method on the *RDD* wrapper. When the builder is eventually configured, one needs to call
`saveToCassandra` method on it to run writing job.

In the following example, a `JavaRDD` of `Person` elements is saved to Cassandra table `ks.people` with a default
mapping and configuration.

Example: 

```java
List<Person> people = Arrays.asList(
        Person.newInstance(1, "John", new Date()),
        Person.newInstance(2, "Anna", new Date()),
        Person.newInstance(3, "Andrew", new Date())
);
JavaRDD<Person> rdd = sc.parallelize(people);
javaFunctions(rdd).writerBuilder("ks", "people", mapToRow(Person.class)).saveToCassandra();
```

There are several `mapToRow` overloaded methods available to make it easier to get the proper `RowWriterFactory`
instance (which is the required third argument of `writerBuilder` method). In its simplest form, it takes the class
of *RDD* elements and uses a default `JavaBeanColumnMapper` to map those elements to Cassandra rows. Custom column name
to attribute translations can be specified in order to override the default logic. If `JavaBeanColumnMapper` is not an
option, a custom column mapper can be specified as well.

### Extensions for Spark Streaming

The main entry point for Spark Streaming in Java is `JavaStreamingContext` object. Like for `JavaSparkContext`, we 
can use `javaFunctions` method to access Connector specific functionality. For example, we can create an ordinary 
`CassandraJavaRDD` by invoking the same `cassandraTable` method as we do for `SparkContext`. There is nothing specific
to streaming in this case - these methods are provided only for convenience and they use `SparkContext` wrapped by 
`StreamingContext` under the hood. 

You may also save the data from `JavaDStream` to Cassandra. Again, you need to use `javaFunctions` method to create 
a special wrapper around `JavaDStream` and then invoke `writerBuilder` method and finally `saveToCassandra` on it.
*DStream* is a sequence of *RDDs* and when you invoke `saveToCassandra` on the builder, it will follow saving to
Cassandra all the *RDDs* in that *DStream*.

[Next - Spark Streaming with Cassandra](8_streaming.md)
