# Documentation

## Using Connector in Java
This section describes how to access the functionality of Connector when you write your program in Java.
It is assumed that you familiarized with the previous sections and you understand how Connector works.

The best way to use Connector Java API is to import statically all the methods in `CassandraJavaUtil`. 
This utility class is the main entry point for Connector Java API.

### Working with CassandraJavaRDD
`CassandraJavaRDD` is a `CassandraRDD` counterpart in Java. It allows to invoke easily Connector specific methods
in order to enforce selection or projection on the database side. However, conversely to `CassandraRDD`, it extends
`JavaRDD` which is much more suitable for the development of Spark applications in Java. 

In order to create `CassandraJavaRDD` you need to invoke one of the `cassandraTable` methods of a special 
wrapper around `SparkContext`. The wrapper can be easily created with use of one of the overloaded `javaFunctions`
method in `CassandraJavaUtil`. 

Example:

```java
JavaSparkContext jsc = new JavaSparkContext(sc);
JavaRDD<String> cassandraRowsRDD = javaFunctions(sc).cassandraTable("ks", "tab").toJavaRDD()
        .map(new Function<CassandraRow, String>() {
            @Override
            public String call(CassandraRow cassandraRow) throws Exception {
                return cassandraRow.toString();
            }
        });
System.out.println("Data as CassandraRows: \n" + StringUtils.join("\n", cassandraRowsRDD.toArray()));
```

### Extensions for Spark Streaming
