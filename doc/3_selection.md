# Documentation
## Server-side data selection, filtering and grouping

In this section, you'll learn how to reduce the amount of data transferred from Cassandra to Spark
to speed up processing.

### Selecting a subset of columns

For performance reasons, you should not fetch columns you don't need. You can achieve this with the `select` method.

```scala
sc.cassandraTable("test", "users").select("username").toArray.foreach(println)
// CassandraRow{username: noemail} 
// CassandraRow{username: someone}
```

The `select` method can be chained. Every next call can be used to select a subset of columns already selected.
Selecting a non-existing column would result in throwing an exception.

### Filtering rows

To filter rows, you can use the filter transformation provided by Spark. 
However, this approach causes all rows to be fetched from Cassandra and then filtered by Spark. 
Also, some CPU cycles are wasted serializing and deserializing objects that wouldn't be 
included in the result. To avoid this overhead, `CassandraRDD` offers the `where` method, which lets you pass 
arbitrary CQL condition(s) to filter the row set on the server.

```scala
sc.cassandraTable("test", "cars").select("id", "model").where("color = ?", "black").toArray.foreach(println)
// CassandraRow[id: KF-334L, model: Ford Mondeo]
// CassandraRow[id: MT-8787, model: Hyundai x35]

sc.cassandraTable("test", "cars").select("id", "model").where("color = ?", "silver").toArray.foreach(println)
// CassandraRow[id: WX-2234, model: Toyota Yaris]
```

Note: Although the `ALLOW FILTERING` clause is implicitly added to the generated CQL query, not all predicates 
are currently allowed by the Cassandra engine. This limitation is going to be addressed in the future 
Cassandra releases. Currently, `ALLOW FILTERING` works well 
with columns indexed by secondary indexes or clustering columns.  

### Grouping rows by partition key

Physically, cassandra stores data already grouped by Cassandra partition key and ordered by clustering
column(s) within each partition. As a single Cassandra partition never spans multiple Spark partitions,
it is possible to very efficiently group data by partition key without shuffling data around.
Call `spanBy` or `spanByKey` methods instead of `groupBy` or `groupByKey`:

```sql
CREATE TABLE events (year int, month int, ts timestamp, data varchar);
```

```scala
sc.cassandraTable("test", "events")
  .spanBy(row => (row.getInt("year"), row.getInt("month")))

sc.cassandraTable("test", "events")
  .keyBy(row => (row.getInt("year"), row.getInt("month")))
  .spanByKey
```

The methods `spanBy` and `spanByKey` iterate every Spark partition locally
and put every RDD item into the same group as long as the key doesn't change.
Whenever the key changes, a new group is started. You need enough memory
to store the biggest group.

[Next - Working with user-defined case classes and tuples](4_mapper.md)