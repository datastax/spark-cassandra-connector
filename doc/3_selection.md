# Documentation
If you have the option we recommend using [DataFrames](14_data_frames.md) instead of RDDs

## Server-side data selection, filtering and grouping

In this section, you'll learn how to reduce the amount of data 
transferred from Cassandra to Spark to speed up processing.

### Selecting columns - .select()

For performance reasons, you should not fetch columns you don't need. 
You can achieve this with the `select` method.

#### Example Using Select to Prune Cassandra Columns
```scala
sc.cassandraTable("test", "users").select("username").toArray.foreach(println)
// CassandraRow{username: noemail} 
// CassandraRow{username: someone}
```

The `select` method can be chained. Every next call can be used to 
select a subset of columns already selected. Selecting a non-existing 
column would result in throwing an exception.

The `select` method allows querying for TTL and timestamp of the table cell. 

#### Example Using Select to Retreive TTL and Timestamp 
```scala
val row = rdd.select("column", "column".ttl, "column".writeTime).first
val ttl = row.getLong("ttl(column)")
val timestamp = row.getLong("writetime(column)")         
```

The selected columns can be given aliases by calling `as` on the column selector, 
which is particularly handy when fetching TTLs and timestamps.

#### Example Using "as" to Rename a Column
```scala
rdd.select("column".ttl as "column_ttl").first
val ttl = row.getLong("column_ttl")         
```

### Filtering rows - .where()

To filter rows, you can use the filter transformation provided by Spark. 
However, this approach causes all rows to be fetched from Cassandra and 
then filtered by Spark. Also, some CPU cycles are wasted serializing and 
deserializing objects that wouldn't be included in the result. To avoid 
this overhead, `CassandraRDD` offers the `where` method, which lets you pass 
arbitrary CQL condition(s) to filter the row set on the server.

#### Example Using Where to Filter Cassandra Data Server Side
```scala
sc.cassandraTable("test", "cars").select("id", "model").where("color = ?", "black").toArray.foreach(println)
// CassandraRow[id: KF-334L, model: Ford Mondeo]
// CassandraRow[id: MT-8787, model: Hyundai x35]

sc.cassandraTable("test", "cars").select("id", "model").where("color = ?", "silver").toArray.foreach(println)
// CassandraRow[id: WX-2234, model: Toyota Yaris]
```

Note: Although the `ALLOW FILTERING` clause is implicitly added to the 
generated CQL query, not all predicates are currently allowed by the 
Cassandra engine. This limitation is going to be addressed in the future 
Cassandra releases. Currently, `ALLOW FILTERING` works well 
with columns indexed by clustering columns.

### Ordering rows

CQL allows for requesting ascending or descending order of rows within 
a single Cassandra partition. It is allowed to pass the ordering 
direction by `withAscOrder` or `withDescOrder` methods of `CassandraRDD`. 
Note that it will work only if there is at least one clustering column in the table
and a partition key predicate is specified by `where` clause.

### Limiting rows

When a table is designed so that it include clustering keys and the use 
case is that only the first `n` rows from an explicitly specified 
Cassandra partition are supposed to be fetched, one can find
useful the `limit` method. It allows to add the `LIMIT` clause to each 
CQL statement executed for a particular RDD.

Note that, when you specify the limit without specifying a partition key 
predicate for the where clause, you will get unpredictable amount of 
rows because the limit will be applied on each Spark partition which 
is created for the RDD.

If you are connecting to Cassandra 3.6 or greater you can also use the 
`perPartitionLimit` method. This will add `PER PARTITION LIMIT` clause
to all CQL executed by the RDD. The `PARTITION` here is a Cassandra 
Partition so it will only retrieve `rowsNumber` CQL Rows for each 
partition key in the result.

### Grouping rows by partition key

Physically, Cassandra stores data already grouped by partition key and 
ordered by clustering column(s) within each partition. As a single 
Cassandra partition never spans multiple Spark partitions, it is possible 
to very efficiently group data by partition key without shuffling data around.
Call `spanBy` or `spanByKey` methods instead of `groupBy` or `groupByKey`:

```sql
CREATE TABLE events (year int, month int, ts timestamp, data varchar, PRIMARY KEY (year,month,ts));
```

```scala
sc.cassandraTable("test", "events")
  .spanBy(row => (row.getInt("year"), row.getInt("month")))

sc.cassandraTable("test", "events")
  .keyBy(row => (row.getInt("year"), row.getInt("month")))
  .spanByKey
```

Note: This only works for sequentially ordered data. Because data is ordered in Cassandra by the
clustering keys, all viable spans must follow the natural clustering key order.

This means in the above example that `spanBy` will be possible on (year), (year,month),
(year,month,ts) but not (month), (ts), or (month,ts).

The methods `spanBy` and `spanByKey` iterate every Spark partition locally
and put every RDD item into the same group as long as the key doesn't change.
Whenever the key changes, a new group is started. You need enough memory
to store the biggest group.

### Counting rows

Although Spark provides `count()` method, it requires all the rows to be 
fetched from Cassandra, which adds significant memory and network overhead. 
Instead, `cassandraCount()` method can be used on any  Cassandra based RDD 
to push down selection of `count(*)` and fetching the number of rows directly.
 
Note: Until release 1.2.4, Spark count was overridden by native 
Cassandra count in all Cassandra based RDDs. Since 1.2.4, `count()`
 is for Spark count and `cassandraCount()` is for Cassandra native count.

[Next - Working with user-defined case classes and tuples](4_mapper.md)