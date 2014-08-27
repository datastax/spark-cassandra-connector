package com.datastax.spark.connector.rdd.partitioner

import java.net.InetAddress
import java.nio.ByteBuffer

import com.datastax.driver.core.DataType
import com.datastax.spark.connector.types.ColumnType
import org.apache.spark.Logging

import com.datastax.spark.connector.cql.{CassandraConnector, TableDef}
import com.datastax.spark.connector.rdd._
import com.datastax.spark.connector.rdd.partitioner.dht.{Token, TokenFactory}
import com.datastax.spark.connector.util.CqlWhereParser._
import com.datastax.spark.connector.util._
import org.apache.cassandra.thrift
import org.apache.cassandra.thrift.Cassandra
import org.apache.spark.Partition
import org.apache.thrift.TApplicationException

import scala.collection.JavaConversions._
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool
import scala.reflect.macros.ParseException

/** Creates CassandraPartitions for given Cassandra table */
class CassandraRDDPartitioner[V, T <: Token[V]] (
                                                 connector: CassandraConnector,
                                                 tableDef: TableDef,
                                                 splitSize: Long)(
                                                 implicit
                                                 tokenFactory: TokenFactory[V, T])  extends Logging {

  type Token = com.datastax.spark.connector.rdd.partitioner.dht.Token[T]
  type TokenRange = com.datastax.spark.connector.rdd.partitioner.dht.TokenRange[V, T]

  private val keyspaceName = tableDef.keyspaceName
  private val tableName = tableDef.tableName
  val pkNames = tableDef.partitionKey.map(_.columnName)

  private def unthriftify(tr: thrift.TokenRange): TokenRange = {
    val startToken = tokenFactory.fromString(tr.start_token)
    val endToken = tokenFactory.fromString(tr.end_token)
    val endpoints = tr.endpoints.map(InetAddress.getByName).toSet
    new TokenRange(startToken, endToken, endpoints, None)
  }

  private def describeRing(client: Cassandra.Iface): Seq[TokenRange] = {
    val ring =
      try {
        client.describe_local_ring(keyspaceName)
      }
      catch {
        case e: TApplicationException if e.getType == TApplicationException.UNKNOWN_METHOD =>
          client.describe_ring(keyspaceName)
        case e: java.lang.NoSuchMethodError =>
          client.describe_ring(keyspaceName)
      }
    ring.map(unthriftify)
  }

  private def quote(name: String) = "\"" + name + "\""

  private def splitsOf(tokenRanges: Iterable[TokenRange], splitter: TokenRangeSplitter[V, T]): Iterable[TokenRange] = {
    val parTokenRanges = tokenRanges.par
    parTokenRanges.tasksupport = new ForkJoinTaskSupport(CassandraRDDPartitioner.pool)
    (for (tokenRange <- parTokenRanges;
          split <- splitter.split(tokenRange, splitSize)) yield split).seq
  }

  private def splitToCqlClause(range: TokenRange): Iterable[CqlTokenRange] = {
    val startToken = tokenFactory.toString(range.start)
    val endToken = tokenFactory.toString(range.end)
    val pk = pkNames.map(quote).mkString(", ")

    if (range.end == tokenFactory.minToken)
      List(CqlTokenRange(s"token($pk) > $startToken"))
    else if (range.start == tokenFactory.minToken)
      List(CqlTokenRange(s"token($pk) <= $endToken"))
    else if (!range.isWrapAround)
      List(CqlTokenRange(s"token($pk) > $startToken AND token($pk) <= $endToken"))
    else
      List(
        CqlTokenRange(s"token($pk) > $startToken"),
        CqlTokenRange(s"token($pk) <= $endToken"))
  }

  /** This works only for numeric tokens */
  private def tokenCount(range: TokenRange): BigInt = {
    val start = BigInt(tokenFactory.toString(range.start))
    val end = BigInt(tokenFactory.toString(range.end))
    if (start < end)
      end - start
    else
      end - start + tokenFactory.totalTokenCount
  }

  /** Rows per token average is required for fast local range splitting.
    * Used only for Murmur3Partitioner and RandomPartitioner.  */
  private def estimateCassandraPartitionsPerToken(tokenRanges: Seq[TokenRange]): Double = {
    val random = new scala.util.Random(0)
    val tokenRangeSample = random.shuffle(tokenRanges).take(CassandraRDDPartitioner.TokenRangeSampleSize)
    val splitter = new ServerSideTokenRangeSplitter(connector, keyspaceName, tableName, tokenFactory)
    val splits = splitsOf(tokenRangeSample, splitter)
    val tokenCountSum = splits.map(tokenCount).sum
    val rowCountSum = splits.map(_.rowCount.get).sum
    rowCountSum.toDouble / tokenCountSum.toDouble
  }

  private def createSplitterFor(tokenRanges: Seq[TokenRange]): TokenRangeSplitter[V, T] = {
    tokenFactory.asInstanceOf[TokenFactory[_, _]] match {
      case TokenFactory.RandomPartitionerTokenFactory =>
        val partitionsPerToken = estimateCassandraPartitionsPerToken(tokenRanges)
        new RandomPartitionerTokenRangeSplitter(partitionsPerToken).asInstanceOf[TokenRangeSplitter[V, T]]
      case TokenFactory.Murmur3TokenFactory =>
        val partitionsPerToken = estimateCassandraPartitionsPerToken(tokenRanges)
        new Murmur3PartitionerTokenRangeSplitter(partitionsPerToken).asInstanceOf[TokenRangeSplitter[V, T]]
      case _ =>
        new ServerSideTokenRangeSplitter(connector, keyspaceName, tableName, tokenFactory)
    }
  }

  private def containsPartitionKey(predicates: Seq[Predicate]) = {
    predicates.map(_.columnName).intersect(pkNames).nonEmpty
  }

  private def crossPredicates(xs: Seq[Seq[EqPredicate]], ys: Seq[EqPredicate]): Seq[Seq[EqPredicate]] =
    if (xs.isEmpty) ys.map(Seq(_))
    else
      for {x <- xs; y <- ys} yield x :+ y


  private def serializeValue (columnType: ColumnType[_], value: Literal): ByteBuffer = {
    val dataType:DataType = ColumnType.toPrimitiveDriverType(columnType)
    value match {
      case Placeholder(v) => dataType.serialize(columnType.converterToCassandra.convert(v))
      case Value(v) => dataType.parse(v)
    }
  }
  def calculateToken(keyPredicates: Seq[EqPredicate]) =  {
    val values = keyPredicates.map(p => (p.columnName-> p.value)).toMap
    val serValues = tableDef.partitionKey.map(c =>
       serializeValue(c.columnType, values(c.columnName)))
    // TODO support composed keys
    tokenFactory.getToken(serValues(0))
  }
  def endpointForKey (keyPredicates: Seq[EqPredicate], tokenRanges:Seq[TokenRange]): Iterable[InetAddress] = {
    val token = calculateToken(keyPredicates)
    tokenRanges.filter(_.contains(token) ).flatMap(_.endpoints).distinct
  }

  private def partitionKeBasedPartitions (whereClause: CqlWhereClause,
                                        predicates : Seq[Predicate],
                                        tokenRanges:Seq[TokenRange]): Array[Partition] = {

    val (knownPredicates, unknownPredicates) = predicates.span (!_.isInstanceOf[UnknownPredicate])
    val (keyPredicates, nonKeyPredicates) = knownPredicates.partition(p => pkNames contains p.columnName)
    val crossedKeyPredicates: Seq[Seq[EqPredicate]] = keyPredicates.foldLeft (Seq[Seq[EqPredicate]]()) {
      (list, predicate) =>
        predicate match {
          case predicate: EqPredicate =>  crossPredicates(list, Seq(predicate))

          case InPredicate(columnName, value) => {
            val eqPredicates = value.asInstanceOf[Product].productIterator.toSeq.map(v =>
              EqPredicate(columnName, Placeholder(v)))
            crossPredicates(list, eqPredicates)
          }

          case InPredicateList(columnName, values) => {
            val eqPredicates = values.map(EqPredicate(columnName, _))
            crossPredicates(list, eqPredicates)
          }

          case _ => throw new RuntimeException("Only in and = operation accepted for partition keys")

        }
    }
    if(keyPredicates.isEmpty) throw new RuntimeException("key predicates are empty")

    (for ((keyPredicates, index) <- crossedKeyPredicates.zipWithIndex) yield {
      val predicates = keyPredicates ++ nonKeyPredicates  ++ unknownPredicates
      //collect known values for '?' for current predicate
      val qValues = predicates.map(_ match {
        case InPredicateList(_, values) => values collect { case Placeholder(value) => value}
        case InPredicate(_, value) => Seq(value)
        case EqPredicate(_, Placeholder(value)) => Seq(value)
        case RangePredicate(_, _, Placeholder(value)) => Seq(value)
        case UnknownPredicate(_, _, values) => values
        case _ => Seq()
      }).reduce(_ ++ _)

     CassandraCustomPartition (index, endpointForKey(keyPredicates, tokenRanges),
            CqlWhereClause (predicates.map (_.toCqlString), qValues))
    }).toArray
  }

  /** Computes Spark partitions of the given table. Called by [[CassandraRDD]]. */
  def partitions(whereClause: CqlWhereClause): Array[Partition] = {
    connector.withCassandraClientDo {
      client =>
        val tokenRanges = describeRing(client)
        val endpointCount = tokenRanges.map(_.endpoints).reduce(_ ++ _).size
        val splitter = createSplitterFor(tokenRanges)
        val splits = splitsOf(tokenRanges, splitter).toSeq
        val maxGroupSize = tokenRanges.size / endpointCount
        val clusterer = new TokenRangeClusterer[V, T](splitSize, maxGroupSize)
        val groups = clusterer.group(splits).toArray
        val qValIterator=whereClause.values.iterator
        val predicates: Seq[Predicate] = whereClause.predicates.map(
          predicate => CqlWhereParser.predicates(predicate, qValIterator)).fold(Seq[Predicate]()) {
          _ ++ _
        }
        try {
          if (containsPartitionKey(predicates)) {
            partitionKeBasedPartitions (whereClause,predicates,tokenRanges)
          } else
            for ((group, index) <- groups.zipWithIndex) yield {
              val cqlPredicates = group.flatMap(splitToCqlClause)
              val endpoints = group.map(_.endpoints).reduce(_ intersect _)
              val rowCount = group.map(_.rowCount.get).sum
              CassandraRingPartition(index, endpoints, cqlPredicates, rowCount)
            }
        } catch {
          case x: Throwable => {
            logWarning("Partitioning error, one Partition for the query where created", x);
            Array(CassandraRingPartition(0, tokenRanges.flatMap(_.endpoints).distinct, List(CqlTokenRange("")), 0))
          }
        }
    }

  }

}

object CassandraRDDPartitioner {
  /** Affects how many concurrent threads are used to fetch split information from cassandra nodes, in `getPartitions`.
    * Does not affect how many Spark threads fetch data from Cassandra. */
  val MaxParallelism = 16

  /** How many token ranges to sample in order to estimate average number of rows per token */
  val TokenRangeSampleSize = 16

  private val pool: ForkJoinPool = new ForkJoinPool(MaxParallelism)
}
