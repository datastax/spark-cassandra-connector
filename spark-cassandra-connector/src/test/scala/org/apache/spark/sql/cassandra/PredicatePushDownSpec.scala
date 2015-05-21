package org.apache.spark.sql.cassandra

import org.scalatest.{Matchers, FlatSpec}

import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.types.IntType

class PredicatePushDownSpec extends FlatSpec with Matchers {

  // We don't want this test to rely on any Spark code,
  // so we're using our own Filters
  trait Filter
  case class EqFilter(columnName: String) extends Filter
  case class InFilter(columnName: String) extends Filter
  case class LtFilter(columnName: String) extends Filter
  case class GtFilter(columnName: String) extends Filter
  case object UnsupportedFilter extends Filter

  implicit object FilterOps extends PredicateOps[Filter] {
    override def columnName(p: Filter) = p match {
      case EqFilter(name) => name
      case InFilter(name) => name
      case LtFilter(name) => name
      case GtFilter(name) => name
      case UnsupportedFilter => throw new IllegalArgumentException("Unsupported predicate")
    }
    override def isRangePredicate(p: Filter) = p.isInstanceOf[LtFilter] || p.isInstanceOf[GtFilter]
    override def isSingleColumnPredicate(p: Filter) = p != UnsupportedFilter
    override def isEqualToPredicate(p: Filter) = p.isInstanceOf[EqFilter]
    override def isInPredicate(p: Filter) = p.isInstanceOf[InFilter]
  }

  val pk1 = ColumnDef("pk1", PartitionKeyColumn, IntType)
  val pk2 = ColumnDef("pk2", PartitionKeyColumn, IntType)
  val c1 = ColumnDef("c1", ClusteringColumn(0), IntType)
  val c2 = ColumnDef("c2", ClusteringColumn(1), IntType)
  val c3 = ColumnDef("c3", ClusteringColumn(2), IntType)
  val i1 = ColumnDef("i1", RegularColumn, IntType, indexed = true)
  val i2 = ColumnDef("i2", RegularColumn, IntType, indexed = true)
  val r1 = ColumnDef("r1", RegularColumn, IntType)
  val r2 = ColumnDef("r2", RegularColumn, IntType)

  val table = TableDef(
    keyspaceName = "test",
    tableName = "test",
    partitionKey = Seq(pk1, pk2),
    clusteringColumns = Seq(c1, c2, c3),
    regularColumns = Seq(i1, i2, r1, r2)
  )

  "PredicatePushDown" should "push down all equality predicates restricting partition key columns" in {
    val f1 = EqFilter("pk1")
    val f2 = EqFilter("pk2")
    val ppd = new PredicatePushDown(Set[Filter](f1, f2), table)
    ppd.predicatesToPushDown should contain allOf(f1, f2)
    ppd.predicatesToPreserve shouldBe empty
  }

  it should "not push down a partition key predicate for a part of the partition key" in {
    val f1 = EqFilter("pk1")
    val ppd1 = new PredicatePushDown(Set[Filter](f1), table)
    ppd1.predicatesToPushDown shouldBe empty
    ppd1.predicatesToPreserve should contain(f1)

    val f2 = EqFilter("pk2")
    val ppd2 = new PredicatePushDown(Set[Filter](f2), table)
    ppd2.predicatesToPushDown shouldBe empty
    ppd2.predicatesToPreserve should contain(f2)
  }

  it should "not push down a range partition key predicate" in {
    val f1 = EqFilter("pk1")
    val f2 = LtFilter("pk2")
    val ppd = new PredicatePushDown(Set[Filter](f1, f2), table)
    ppd.predicatesToPushDown shouldBe empty
    ppd.predicatesToPreserve should contain allOf(f1, f2)
  }

  it should "push down an IN partition key predicate on the last partition key column" in {
    val f1 = EqFilter("pk1")
    val f2 = InFilter("pk2")
    val ppd = new PredicatePushDown(Set[Filter](f1, f2), table)
    ppd.predicatesToPushDown should contain allOf(f1, f2)
    ppd.predicatesToPreserve shouldBe empty
  }

  it should "not push down an IN partition key predicate on the non-last partition key column" in {
    val f1 = InFilter("pk1")
    val f2 = EqFilter("pk2")
    val ppd = new PredicatePushDown(Set[Filter](f1, f2), table)
    ppd.predicatesToPushDown shouldBe empty
    ppd.predicatesToPreserve should contain allOf(f1, f2)
  }

  it should "push down the first clustering column predicate" in {
    val f1 = EqFilter("c1")
    val ppd = new PredicatePushDown(Set[Filter](f1), table)
    ppd.predicatesToPushDown should contain only f1
    ppd.predicatesToPreserve shouldBe empty
  }

  it should "push down the first and the second clustering column predicate" in {
    val f1 = EqFilter("c1")
    val f2 = LtFilter("c2")
    val ppd = new PredicatePushDown(Set[Filter](f1, f2), table)
    ppd.predicatesToPushDown should contain only(f1, f2)
    ppd.predicatesToPreserve shouldBe empty
  }

  it should "push down restrictions on only the initial clustering columns" in {
    val f1 = EqFilter("c1")
    val f2 = EqFilter("c3")
    
    val ppd1 = new PredicatePushDown(Set[Filter](f1, f2), table)
    ppd1.predicatesToPushDown should contain only f1
    ppd1.predicatesToPreserve should contain only f2

    val ppd2 = new PredicatePushDown(Set[Filter](f2), table)
    ppd2.predicatesToPushDown shouldBe empty
    ppd2.predicatesToPreserve should contain only f2
  }

  it should "push down only one range predicate restricting the first clustering column, " +
      "if there are more range predicates on different clustering columns" in {
    val f1 = LtFilter("c1")
    val f2 = LtFilter("c2")
    val ppd = new PredicatePushDown(Set[Filter](f1, f2), table)
    ppd.predicatesToPushDown should contain only f1
    ppd.predicatesToPreserve should contain only f2
  }

  it should "push down multiple range predicates for the same clustering column" in {
    val f1 = LtFilter("c1")
    val f2 = GtFilter("c1")
    val ppd = new PredicatePushDown(Set[Filter](f1, f2), table)
    ppd.predicatesToPushDown should contain allOf (f1, f2)
    ppd.predicatesToPreserve shouldBe empty
  }

  it should "push down clustering column predicates when the last clustering column is restricted by IN" in {
    val f1 = EqFilter("c1")
    val f2 = EqFilter("c2")
    val f3 = InFilter("c3")
    val ppd = new PredicatePushDown(Set[Filter](f1, f2, f3), table)
    ppd.predicatesToPushDown should contain only(f1, f2, f3)
    ppd.predicatesToPreserve shouldBe empty
  }

  it should "stop pushing down clustering column predicates on the first range predicate" in {
    val f1 = EqFilter("c1")
    val f2 = LtFilter("c2")
    val f3 = EqFilter("c3")
    val ppd = new PredicatePushDown(Set[Filter](f1, f2, f3), table)
    ppd.predicatesToPushDown should contain only(f1, f2)
    ppd.predicatesToPreserve should contain only f3
  }

  it should "not push down IN restriction on non-last column" in {
    val f1 = EqFilter("c1")
    val f2 = InFilter("c2")
    val f3 = EqFilter("c3")
    val ppd = new PredicatePushDown(Set[Filter](f1, f2, f3), table)
    ppd.predicatesToPushDown should contain only f1
    ppd.predicatesToPreserve should contain only (f2, f3)
  }

  it should "not push down any clustering column predicates, if the first clustering column is missing" in {
    val f1 = EqFilter("c2")
    val ppd = new PredicatePushDown(Set[Filter](f1), table)
    ppd.predicatesToPushDown shouldBe empty
    ppd.predicatesToPreserve should contain only f1
  }

  it should "push down equality predicates on regular indexed columns" in {
    val f1 = EqFilter("i1")
    val ppd = new PredicatePushDown(Set[Filter](f1), table)
    ppd.predicatesToPushDown should contain only f1
    ppd.predicatesToPreserve shouldBe empty
  }

  it should "not push down range predicates on regular indexed columns" in {
    val f1 = LtFilter("i1")
    val ppd = new PredicatePushDown(Set[Filter](f1), table)
    ppd.predicatesToPushDown shouldBe empty
    ppd.predicatesToPreserve should contain only f1
  }

  it should "not push down IN predicates on regular indexed columns" in {
    val f1 = InFilter("i1")
    val ppd = new PredicatePushDown(Set[Filter](f1), table)
    ppd.predicatesToPushDown shouldBe empty
    ppd.predicatesToPreserve should contain only f1
  }

  it should "push down predicates on regular non-indexed and indexed columns" in {
    val f1 = EqFilter("r1")
    val f2 = EqFilter("r2")
    val f3 = EqFilter("i1")
    val ppd = new PredicatePushDown(Set[Filter](f1, f2, f3), table)
    ppd.predicatesToPushDown should contain allOf(f1, f2, f3)
    ppd.predicatesToPreserve shouldBe empty
  }

  it should "not push down predicates on regular non-indexed columns if indexed ones are not included" in {
    val f1 = EqFilter("r1")
    val f2 = EqFilter("r2")
    val ppd = new PredicatePushDown(Set[Filter](f1, f2), table)
    ppd.predicatesToPushDown shouldBe empty
    ppd.predicatesToPreserve should contain allOf(f1, f2)
  }
  
  it should "prefer to push down equality predicates over range predicates" in {
    val f1 = EqFilter("c1")
    val f2 = EqFilter("c2")
    val f3 = LtFilter("c2")
    val ppd = new PredicatePushDown(Set[Filter](f1, f2, f3), table)
    ppd.predicatesToPushDown should contain only(f1, f2)
    ppd.predicatesToPreserve should contain only f3
  }

  it should "not push down unsupported predicates" in {
    val f1 = EqFilter("i1")
    val f2 = UnsupportedFilter
    val ppd = new PredicatePushDown(Set[Filter](f1, f2), table)
    ppd.predicatesToPushDown should contain only f1
    ppd.predicatesToPreserve should contain only f2
  }
}
