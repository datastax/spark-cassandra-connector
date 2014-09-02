package com.datastax.spark.connector.util

import java.util.UUID

import com.datastax.spark.connector.util.CqlWhereParser._

import org.scalatest.{Inside, FlatSpec, Matchers}


class CqlWhereParserTest extends FlatSpec with Matchers with Inside {
  var parser = CqlWhereParser

  "CqlWhereParser" should "parse 'and' operations" in {
    parser.parse("abc > 10 and cde >= 20 and c < 1") should be (
      List(
        RangePredicate("abc", GreaterThan, NumberLiteral("10")),
        RangePredicate("cde", GreaterEqual, NumberLiteral("20")),
        RangePredicate("c", LowerThan, NumberLiteral("1"))
      )
    )
  }

  it should "parse equality predicates" in {
    parser.parse("abc = 10") should be (
      List(EqPredicate("abc", NumberLiteral("10")))
    )
  }

  it should "parse range predicates" in {
    parser.parse("abc > 10") should be (List(RangePredicate("abc", GreaterThan, NumberLiteral("10"))))
    parser.parse("abc < 10") should be (List(RangePredicate("abc", LowerThan, NumberLiteral("10"))))
    parser.parse("abc >= 10") should be (List(RangePredicate("abc", GreaterEqual, NumberLiteral("10"))))
    parser.parse("abc <= 10") should be (List(RangePredicate("abc", LowerEqual, NumberLiteral("10"))))
  }

  it should "parse IN predicates" in {
    parser.parse("cde in (20, 30)") should be (
      List(InListPredicate("cde", ValueList(NumberLiteral("20"), NumberLiteral("30"))))
    )
  }

  it should "parse quoted names" in {
    parser.parse(" \"Cde\" in  (20, 30)")(0).asInstanceOf[SingleColumnPredicate].columnName should be ("Cde")
  }

  it should "return lowercase names" in {
    parser.parse(" Cde in  (20, 30)")(0).asInstanceOf[SingleColumnPredicate].columnName should be ("cde")
  }

  it should "parse strings" in {
    inside (parser.parse(" Cde in  ('20', '30')")(0)) {
      case InListPredicate(name, ValueList(v1, v2)) =>
        v1 should be (StringLiteral("20"))
        v2 should be (StringLiteral("30"))
    }
  }

  it should "distinguish '?' from ?" in {
    inside (parser.parse(" Cde in  ('?', ?, '?', ?)", Seq(1,2))(0)) {
      case InListPredicate(name, list) =>
        list.values(0) should be (StringLiteral("?"))
        list.values(1) should be (Placeholder(1))
        list.values(2) should be (StringLiteral("?"))
        list.values(3) should be (Placeholder(2))
    }
  }

  it should " accept >= " in {
    parser.parse("abc >= ?", Seq(1)).length should be(1)
  }

  it should " accept ?" in {
    inside (parser.parse("abc in  ?", Seq(1))(0)) {
      case InPredicate(name, _) => name should be("abc")
    }
  }

  it should " accept name with quotes and other special symbols" in {
    parser.parse("\"\"\"ab!@#c\" in ?", Seq(1))(0).asInstanceOf[SingleColumnPredicate].columnName should be("\"ab!@#c")
  }

  it should " accept param with quotes and other special symbols" in {
    inside (parser.parse("abc > 'abc''abc$%^'")(0)) {
      case RangePredicate(name, op, StringLiteral(v)) => v should be("abc'abc$%^")
    }
  }

  it should " accept uuid param" in {
    inside (parser.parse("abc > 01234567-0123-0123-0123-0123456789ab")(0)) {
      case RangePredicate(name,  op, UUIDLiteral(v)) => v should be(UUID.fromString("01234567-0123-0123-0123-0123456789ab"))
    }
  }
  it should " accept float param" in {
    inside (parser.parse("abc >-12.e+10")(0)) {
      case RangePredicate(name, op, NumberLiteral(v)) => v should be("-12.e+10")
    }
  }

  it should "parse case insensitive 'aNd' operations" in {
    parser.parse("abc > 10 aNd cde > 20 AND c < 1") should be (
      List(
        RangePredicate("abc", GreaterThan, NumberLiteral("10")),
        RangePredicate("cde", GreaterThan, NumberLiteral("20")),
        RangePredicate("c", LowerThan, NumberLiteral("1"))
      )
    )
  }
  it should "parse case insensitive 'iN' operations" in {
    parser.parse("cde iN  (20, 30)") should be (
      List(InListPredicate("cde", ValueList(NumberLiteral("20"), NumberLiteral("30"))))
    )
  }

  it should "parse case insensitive 'IN' operations ?" in {
    parser.parse("cde IN  ?", Seq(1)) should be (
      List(InPredicate("cde", 1))
    )
  }

}
