package com.datastax.spark.connector.util

import org.scalatest.{FlatSpec, Matchers}

class CqlWhereParserTest extends FlatSpec with Matchers {
  var parser = CqlWhereParser
  "CqlWhereParser" should "parse 'and' operations" in {
    parser.predicates("abc > 10 and cde > 20 and c < 1")(1).columnName should be("cde")
  }

  it should "parse in operations" in {
    parser.predicates("cde in  (20, 30)")(0).columnName should be("cde")
  }


  it should "parse quted names" in {
    parser.predicates(" \"Cde\" in  (20, 30)")(0).columnName should be("Cde")
  }

  it should "lowcase names" in {
    parser.predicates(" Cde in  (20, 30)")(0).columnName should be("cde")
  }
  it should " parse strings" in {
    parser.predicates(" Cde in  ('20', '30')")(0) match {
      case InPredicateList(name, list) => list(0).asInstanceOf[Param].value should be("'20'")
      case _ => assert(false)
    }
  }

  it should " see difference among '?' and ? " in {
    parser.predicates(" Cde in  ('?', ?, '?', ?)")(0) match {
      case InPredicateList(name, list) => {
        list(0).asInstanceOf[Param].value should be("'?'")
        list(1).asInstanceOf[QParam]
        list(2).asInstanceOf[Param].value should be("'?'")
        list(3).asInstanceOf[QParam]
      }
      case _ => assert(false)
    }
  }

  it should " return UnknownPredicate on error" in {
    parser.predicates(" Cde inasa  ('20', '30')")(0) match {
      case  UnknownPredicate(_, data) => data should be ("Cde inasa  ('20', '30')")
    }
  }

  it should " return UnknownPredicate on error in second expr" in {
    parser.predicates("a = 10 and  Cde inasa  ('20', '30')")(1) match {
      case  UnknownPredicate(_, data) => data should be ("Cde inasa  ('20', '30')")
    }
  }
  it should " accept >= " in {
    parser.predicates("abc >= ?").length should be(1)
  }

  it should " accept ?" in {
    parser.predicates("abc in  ?")(0) match {
      case InPredicate(name) => name should be("abc")
      case _ => assert(false)
    }
  }

  it should " accept name with quotes and other special symbols" in {
    parser.predicates("\"\"\"ab!@#c\" in ?")(0).columnName should be("\"\"ab!@#c")
  }

  it should " accept param with quotes and other special symbols" in {
    parser.predicates("abc > 'abc''abc$%^'")(0) match {
      case RangePredicate(name, op, Param(value)) => value should be("'abc''abc$%^'")
      case _ => assert(false)
    }
  }

  it should " accept uuid param" in {
    parser.predicates("abc > 01234567-0123-0123-0123-0123456789ab")(0) match {
      case RangePredicate(name,  op, Param(value)) => value should be("01234567-0123-0123-0123-0123456789ab")
      case _ => assert(false)
    }
  }
  it should " accept float param" in {
    parser.predicates("abc >-12.e+10")(0) match {
      case RangePredicate(name, op, Param(value)) => value should be("-12.e+10")
      case _ => assert(false)
    }
  }

  it should "parse case insensitive 'aNd' operations" in {
    parser.predicates("abc > 10 aNd cde > 20 AND c < 1")(1).columnName should be("cde")
  }

  it should "parse case insensitive 'iN' operations" in {
    parser.predicates("cde iN  (20, 30)")(0).columnName should be("cde")
  }

  it should "parse case insensitive 'IN' operations ?" in {
    parser.predicates("cde IN  ?")(0).columnName should be("cde")
  }
}
