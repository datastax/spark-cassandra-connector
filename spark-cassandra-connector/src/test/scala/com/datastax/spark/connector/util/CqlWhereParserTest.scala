package com.datastax.spark.connector.util

import com.datastax.spark.connector.testkit.AbstractSpec
import org.scalatest.{Matchers, FlatSpec}

import scala.util.Success
import scala.util.parsing.input.CharArrayReader

/**
 * Created by artemaliev on 12/08/14.
 */
class CqlWhereParserTest extends FlatSpec with Matchers {
  var parser = cqlWhereParser
  "CqlWhereParser" should "parse 'and' operations" in {
    parser.columns( "abc > 10 and cde > 20 and c < 1" )(1) should be ("cde")
  }

  "CqlWhereParser" should "parse in operations" in {
    parser.columns( "cde in  (20, 30)" )(0) should be ("cde")
  }


  "CqlWhereParser" should "parse quted names" in {
    parser.columns( " \"Cde\" in  (20, 30)" )(0) should be ("\"Cde\"")
  }

  "CqlWhereParser" should "lowcase names" in {
    parser.columns( " Cde in  (20, 30)" )(0) should be ("cde")
  }
  "CqlWhereParser" should " parse strings" in {
    parser.columns( " Cde in  ('20', '30')" )(0) should be ("cde")
  }

  "CqlWhereParser" should " return empty list on error" in {
    parser.columns( " Cde inasa  ('20', '30')" ).length should be (0)
  }
  "CqlWhereParser" should " accept >= " in {
    parser.columns( "group >= ?" ).length should be (1)
  }

  "CqlWhereParser" should " accept ?" in {
    parser.columns( "group in ?" ).length should be (1)
  }
}
