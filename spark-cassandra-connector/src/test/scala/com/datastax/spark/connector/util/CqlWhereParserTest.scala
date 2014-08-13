package com.datastax.spark.connector.util

import org.scalatest.{Matchers, FlatSpec}

class CqlWhereParserTest extends FlatSpec with Matchers {
  var parser = CqlWhereParser
  "CqlWhereParser" should "parse 'and' operations" in {
    parser.columns( "abc > 10 and cde > 20 and c < 1" )(1).name should be ("cde")
  }

  it should "parse in operations" in {
    parser.columns( "cde in  (20, 30)" )(0).name should be ("cde")
  }


  it should "parse quted names" in {
    parser.columns( " \"Cde\" in  (20, 30)" )(0).name should be ("Cde")
  }

  it should "lowcase names" in {
    parser.columns( " Cde in  (20, 30)" )(0).name should be ("cde")
  }
  it should " parse strings" in {
    parser.columns( " Cde in  ('20', '30')" )(0).values(0) should be ("'20'")
  }

  it should " return empty list on error" in {
    parser.columns( " Cde inasa  ('20', '30')" ).length should be (0)
  }
  it should " accept >= " in {
    parser.columns( "abc >= ?" ).length should be (1)
  }

  it should " accept ?" in {
    parser.columns( "abc in  ?" )(0).values.length should be (0)
  }


  it should " accept name with quotes and other special symbols" in {
    parser.columns( "\"\"\"ab!@#c\" in ?" )(0).name should be ("\"\"ab!@#c")
  }

  it should " accept param with quotes and other special symbols" in {
    parser.columns( "abc > 'abc''abc$%^'" )(0).values(0) should be ("'abc''abc$%^'")
  }

  it should " accept uuid param" in {
    parser.columns( "abc > 01234567-0123-0123-0123-0123456789ab" )(0).values(0) should be ("01234567-0123-0123-0123-0123456789ab");
  }
  it should " accept float param" in {
    parser.columns( "abc >-12.e+10" )(0).values(0) should be ("-12.e+10")
  }
}
