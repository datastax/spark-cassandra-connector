package com.datastax.spark.connector.util

import org.apache.spark.Logging
import scala.util.parsing.combinator.RegexParsers

object cqlWhereParser extends RegexParsers with Logging {
  override type Elem = Char
  def identifier : Parser[String]   = """[_\p{L}][_\p{L}\p{Nd}]*""".r ^^ { _.toLowerCase}
  def quotedIdentifier: Parser[String]   = """\"[_\p{L}][_\p{L}\p{Nd}]*\"""".r ^^ { _.toString }
  def num     = """-?\d+(\.\d*)?([eE][-\+]?\d+)?""".r ^^ { _.toString }
  def param = "?" | num | """'.*'""".r
  def op = "<=" |">=" | "=" | ">" |   "<"
  def inParam =  "(" ~ param ~ rep("," ~ param) ~ ")"
  def expr: Parser[String] = (identifier | quotedIdentifier) ~ (op | "in") ~  (param | inParam)  ^^
    {
      case id ~ "in" ~ inParam => id.toString
      case id ~ op ~ param => id.toString
    }
  def where:Parser[List[String]] =  expr ~ rep( "and" ~ expr) ^^
    {
      case expr ~ list => list.foldLeft(List[String](expr)) {
        case (list,  "and" ~ expr2 ) => list :+ expr2
      }
    }

  def columns (s: String): Seq[String] = {
    parseAll(where, s) match {
      case Success(columns, _) => columns
      case x => logError("Where predicate parsing error:" + x.toString);List()
    }
  }
}

abstract trait Statement
case class Block(statements : List[Statement]) extends Statement
case class ForLoop(variable: String, lowerBound:Int, upperBound: Int, statement:Statement) extends Statement
