package com.datastax.spark.connector.util

import org.apache.spark.Logging

import scala.util.parsing.combinator.RegexParsers

object CqlWhereParser extends RegexParsers with Logging {

  def identifier = """[_\p{L}][_\p{L}\p{Nd}]*""".r ^^ { id => Identifier(id.toLowerCase)}

  def quotedIdentifier = "\"" ~> "(\"\"|[^\"])*".r <~ "\"" ^^ { id => Identifier(id.toString)}

  def num = """-?\d+(\.\d*)?([eE][-\+]?\d+)?""".r

  def bool = "true" | "false"

  def str = """'(''|[^'])*'""".r

  def uuid = """[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}""".r

  def param: Parser[Literal] = ("?" | uuid | num | bool | str) ^^ {
    case "?" => QParam()
    case param => Param(param.toString)
  }

  def op = "<=" | ">=" | "=" | ">" | "<"

  def inParam = "(" ~> param ~ rep("," ~ param) <~ ")" ^^ {
    case param ~ list => list.foldLeft(List[Literal](param)) {
      case (params, "," ~ param) => params :+ param
    }
  }

  def expr: Parser[Predicate] = (identifier | quotedIdentifier) ~ (op | "in") ~ ( param | inParam) ^^ {
    case (id ~ "in" ~ QParam()) => new InPredicate(id.name)
    case (id ~ "in" ~ inParam) => InPredicateList(id.name, inParam.asInstanceOf[List[Literal]])
    case (id ~ "=" ~ param) => EqPredicate(id.name, param.asInstanceOf[Literal])
    case (id ~ op ~ param) => RangePredicate(id.name, Operator(op), param.asInstanceOf[Literal])
  }

  def where = expr ~ rep("and" ~ expr) ^^ {
    case expr ~ list => list.foldLeft(List[Predicate](expr)) {
      case (exprs, "and" ~ expr2) => exprs :+ expr2
    }
  }

  def predicates(s: String): Seq[Predicate] = {
    parseAll(where, s) match {
      case Success(columns, _) => columns
      case x => logError("Where predicate parsing error:" + x.toString); List()
    }
  }

}

trait Literal
case class Operator(op: String) extends Literal
case class Identifier(name: String) extends Literal
case class Param(value: Any) extends Literal
case class QParam() extends Literal

trait Predicate {
  def columnName: String
}
case class InPredicate(columnName: String) extends Predicate
case class InPredicateList(columnName: String, values: List[Literal]) extends Predicate
case class EqPredicate(columnName: String, value: Literal) extends Predicate
case class RangePredicate(columnName: String, operator: Operator, value: Literal) extends Predicate

// for completeness only
