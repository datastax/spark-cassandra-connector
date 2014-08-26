package com.datastax.spark.connector.util

import org.apache.spark.Logging

import scala.util.parsing.combinator.RegexParsers

class  CqlWhereParser (cql: String, qValues: Iterator[Any]) extends RegexParsers with Logging {

  def qValueIterator = qValues
  def cqlString = cql
  def identifier = """[_\p{L}][_\p{L}\p{Nd}]*""".r ^^ { id => Identifier(id.toLowerCase)}

  def quotedIdentifier = "\"" ~> "(\"\"|[^\"])*".r <~ "\"" ^^ { id => Identifier(id.toString)}

  def num = """-?\d+(\.\d*)?([eE][-\+]?\d+)?""".r

  def bool = "true" | "false"

  def str = """'(''|[^'])*'""".r

  def uuid = """[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}""".r

  def param: Parser[Literal] = ("?" | uuid | num | bool | str) ^^ {
    case "?" => Placeholder(qValueIterator.next)
    case param => Value(param.toString)
  }

  def op = "<=" | ">=" | "=" | ">" | "<"
  def inOp = "(?i)in".r ^^ { _=> "in"}
  def andOp = "(?i)and".r ^^ {_ => "and"}

  def inParam = "(" ~> param ~ rep("," ~ param) <~ ")" ^^ {
    case param ~ list => list.foldLeft(List[Literal](param)) {
      case (params, "," ~ param) => params :+ param
    }
  }

  def expr: Parser[Predicate] = (((identifier | quotedIdentifier) ~ (op | inOp) ~ ( param | inParam)) | ".*".r) ^^ {
    case (Identifier(name) ~ "in" ~ Placeholder(value)) => new InPredicate(name, value)
    case (Identifier(name) ~ "in" ~ inParam) => InPredicateList(name, inParam.asInstanceOf[List[Literal]])
    case (Identifier(name) ~ "=" ~ param) => EqPredicate(name, param.asInstanceOf[Literal])
    case (Identifier(name) ~ op ~ param) => RangePredicate(name, Operator(op.asInstanceOf[String]), param.asInstanceOf[Literal])
    case (unknown) => UnknownPredicate("", unknown.toString, qValueIterator.foldLeft(Seq[Any]()) {_ :+ _})
  }

  def where = expr ~ rep(andOp ~ expr) ^^ {
    case expr ~ list => list.foldLeft(List[Predicate](expr)) {
      case (exprs, "and" ~ expr2) => exprs :+ expr2
    }
  }

  def predicates(): Seq[Predicate] = {
    parseAll(where, cqlString) match {
      case Success(columns, _) => columns
      case x => logWarning("Where predicate parsing error:" + x.toString); List()
    }
  }

}
object CqlWhereParser {
  def predicates(cql: String, qValues: Seq[Any]): Seq[Predicate] = predicates(cql, qValues.iterator)
  def predicates(cql: String, qValues: Iterator[Any]): Seq[Predicate] =
    new CqlWhereParser(cql, qValues).predicates()
}


trait Literal {
  def toCqlString ():String
}
case class Operator(op: String) {
  def toCqlString ():String = op
}
case class Identifier(name: String) {
  def toCqlString ():String = name
}
case class Value(value: String) extends Literal{
  override def toCqlString ():String = value
}
case class Placeholder(value: Any) extends Literal {
  override def toCqlString ():String = "?"
}

trait Predicate {
  def columnName: String
  def toCqlString ():String
  protected def quote(name: String) = "\"" + name + "\""

}
case class InPredicate(columnName: String, value: Any) extends Predicate {
  override def toCqlString ():String = quote(columnName) + " in ?"
}
case class InPredicateList(columnName: String, values: List[Literal]) extends Predicate {
  override def toCqlString ():String = quote(columnName) + " in (" + values.map(_.toCqlString).mkString(", ") + ")"
}
case class EqPredicate(columnName: String, value: Literal) extends Predicate {
  override def toCqlString ():String = quote(columnName) + " = " + value.toCqlString()
}
case class RangePredicate(columnName: String, operator: Operator, value: Literal) extends Predicate {
  override def toCqlString ():String = quote(columnName) + " " + operator.toCqlString + " " + value.toCqlString()
}
case class UnknownPredicate(columnName: String, text: String, qValues: Seq[Any]) extends Predicate {
  override def toCqlString ():String = text
}

// for completeness only
