package com.datastax.spark.connector

import org.scalatest.{BeforeAndAfter, Matchers, WordSpecLike}

trait AbstractSpec extends WordSpecLike with Matchers with BeforeAndAfter

object SparkFixture {
  case class WordCount(word: String, count: Int)
}