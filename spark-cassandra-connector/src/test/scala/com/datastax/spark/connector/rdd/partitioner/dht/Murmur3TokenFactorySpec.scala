package com.datastax.spark.connector.rdd.partitioner.dht

import org.scalatest.{FlatSpec, Matchers}

class Murmur3TokenFactorySpec extends FlatSpec with Matchers {

  val factory = TokenFactory.Murmur3TokenFactory
  
  "Murmur3TokenFactory" should "create a token from String" in {
    factory.tokenFromString("0") shouldBe LongToken(0L)
    factory.tokenFromString("-1") shouldBe LongToken(-1L)
    factory.tokenFromString(Long.MaxValue.toString) shouldBe factory.maxToken
    factory.tokenFromString(Long.MinValue.toString) shouldBe factory.minToken
  }

  it should "create a String representation of a token" in {
    factory.tokenToString(LongToken(0L)) shouldBe "0"
    factory.tokenToString(LongToken(-1L)) shouldBe "-1"
    factory.tokenToString(factory.maxToken) shouldBe Long.MaxValue.toString
    factory.tokenToString(factory.minToken) shouldBe Long.MinValue.toString
  }

  it should "calculate the distance between tokens if right > left" in {
    factory.distance(LongToken(0L), LongToken(1L)) shouldBe BigInt(1)
  }

  it should "calculate the distance between tokens if right <= left" in {
    factory.distance(LongToken(0L), LongToken(0L)) shouldBe factory.totalTokenCount
    factory.distance(factory.maxToken, factory.minToken) shouldBe BigInt(0)
  }

  it should "calculate ring fraction" in {
    factory.ringFraction(LongToken(0L), LongToken(0L)) shouldBe 1.0
    factory.ringFraction(LongToken(0L), factory.maxToken) shouldBe 0.5
    factory.ringFraction(factory.maxToken, LongToken(0L)) shouldBe 0.5
  }
}
