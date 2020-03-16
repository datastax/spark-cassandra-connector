package com.datastax.spark.connector.rdd.partitioner.dht

import org.scalatest.{FlatSpec, Matchers}

class RandomPartitionerTokenFactorySpec extends FlatSpec with Matchers {

  val factory = TokenFactory.RandomPartitionerTokenFactory

  "RandomPartitionerTokenFactory" should "create a token from String" in {
    factory.tokenFromString("0") shouldBe BigIntToken(0L)
    factory.tokenFromString("-1") shouldBe BigIntToken(-1L)
    factory.tokenFromString("170141183460469231731687303715884105728") shouldBe
      BigIntToken(BigInt("170141183460469231731687303715884105728"))
  }

  it should "create a String representation of a token" in {
    factory.tokenToString(BigIntToken(0)) shouldBe "0"
    factory.tokenToString(BigIntToken(-1)) shouldBe "-1"
    factory.tokenToString(factory.maxToken) shouldBe "170141183460469231731687303715884105728"
  }

  it should "calculate the distance between tokens if right > left" in {
    factory.distance(BigIntToken(0), BigIntToken(1)) shouldBe BigInt(1)
  }

  it should "calculate the distance between tokens if right <= left" in {
    factory.distance(BigIntToken(0), BigIntToken(0)) shouldBe factory.totalTokenCount
    factory.distance(factory.maxToken, factory.minToken) shouldBe BigInt(0)
  }

  it should "calculate ring fraction" in {
    factory.ringFraction(BigIntToken(0L), BigIntToken(0L)) shouldBe 1.0
    factory.ringFraction(BigIntToken(0L), factory.maxToken) shouldBe 1.0
    factory.ringFraction(factory.maxToken, factory.minToken) shouldBe 0.0
    factory.ringFraction(BigIntToken(0L), BigIntToken(factory.maxToken.value /  2)) shouldBe 0.5
  }

}
