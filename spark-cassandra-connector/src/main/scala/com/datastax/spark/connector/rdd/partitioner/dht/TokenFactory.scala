package com.datastax.spark.connector.rdd.partitioner.dht

import scala.language.existentials

trait TokenFactory[V, T <: Token[V]] {
  def minToken: T
  def maxToken: T
  def totalTokenCount: BigInt
  def tokenFromString(string: String): T
  def tokenToString(token: T): String
}

object TokenFactory {

  type V = t forSome { type t }
  type T = t forSome { type t <: Token[V] }

  implicit object Murmur3TokenFactory extends TokenFactory[Long, LongToken] {
    override val minToken = LongToken(Long.MinValue)
    override val maxToken = LongToken(Long.MaxValue)
    override val totalTokenCount = BigInt(maxToken.value) - BigInt(minToken.value)
    override def tokenFromString(string: String) = LongToken(string.toLong)
    override def tokenToString(token: LongToken) = token.value.toString
  }

  implicit object RandomPartitionerTokenFactory extends TokenFactory[BigInt, BigIntToken] {
    override val minToken = BigIntToken(-1)
    override val maxToken = BigIntToken(BigInt(2).pow(127))
    override val totalTokenCount = maxToken.value - minToken.value
    override def tokenFromString(string: String) = BigIntToken(BigInt(string))
    override def tokenToString(token: BigIntToken) = token.value.toString()
  }

  def forCassandraPartitioner(partitionerClassName: String): TokenFactory[V, T] = {
    val partitioner =
      partitionerClassName match {
        case "org.apache.cassandra.dht.Murmur3Partitioner" => Murmur3TokenFactory
        case "org.apache.cassandra.dht.RandomPartitioner" => RandomPartitionerTokenFactory
        case _ => throw new IllegalArgumentException(s"Unsupported partitioner: $partitionerClassName")
      }
    partitioner.asInstanceOf[TokenFactory[V, T]]
  }
}




