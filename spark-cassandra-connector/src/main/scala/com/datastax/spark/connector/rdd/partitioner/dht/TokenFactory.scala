package com.datastax.spark.connector.rdd.partitioner.dht

import java.nio.ByteBuffer

import scala.language.existentials
import org.apache.cassandra.utils.{FBUtilities, MurmurHash}

trait TokenFactory[V, T <: Token[V]] {
  def minToken: T
  def maxToken: T
  def totalTokenCount: BigInt
  def fromString(string: String): T
  def toString(token: T): String
  def getToken (key: ByteBuffer): T

}

object TokenFactory {

  type V = t forSome { type t }
  type T = t forSome { type t <: Token[V] }

  implicit object Murmur3TokenFactory extends TokenFactory[Long, LongToken] {
    override val minToken = LongToken(Long.MinValue)
    override val maxToken = LongToken(Long.MaxValue)
    override val totalTokenCount = BigInt(maxToken.value) - BigInt(minToken.value)
    override def fromString(string: String) = LongToken(string.toLong)
    override def toString(token: LongToken) = token.value.toString
    override def getToken(key: ByteBuffer): LongToken = {
      if (key.remaining == 0) return minToken
      val hash: Array[Long] = new Array[Long](2)
      MurmurHash.hash3_x64_128(key, key.position, key.remaining, 0, hash)
      return new LongToken(normalize(hash(0)))
    }

    private def normalize(v: Long): Long = {
      return if (v == Long.MinValue) Long.MaxValue else v
    }
  }

  implicit object RandomPartitionerTokenFactory extends TokenFactory[BigInt, BigIntToken] {
    override val minToken = BigIntToken(-1)
    override val maxToken = BigIntToken(BigInt(2).pow(127))
    override val totalTokenCount = maxToken.value - minToken.value
    override def fromString(string: String) = BigIntToken(BigInt(string))
    override def toString(token: BigIntToken) = token.value.toString()
    override def getToken(key: ByteBuffer): BigIntToken = {
      if (key.remaining == 0) return minToken
      return new BigIntToken(FBUtilities.hashToBigInteger(key))
    }
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




