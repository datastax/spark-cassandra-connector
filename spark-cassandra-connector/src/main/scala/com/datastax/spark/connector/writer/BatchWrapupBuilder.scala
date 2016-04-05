package com.datastax.spark.connector.writer

import scala.collection.JavaConverters._

trait BatchWrapupBuilder[T] extends Serializable {
  def add(t: T): Unit
  def build(): Seq[RichBoundStatement]
}

trait JavaBatchWrapupBuilder[T] extends BatchWrapupBuilder[T] {
  def add(t: T): Unit
  def buildIterable(): java.lang.Iterable[RichBoundStatement]
  override def build(): Seq[RichBoundStatement] = buildIterable().asScala.toSeq
}

trait JavaBatchWrapupBuilderFactory[T] extends Function0[BatchWrapupBuilder[T]] with Serializable {
  def get(): JavaBatchWrapupBuilder[T]
  def apply: JavaBatchWrapupBuilder[T] = get()
}
