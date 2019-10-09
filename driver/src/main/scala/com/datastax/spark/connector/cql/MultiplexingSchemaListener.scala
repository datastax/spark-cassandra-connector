/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.spark.connector.cql

import com.datastax.oss.driver.api.core.`type`.UserDefinedType
import com.datastax.oss.driver.api.core.metadata.schema.{AggregateMetadata, FunctionMetadata, KeyspaceMetadata, SchemaChangeListener, TableMetadata, ViewMetadata}

import scala.collection.concurrent.TrieMap

/**
  * Uses an internal TrieMap to collect incoming listeners based on their hashCodes. This means that identical
  * objects cannot be added, if you need to add the same SchemaListener more than once be sure that it is not
  * the identical object.
  */
class MultiplexingSchemaListener() extends SchemaChangeListener{

  private val listeners : TrieMap[Int, SchemaChangeListener] = new TrieMap[Int, SchemaChangeListener]()

  /**
    * Adds a listener. If another listener already exists with the same hashcode it will
    * be overridden by this listener.
    */
  def addListener(listener: SchemaChangeListener): Option[SchemaChangeListener] = {
    listeners.put(listener.hashCode(), listener)
  }

  /**
    * Removes a listener with the same hashcode as the argument listener if it exists
    */
  def removeListener(listener: SchemaChangeListener): Option[SchemaChangeListener] = {
    listeners.remove(listener.hashCode())
  }

  /**
    * Removes all registered listeners
    */
  def clearListeners(): Unit = {
    listeners.clear()
  }

  /**
    * Returns the current number of registered listeners
    */
  def registeredListenerCount(): Int = {
    listeners.size
  }

  /**
    * Returns a read only snapshot representation of current registered listeners. This is thread-safe
    * but may contain references to items which were already removed or be missing elements that were just
    * added.
    */
  def getListeners(): Iterable[SchemaChangeListener] =
    listeners.readOnlySnapshot().values

  override def onKeyspaceCreated(keyspace: KeyspaceMetadata): Unit =
    getListeners.foreach(_.onKeyspaceCreated(keyspace))

  override def onKeyspaceDropped(keyspace: KeyspaceMetadata): Unit =
    getListeners.foreach(_.onKeyspaceDropped(keyspace))

  override def onKeyspaceUpdated(current: KeyspaceMetadata, previous: KeyspaceMetadata): Unit =
    getListeners.foreach(_.onKeyspaceUpdated(current, previous))

  override def onTableCreated(table: TableMetadata): Unit =
    getListeners.foreach(_.onTableCreated(table))

  override def onTableDropped(table: TableMetadata): Unit =
    getListeners.foreach(_.onTableDropped(table))

  override def onTableUpdated(current: TableMetadata, previous: TableMetadata): Unit =
    getListeners.foreach(_.onTableUpdated(current, previous))

  override def onUserDefinedTypeCreated(`type`: UserDefinedType): Unit =
    getListeners.foreach(_.onUserDefinedTypeCreated(`type`))

  override def onUserDefinedTypeDropped(`type`: UserDefinedType): Unit =
    getListeners.foreach(_.onUserDefinedTypeDropped(`type`))

  override def onUserDefinedTypeUpdated(current: UserDefinedType, previous: UserDefinedType): Unit =
    getListeners.foreach(_.onUserDefinedTypeUpdated(current, previous))

  override def onFunctionCreated(function: FunctionMetadata): Unit =
    getListeners.foreach(_.onFunctionCreated(function))

  override def onFunctionDropped(function: FunctionMetadata): Unit =
    getListeners.foreach(_.onFunctionDropped(function))

  override def onFunctionUpdated(current: FunctionMetadata, previous: FunctionMetadata): Unit =
    getListeners.foreach(_.onFunctionUpdated(current, previous))

  override def onAggregateCreated(aggregate: AggregateMetadata): Unit =
    getListeners.foreach(_.onAggregateCreated(aggregate))

  override def onAggregateDropped(aggregate: AggregateMetadata): Unit =
    getListeners.foreach(_.onAggregateDropped(aggregate))

  override def onAggregateUpdated(current: AggregateMetadata, previous: AggregateMetadata): Unit =
    getListeners.foreach(_.onAggregateUpdated(current, previous))

  override def onViewCreated(view: ViewMetadata): Unit =
    getListeners.foreach(_.onViewCreated(view))

  override def onViewDropped(view: ViewMetadata): Unit =
    getListeners.foreach(_.onViewDropped(view))

  override def onViewUpdated(current: ViewMetadata, previous: ViewMetadata): Unit =
    getListeners.foreach(_.onViewUpdated(current, previous))

  override def close(): Unit =
    getListeners.foreach(_.close())
}