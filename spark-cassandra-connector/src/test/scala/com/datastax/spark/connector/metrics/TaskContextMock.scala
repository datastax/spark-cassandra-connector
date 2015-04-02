package com.datastax.spark.connector.metrics

import org.apache.spark.TaskContext
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.util.TaskCompletionListener

class TaskContextMock extends TaskContext {
  val metrics = new TaskMetrics

  override def isCompleted(): Boolean = ???

  override def addOnCompleteCallback(f: () => Unit): Unit = ???

  override def taskMetrics(): TaskMetrics = metrics

  override def isRunningLocally(): Boolean = ???

  override def isInterrupted(): Boolean = ???

  override def runningLocally(): Boolean = ???

  override def partitionId(): Int = ???

  override def addTaskCompletionListener(listener: TaskCompletionListener): TaskContext = ???

  override def addTaskCompletionListener(f: (TaskContext) => Unit): TaskContext = ???

  override def attemptId(): Long = ???

  override def stageId(): Int = ???

  override def attemptNumber(): Int = ???

  override def taskAttemptId(): Long = ???
}
