package com.datastax.spark.connector.bulk

import java.net.InetAddress

import org.apache.cassandra.streaming._
import org.slf4j.Logger

import scala.collection.mutable.{HashMap, MultiMap, Set, StringBuilder}

import scala.collection.JavaConverters._

class BulkProgressIndicator(log: Logger) extends StreamEventHandler {
  private val startTime: Long = System.nanoTime()
  private var lastTime: Long = System.nanoTime()

  private var totalFiles = 0L

  private val sessionsByHost = new HashMap[InetAddress, Set[SessionInfo]] with MultiMap[InetAddress, SessionInfo]

  override def onSuccess(result: StreamState): Unit = {
    // Do Nothing.
  }

  override def onFailure(t: Throwable): Unit = {
    // Do Nothing.
  }

  override def handleStreamEvent(event: StreamEvent): Unit = {
    event.eventType match {
      case StreamEvent.Type.STREAM_PREPARED =>
        val currentSessionInfo = event.asInstanceOf[StreamEvent.SessionPreparedEvent].session
        sessionsByHost.addBinding(currentSessionInfo.peer, currentSessionInfo)

        log.info(s"Session to ${currentSessionInfo.connecting.getHostAddress}.")
      case StreamEvent.Type.STREAM_COMPLETE =>
        val currentCompletionEvent = event.asInstanceOf[StreamEvent.SessionCompleteEvent]

        if (currentCompletionEvent.success) {
          log.info(s"Stream to ${currentCompletionEvent.peer.getHostAddress} successful.")
        } else {
          log.info(s"Stream to ${currentCompletionEvent.peer.getHostAddress} failed.")
        }
      case StreamEvent.Type.FILE_PROGRESS =>
        val currentProgressInfo = event.asInstanceOf[StreamEvent.ProgressEvent].progress

        val currentTime = System.nanoTime()
        val deltaTime = currentTime - lastTime

        val currentProgressBuilder = new StringBuilder()
        currentProgressBuilder.append("Stream Progress: ")

        var totalProgress = 0L
        var totalSize = 0L

        var updateTotalFiles = totalFiles == 0
        for (currentPeer <- sessionsByHost.keys) {
          currentProgressBuilder.append(s"[${currentPeer.toString}]")
          for (currentSession <- sessionsByHost.getOrElse(currentPeer, Set.empty[SessionInfo])) {
            val currentSize = currentSession.getTotalSizeToSend
            var currentProgress = 0L
            var completitionStatus = 0

            if (
              currentSession.peer == currentProgressInfo.peer &&
              currentSession.sessionIndex == currentProgressInfo.sessionIndex) {
              currentSession.updateProgress(currentProgressInfo)
            }
            for (existingProgressInfo <- currentSession.getSendingFiles.asScala) {
              if (existingProgressInfo.isCompleted)
                completitionStatus += 1

              currentProgress += existingProgressInfo.currentBytes
            }
            totalProgress += currentProgress
            totalSize += currentSize

            currentProgressBuilder.append(s"-${currentSession.sessionIndex}:$completitionStatus/${currentSession.getTotalFilesToSend} ")

            if (updateTotalFiles) {
              totalFiles += currentSession.getTotalFilesToSend
            }
          }
        }

        log.info(currentProgressBuilder.toString().trim)
      case _ => // Do Nothing.
    }
  }
}
