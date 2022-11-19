package org.dataschool
package listener

import org.apache.spark.sql.streaming.StreamingQueryListener._

object GrafanaStreamingQueryListenerManager {
  var listeners: Map[String, GrafanaStreamListener] = Map()

  def register(listener: GrafanaStreamListener): String = {
    this.synchronized {
      val uuid = java.util.UUID.randomUUID().toString
      listeners = listeners + (uuid -> listener)
      uuid
    }
  }

  def unregister(uuid: String): Unit = {
    this.synchronized {
      listeners = listeners - uuid
    }
  }

  def onQueryStarted(event: QueryStartedEvent): Unit = {
    for {(_, listener) <- listeners} listener.onQueryStarted(event)
  }

  def onQueryTerminated(event: QueryTerminatedEvent): Unit = {
    for {(_, listener) <- listeners} listener.onQueryTerminated(event)
  }

  def onQueryProgress(event: QueryProgressEvent): Unit = {
    for {(_, listener) <- listeners} listener.onQueryProgress(event)
  }
}
