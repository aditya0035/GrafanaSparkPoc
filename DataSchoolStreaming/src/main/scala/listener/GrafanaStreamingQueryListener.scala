package org.dataschool
package listener

import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener._

class GrafanaStreamingQueryListener extends StreamingQueryListener{
  override def onQueryStarted(event: QueryStartedEvent): Unit = {
    GrafanaStreamingQueryListenerManager.onQueryStarted(event)
  }

  override def onQueryProgress(event: QueryProgressEvent): Unit = {
    event.progress.tim
    GrafanaStreamingQueryListenerManager.onQueryProgress(event)
  }

  override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {
    GrafanaStreamingQueryListenerManager.onQueryTerminated(event)
  }
}
