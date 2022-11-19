package org.dataschool
package listener

import org.apache.spark.sql.streaming.StreamingQueryListener._

trait GrafanaStreamListener {
  def onQueryStarted(event: QueryStartedEvent): Unit

  def onQueryProgress(event: QueryProgressEvent): Unit

  def onQueryTerminated(event: QueryTerminatedEvent): Unit
}
