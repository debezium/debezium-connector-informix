package laoflch.debezium.connector.informix

import io.debezium.pipeline.{ErrorHandler, EventDispatcher}
import io.debezium.pipeline.source.spi.{ChangeEventSourceFactory, SnapshotChangeEventSource, SnapshotProgressListener, StreamingChangeEventSource}
import io.debezium.pipeline.spi.OffsetContext
import io.debezium.relational.TableId
import io.debezium.util.Clock

class InformixChangeEventSourceFactory(val configuration: InformixConnectorConfig,
                                       val dataConnection: InformixConnection,
                                       val metadataConnection: InformixConnection,
                                       val errorHandler: ErrorHandler,
                                       val dispatcher: EventDispatcher[TableId],
                                       val clock: Clock,
                                       val schema: InformixDatabaseSchema) extends ChangeEventSourceFactory {

  override def getSnapshotChangeEventSource(offsetContext: OffsetContext,
                                            snapshotProgressListener: SnapshotProgressListener) = {
    new InformixSnapshotChangeEventSource(configuration,
      offsetContext.asInstanceOf[InformixOffsetContext],
      dataConnection, schema, dispatcher, clock, snapshotProgressListener)
  }

  override def getStreamingChangeEventSource(offsetContext: OffsetContext) = {
    new InformixStreamingChangeEventSource(configuration,
      offsetContext.asInstanceOf[InformixOffsetContext],
      dataConnection, /*metadataConnection,*/ dispatcher, errorHandler, clock, schema)
  }
}