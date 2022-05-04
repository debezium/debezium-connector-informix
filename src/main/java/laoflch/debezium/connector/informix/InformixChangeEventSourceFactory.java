package laoflch.debezium.connector.informix;

import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;

public class InformixChangeEventSourceFactory implements ChangeEventSourceFactory {

    private final InformixConnectorConfig configuration;
    private final InformixConnection dataConnection;
    private final InformixConnection metadataConnection;
    private final ErrorHandler errorHandler;
    private final EventDispatcher<TableId> dispatcher;
    private final Clock clock;
    private final InformixDatabaseSchema schema;

    public InformixChangeEventSourceFactory(InformixConnectorConfig configuration, InformixConnection dataConnection, InformixConnection metadataConnection,
                                       ErrorHandler errorHandler, EventDispatcher<TableId> dispatcher, Clock clock, InformixDatabaseSchema schema) {
        this.configuration = configuration;
        this.dataConnection = dataConnection;
        this.metadataConnection = metadataConnection;
        this.errorHandler = errorHandler;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.schema = schema;
    }

    @Override
    public SnapshotChangeEventSource getSnapshotChangeEventSource(OffsetContext offsetContext, SnapshotProgressListener snapshotProgressListener) {
        return new InformixSnapshotChangeEventSource(configuration, (InformixOffsetContext) offsetContext, dataConnection, schema, dispatcher, clock, snapshotProgressListener);
    }

    @Override
    public StreamingChangeEventSource getStreamingChangeEventSource(OffsetContext offsetContext) {
        return new InformixStreamingChangeEventSource(
                configuration,
                (InformixOffsetContext) offsetContext,
                dataConnection,
                dispatcher,
                errorHandler,
                clock,
                schema);
    }
}

