/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import java.util.Optional;

import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Clock;

public class InformixChangeEventSourceFactory implements ChangeEventSourceFactory<InformixPartition, InformixOffsetContext> {

    private final InformixConnectorConfig configuration;
    private final MainConnectionProvidingConnectionFactory<InformixConnection> connectionFactory;
    private final MainConnectionProvidingConnectionFactory<InformixConnection> cdcConnectionFactory;
    private final ErrorHandler errorHandler;
    private final EventDispatcher<InformixPartition, TableId> dispatcher;
    private final Clock clock;
    private final InformixDatabaseSchema schema;
    private final SnapshotterService snapshotterService;

    public InformixChangeEventSourceFactory(InformixConnectorConfig configuration,
                                            MainConnectionProvidingConnectionFactory<InformixConnection> connectionFactory,
                                            MainConnectionProvidingConnectionFactory<InformixConnection> cdcConnectionFactory,
                                            ErrorHandler errorHandler, EventDispatcher<InformixPartition, TableId> dispatcher,
                                            Clock clock, InformixDatabaseSchema schema, SnapshotterService snapshotterService) {
        this.configuration = configuration;
        this.connectionFactory = connectionFactory;
        this.cdcConnectionFactory = cdcConnectionFactory;
        this.errorHandler = errorHandler;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.schema = schema;
        this.snapshotterService = snapshotterService;
    }

    @Override
    public SnapshotChangeEventSource<InformixPartition, InformixOffsetContext> getSnapshotChangeEventSource(SnapshotProgressListener<InformixPartition> snapshotProgressListener,
                                                                                                            NotificationService<InformixPartition, InformixOffsetContext> notificationService) {
        return new InformixSnapshotChangeEventSource(
                configuration,
                connectionFactory,
                schema,
                dispatcher,
                clock,
                snapshotProgressListener,
                notificationService,
                snapshotterService);
    }

    @Override
    public StreamingChangeEventSource<InformixPartition, InformixOffsetContext> getStreamingChangeEventSource() {
        return new InformixStreamingChangeEventSource(
                configuration,
                cdcConnectionFactory.mainConnection(),
                connectionFactory.mainConnection(),
                dispatcher,
                errorHandler,
                clock,
                schema);
    }

    @Override
    public Optional<IncrementalSnapshotChangeEventSource<InformixPartition, ? extends DataCollectionId>> getIncrementalSnapshotChangeEventSource(
                                                                                                                                                 InformixOffsetContext offsetContext,
                                                                                                                                                 SnapshotProgressListener<InformixPartition> snapshotProgressListener,
                                                                                                                                                 DataChangeEventListener<InformixPartition> dataChangeEventListener,
                                                                                                                                                 NotificationService<InformixPartition, InformixOffsetContext> notificationService) {

        // If no signal data collection is configured, incremental snapshots are not supported
        if (configuration.getSignalingDataCollectionId() == null) {
            return Optional.empty();
        }

        // Create secondary database connection if configured
        InformixConnection snapshotConnection = createSnapshotConnection();

        return Optional.of(new InformixSignalBasedIncrementalSnapshotChangeEventSource(
                configuration,
                connectionFactory.mainConnection(), // Primary connection for signals/status
                snapshotConnection, // Snapshot connection for data reads (may be same as primary)
                dispatcher, schema, clock,
                snapshotProgressListener,
                dataChangeEventListener,
                notificationService));
    }

    private InformixConnection createSnapshotConnection() {
        return configuration.getSnapshotDatabaseConfig()
                .map(InformixConnection::new)
                .orElse(connectionFactory.mainConnection());
    }
}
