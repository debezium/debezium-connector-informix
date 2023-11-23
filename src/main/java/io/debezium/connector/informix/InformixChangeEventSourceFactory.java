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
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
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

    public InformixChangeEventSourceFactory(InformixConnectorConfig configuration,
                                            MainConnectionProvidingConnectionFactory<InformixConnection> connectionFactory,
                                            MainConnectionProvidingConnectionFactory<InformixConnection> cdcConnectionFactory,
                                            ErrorHandler errorHandler, EventDispatcher<InformixPartition, TableId> dispatcher,
                                            Clock clock, InformixDatabaseSchema schema) {
        this.configuration = configuration;
        this.connectionFactory = connectionFactory;
        this.cdcConnectionFactory = cdcConnectionFactory;
        this.errorHandler = errorHandler;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.schema = schema;
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
                notificationService);
    }

    @Override
    public StreamingChangeEventSource<InformixPartition, InformixOffsetContext> getStreamingChangeEventSource() {
        return new InformixStreamingChangeEventSource(
                configuration,
                cdcConnectionFactory.mainConnection(),
                cdcConnectionFactory.newConnection(),
                dispatcher,
                errorHandler,
                clock,
                schema);
    }

    @Override
    public Optional<IncrementalSnapshotChangeEventSource<InformixPartition, ? extends DataCollectionId>> getIncrementalSnapshotChangeEventSource(InformixOffsetContext offsetContext,
                                                                                                                                                 SnapshotProgressListener<InformixPartition> snapshotProgressListener,
                                                                                                                                                 DataChangeEventListener<InformixPartition> dataChangeEventListener,
                                                                                                                                                 NotificationService<InformixPartition, InformixOffsetContext> notificationService) {

        // If no data collection id is provided, don't return an instance as the implementation requires
        // that a signal data collection id be provided to work.
        return Optional.ofNullable(configuration.getSignalingDataCollectionId())
                .map(s -> new SignalBasedIncrementalSnapshotChangeEventSource<>(
                        configuration,
                        connectionFactory.mainConnection(),
                        dispatcher, schema, clock,
                        snapshotProgressListener,
                        dataChangeEventListener,
                        notificationService));
    }
}
