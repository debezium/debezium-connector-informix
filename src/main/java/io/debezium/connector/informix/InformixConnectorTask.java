/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.bean.StandardBeanNames;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.document.DocumentReader;
import io.debezium.jdbc.DefaultMainConnectionProvidingConnectionFactory;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.SignalProcessor;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.Clock;

/**
 * The main task executing streaming from Informix.
 * Responsible for lifecycle management the streaming code.
 *
 * @author Jiri Pechanec, Laoflch Luo, Lars M Johansson
 *
 */
public class InformixConnectorTask extends BaseSourceTask<InformixPartition, InformixOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(InformixConnectorTask.class);

    private static final String CONTEXT_NAME = "informix-server-connector-task";

    private volatile InformixTaskContext taskContext;
    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile InformixConnection dataConnection;
    private volatile InformixConnection cdcConnection;
    private volatile ErrorHandler errorHandler;
    private volatile InformixDatabaseSchema schema;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    protected ChangeEventSourceCoordinator<InformixPartition, InformixOffsetContext> start(Configuration config) {
        final InformixConnectorConfig connectorConfig = new InformixConnectorConfig(config);
        final TopicNamingStrategy<TableId> topicNamingStrategy = connectorConfig.getTopicNamingStrategy(CommonConnectorConfig.TOPIC_NAMING_STRATEGY);
        final SchemaNameAdjuster schemaNameAdjuster = connectorConfig.schemaNameAdjuster();

        MainConnectionProvidingConnectionFactory<InformixConnection> connectionFactory = new DefaultMainConnectionProvidingConnectionFactory<>(
                () -> new InformixConnection(connectorConfig.getJdbcConfig()));
        MainConnectionProvidingConnectionFactory<InformixConnection> cdcConnectionFactory = new DefaultMainConnectionProvidingConnectionFactory<>(
                () -> new InformixConnection(connectorConfig.getCdcJdbcConfig()));
        dataConnection = connectionFactory.mainConnection();
        cdcConnection = cdcConnectionFactory.mainConnection();
        try {
            dataConnection.setAutoCommit(false);
        }
        catch (SQLException e) {
            throw new ConnectException(e);
        }

        final InformixValueConverters valueConverters = new InformixValueConverters(connectorConfig.getDecimalMode(), connectorConfig.getTemporalPrecisionMode(),
                connectorConfig.binaryHandlingMode());
        schema = new InformixDatabaseSchema(connectorConfig, topicNamingStrategy, valueConverters, schemaNameAdjuster, dataConnection);
        schema.initializeStorage();

        Offsets<InformixPartition, InformixOffsetContext> previousOffsets = getPreviousOffsets(new InformixPartition.Provider(connectorConfig),
                new InformixOffsetContext.Loader(connectorConfig));

        // Manual Bean Registration
        connectorConfig.getBeanRegistry().add(StandardBeanNames.CONFIGURATION, config);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.CONNECTOR_CONFIG, connectorConfig);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.DATABASE_SCHEMA, schema);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.JDBC_CONNECTION, dataConnection);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.VALUE_CONVERTER, valueConverters);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.OFFSETS, previousOffsets);

        // Service providers
        registerServiceProviders(connectorConfig.getServiceRegistry());

        final InformixPartition partition = previousOffsets.getTheOnlyPartition();
        final InformixOffsetContext previousOffset = previousOffsets.getTheOnlyOffset();

        final SnapshotterService snapshotterService = connectorConfig.getServiceRegistry().tryGetService(SnapshotterService.class);

        validateAndLoadSchemaHistory(connectorConfig, dataConnection::validateLogPosition, previousOffsets, schema,
                snapshotterService.getSnapshotter());

        taskContext = new InformixTaskContext(connectorConfig, schema);

        final Clock clock = Clock.system();

        // Set up the task record queue ...
        this.queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .pollInterval(connectorConfig.getPollInterval())
                .maxBatchSize(connectorConfig.getMaxBatchSize())
                .maxQueueSize(connectorConfig.getMaxQueueSize())
                .loggingContextSupplier(() -> taskContext.configureLoggingContext(CONTEXT_NAME))
                .build();

        errorHandler = new InformixErrorHandler(connectorConfig, queue, errorHandler);

        final InformixEventMetadataProvider metadataProvider = new InformixEventMetadataProvider();

        final SignalProcessor<InformixPartition, InformixOffsetContext> signalProcessor = new SignalProcessor<>(
                InformixConnector.class,
                connectorConfig,
                Map.of(),
                getAvailableSignalChannels(),
                DocumentReader.defaultReader(),
                previousOffsets);

        final EventDispatcher<InformixPartition, TableId> dispatcher = new EventDispatcher<>(
                connectorConfig,
                topicNamingStrategy,
                schema,
                queue,
                connectorConfig.getTableFilters().dataCollectionFilter(),
                DataChangeEvent::new,
                null,
                connectorConfig.createHeartbeat(topicNamingStrategy, schemaNameAdjuster, null, null),
                schemaNameAdjuster,
                new InformixTransactionMonitor(
                        connectorConfig,
                        metadataProvider,
                        schemaNameAdjuster,
                        (record) -> {
                            queue.enqueue(new DataChangeEvent(record));
                        },
                        topicNamingStrategy.transactionTopic()),
                signalProcessor);

        final NotificationService<InformixPartition, InformixOffsetContext> notificationService = new NotificationService<>(
                getNotificationChannels(),
                connectorConfig,
                InformixSchemaFactory.get(),
                dispatcher::enqueueNotification);

        final ChangeEventSourceCoordinator<InformixPartition, InformixOffsetContext> coordinator = new ChangeEventSourceCoordinator<>(
                previousOffsets,
                errorHandler,
                InformixConnector.class,
                connectorConfig,
                new InformixChangeEventSourceFactory(connectorConfig, connectionFactory, cdcConnectionFactory, errorHandler, dispatcher, clock, schema,
                        snapshotterService),
                new DefaultChangeEventSourceMetricsFactory<>(),
                dispatcher, schema,
                signalProcessor,
                notificationService,
                snapshotterService);

        coordinator.start(taskContext, this.queue, metadataProvider);

        return coordinator;
    }

    @Override
    protected List<SourceRecord> doPoll() throws InterruptedException {

        return queue.poll().stream()
                .map(DataChangeEvent::getRecord)
                .collect(Collectors.toList());
    }

    @Override
    protected void doStop() {
        try {
            if (dataConnection != null) {
                // Informix may have an active in-progress transaction associated with the connection and if so,
                // it will throw an exception during shutdown because the active transaction exists. This
                // is meant to help avoid this by rolling back the current active transaction, if exists.
                if (dataConnection.isConnected()) {
                    try {
                        dataConnection.rollback();
                    }
                    catch (SQLException e) {
                        // ignore
                    }
                }
                dataConnection.close();
            }
        }
        catch (SQLException e) {
            LOGGER.error("Exception while closing JDBC connection", e);
        }

        try {
            if (cdcConnection != null) {
                cdcConnection.close();
            }
        }
        catch (SQLException e) {
            LOGGER.error("Exception while closing CDC JDBC connection", e);
        }

        if (schema != null) {
            schema.close();
        }
    }

    @Override
    protected Iterable<Field> getAllConfigurationFields() {
        return InformixConnectorConfig.ALL_FIELDS;
    }
}
