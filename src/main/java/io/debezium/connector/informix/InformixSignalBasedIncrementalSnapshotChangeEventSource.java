/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.snapshot.incremental.DefaultChunkQueryBuilder;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.schema.DatabaseSchema;
import io.debezium.util.Clock;

public class InformixSignalBasedIncrementalSnapshotChangeEventSource
        extends SignalBasedIncrementalSnapshotChangeEventSource<InformixPartition, TableId> {

    private static final Logger LOGGER = LoggerFactory.getLogger(InformixSignalBasedIncrementalSnapshotChangeEventSource.class);

    private final InformixConnection snapshotReadConnection;

    /**
     * Creates a new incremental snapshot source
     *
     * @param config Connector configuration
     * @param primaryConnection Connection to primary database (for signals and status)
     * @param snapshotReadConnection Connection to secondary database (for snapshot queries), or null to use primary
     * @param dispatcher Event dispatcher
     * @param databaseSchema Database schema
     * @param clock Clock instance
     * @param progressListener Progress listener
     * @param dataChangeEventListener Data change event listener
     * @param notificationService Notification service
     */
    public InformixSignalBasedIncrementalSnapshotChangeEventSource(
                                                                   InformixConnectorConfig config,
                                                                   JdbcConnection primaryConnection,
                                                                   InformixConnection snapshotReadConnection,
                                                                   EventDispatcher<InformixPartition, TableId> dispatcher,
                                                                   DatabaseSchema<?> databaseSchema,
                                                                   Clock clock,
                                                                   SnapshotProgressListener<InformixPartition> progressListener,
                                                                   DataChangeEventListener<InformixPartition> dataChangeEventListener,
                                                                   NotificationService<InformixPartition, ? extends OffsetContext> notificationService) {

        super(config, primaryConnection, dispatcher, databaseSchema, clock,
                progressListener, dataChangeEventListener, notificationService);

        // Use snapshot connection if provided, otherwise fall back to primary
        this.snapshotReadConnection = snapshotReadConnection != null
                ? snapshotReadConnection
                : (InformixConnection) primaryConnection;

        // Replace the chunk query builder to use snapshot connection for chunk data reads
        // This handles:
        // Building chunk queries (buildChunkQuery)
        // Creating prepared statements (readTableChunkStatement) -> uses snapshot connection
        // Building max key query SQL (buildMaxPrimaryKeyQuery)
        //
        // Max key query SQL is built here, but executed by base class using jdbcConnection (primary).

        this.chunkQueryBuilder = new DefaultChunkQueryBuilder<>(config, this.snapshotReadConnection);

        if (snapshotReadConnection != null && snapshotReadConnection != primaryConnection) {
            LOGGER.info("Incremental snapshot configured with split database mode:");
            LOGGER.info("  - Secondary DB (chunk SELECT queries, 99% of load): {}:{}",
                    this.snapshotReadConnection.config().getHostname(),
                    this.snapshotReadConnection.config().getPort());
            LOGGER.info("  - Primary DB (signals, max key, status): {}:{}",
                    primaryConnection.config().getHostname(),
                    primaryConnection.config().getPort());
        }
        else {
            LOGGER.info("Incremental snapshot using primary database for all operations");
        }
    }

    @Override
    protected void preReadChunk(IncrementalSnapshotContext<TableId> context) {
        try {
            // Validate snapshot database connection (for chunk data reads)
            if (!snapshotReadConnection.isValid()) {
                LOGGER.debug("Reconnecting to snapshot database");
                snapshotReadConnection.connect();
            }
            // Validate primary database connection (for signal table operations)
            if (!jdbcConnection.isValid()) {
                LOGGER.debug("Reconnecting to primary database");
                jdbcConnection.connect();
            }
        }
        catch (SQLException e) {
            throw new DebeziumException("Database error while validating connections in preReadChunk", e);
        }
    }

    @Override
    protected Table readSchemaForTable(TableId tableId) throws SQLException {
        LOGGER.debug("Reading schema for table '{}' from snapshot database", tableId);

        Tables tempTables = new Tables();
        try {
            // Read schema from snapshot database
            snapshotReadConnection.readSchema(
                    tempTables,
                    null,
                    tableId.schema(),
                    Tables.TableFilter.fromPredicate(id -> id.equals(tableId)),
                    null,
                    false);
        }
        catch (SQLException e) {
            LOGGER.error("Failed to read schema for table '{}' from snapshot database", tableId, e);
            throw new DebeziumException("Schema read failed for " + tableId, e);
        }

        Table newTable = tempTables.forTable(tableId);
        if (newTable == null) {
            throw new DebeziumException("Table schema not found for " + tableId);
        }

        LOGGER.debug("Successfully read schema for table '{}' from snapshot database", tableId);
        return newTable;
    }
}
