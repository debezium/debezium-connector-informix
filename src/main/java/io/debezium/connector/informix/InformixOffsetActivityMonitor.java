/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import java.time.Duration;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.pipeline.monitor.OffsetActivityMonitor;

/**
 * An {@link OffsetActivityMonitor} that tracks state changes to the connector's offsets.
 * <p>
 * The offset change position, the combination of the commit, change, and begin log sequence
 * numbers along with the transaction id, is compared against the value captured when the
 * monitor was last consulted, and when the position has not moved, a warning is logged. The
 * combination is used rather than the commit log sequence number alone so that progress
 * within a single large transaction is not reported as stale.
 *
 * @author Chris Cranford
 */
public class InformixOffsetActivityMonitor implements OffsetActivityMonitor<InformixPartition, InformixOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(InformixOffsetActivityMonitor.class);

    private final Duration checkInterval;

    private TxLogPosition previousPosition;

    public InformixOffsetActivityMonitor(Duration checkInterval) {
        this.checkInterval = checkInterval;
    }

    @Override
    public void checkForStaleOffsets(InformixPartition partition, InformixOffsetContext offsetContext) {
        final TxLogPosition position = offsetContext.getChangePosition();

        // Check for stale state
        if (Objects.equals(previousPosition, position)) {
            LOGGER.warn("Offset position {} has not changed in at least {} milliseconds. " +
                    "This may indicate the database is idle, there are no changes for the captured tables, " +
                    "or that the connector is no longer receiving records from the CDC stream.",
                    position, checkInterval.toMillis());
        }

        // Update tracked stats
        previousPosition = position;
    }

}