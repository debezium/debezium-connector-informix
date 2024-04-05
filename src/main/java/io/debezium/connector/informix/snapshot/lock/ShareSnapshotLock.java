/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix.snapshot.lock;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import io.debezium.annotation.ConnectorSpecific;
import io.debezium.connector.informix.InformixConnector;
import io.debezium.connector.informix.InformixConnectorConfig;
import io.debezium.snapshot.spi.SnapshotLock;

@ConnectorSpecific(connector = InformixConnector.class)
public class ShareSnapshotLock implements SnapshotLock {

    @Override
    public String name() {
        return InformixConnectorConfig.SnapshotLockingMode.SHARE.getValue();
    }

    @Override
    public void configure(Map<String, ?> properties) {

    }

    @Override
    public Optional<String> tableLockingStatement(Duration lockTimeout, String tableId) {

        return Optional.of(String.format("LOCK TABLE %s IN SHARE MODE", tableId));
    }
}
