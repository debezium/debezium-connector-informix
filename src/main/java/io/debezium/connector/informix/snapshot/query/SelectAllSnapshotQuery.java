/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix.snapshot.query;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.snapshot.spi.SnapshotQuery;
import io.debezium.util.Strings;

public class SelectAllSnapshotQuery implements SnapshotQuery {

    @Override
    public String name() {
        return CommonConnectorConfig.SnapshotQueryMode.SELECT_ALL.getValue();
    }

    @Override
    public void configure(Map<String, ?> properties) {

    }

    @Override
    public Optional<String> snapshotQuery(String tableId, List<String> snapshotSelectColumns) {

        String joinedColumns = !snapshotSelectColumns.isEmpty() ? Strings.join(", ", snapshotSelectColumns) : "*";

        return Optional.of(String.format("SELECT %s FROM %s", joinedColumns, tableId));
    }
}
