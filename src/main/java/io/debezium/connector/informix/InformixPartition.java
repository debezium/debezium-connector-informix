/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.AbstractPartition;
import io.debezium.util.Collect;

public class InformixPartition extends AbstractPartition implements Partition {

    private static final String PARTITION_KEY = "databaseName";

    public InformixPartition(String databaseName) {
        super(databaseName);
    }

    @Override
    public Map<String, String> getSourcePartition() {
        return Collect.hashMapOf(PARTITION_KEY, databaseName);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final InformixPartition other = (InformixPartition) obj;
        return Objects.equals(databaseName, other.databaseName);
    }

    @Override
    public int hashCode() {
        return databaseName.hashCode();
    }

    @Override
    public String toString() {
        return "InformixPartition [sourcePartition=" + getSourcePartition() + "]";
    }

    static class Provider implements Partition.Provider<InformixPartition> {
        private final InformixConnectorConfig connectorConfig;

        Provider(InformixConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
        }

        @Override
        public Set<InformixPartition> getPartitions() {
            return Collections.singleton(new InformixPartition(connectorConfig.getLogicalName()));
        }
    }
}
