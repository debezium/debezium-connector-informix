/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import io.debezium.config.Configuration.Builder;
import io.debezium.connector.informix.InformixConnectorConfig.SnapshotMode;
import io.debezium.connector.informix.util.TestHelper;

/**
 * Integration test to verify different Informix datatypes as captured during streaming.
 *
 * @author Jiri Pechanec, Lars M Johansson
 */
public class StreamingDatatypesIT extends AbstractInformixDatatypesTest {

    @Override
    protected Builder connectorConfig() {
        return TestHelper.defaultConfig()
                .with(InformixConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA);
    }

    @Override
    protected boolean insertRecordsDuringTest() {
        return true;
    }
}