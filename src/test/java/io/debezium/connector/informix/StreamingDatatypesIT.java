/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import java.util.stream.Collectors;

import org.junit.Before;

import io.debezium.config.Configuration;
import io.debezium.config.Configuration.Builder;
import io.debezium.connector.informix.InformixConnectorConfig.SnapshotMode;
import io.debezium.connector.informix.util.TestHelper;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.util.Testing;

/**
 * Integration test to verify different Oracle datatypes as captured during streaming.
 *
 * @author Jiri Pechanec, Lars M Johansson
 */
public class StreamingDatatypesIT extends AbstractInformixDatatypesTest {

    @Before
    public void before() throws Exception {
        init(TemporalPrecisionMode.ADAPTIVE);
    }

    @Override
    protected void init(TemporalPrecisionMode temporalPrecisionMode) throws Exception {
        dropTables();
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);

        Configuration config = connectorConfig()
                .with(InformixConnectorConfig.TIME_PRECISION_MODE, temporalPrecisionMode)
                .build();

        createTables();

        start(InformixConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
    }

    protected Builder connectorConfig() {
        String tableIncludeList = getAllTables().stream()
                .map(t -> TestHelper.TEST_DATABASE + '.' + t)
                .collect(Collectors.joining(","));

        return TestHelper.defaultConfig()
                .with(InformixConnectorConfig.TABLE_INCLUDE_LIST, tableIncludeList)
                .with(InformixConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA);
    }

    @Override
    protected boolean insertRecordsDuringTest() {
        return true;
    }
}
