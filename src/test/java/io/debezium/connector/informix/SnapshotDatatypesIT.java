/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import java.sql.SQLException;

import org.junit.jupiter.api.BeforeAll;

import io.debezium.config.Configuration.Builder;
import io.debezium.connector.informix.InformixConnectorConfig.SnapshotMode;
import io.debezium.connector.informix.util.TestHelper;

/**
 * Integration test to verify different Informix datatypes as captured during initial snapshotting.
 *
 * @author Jiri Pechanec, Lars M Johansson
 */
public class SnapshotDatatypesIT extends AbstractInformixDatatypesTest {

    @BeforeAll
    public static void beforeClass() throws SQLException {
        insertStringTypes();
        insertFpTypes();
        insertIntTypes();
        insertTimeTypes();
        insertClobTypes();
    }

    @Override
    protected Builder connectorConfig() {
        return TestHelper.defaultConfig()
                .with(InformixConnectorConfig.SNAPSHOT_MODE, SnapshotMode.ALWAYS);
    }

    @Override
    protected boolean insertRecordsDuringTest() {
        return false;
    }
}