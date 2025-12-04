/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import java.sql.SQLException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

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

    private String testMethodName;

    @BeforeAll
    public static void beforeClass() throws SQLException {
        AbstractInformixDatatypesTest.beforeClass();
        createTables();
    }

    @BeforeEach
    public void before(TestInfo testInfo) throws Exception {
        testMethodName = testInfo.getTestMethod().get().getName();
        init(TemporalPrecisionMode.ADAPTIVE);
    }

    @Override
    protected void init(TemporalPrecisionMode temporalPrecisionMode) throws Exception {
        initializeConnectorTestFramework();
        Testing.Debug.enable();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);

        Configuration config = connectorConfig()
                .with(InformixConnectorConfig.TIME_PRECISION_MODE, temporalPrecisionMode)
                .build();

        start(InformixConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
    }

    protected Builder connectorConfig() {
        return TestHelper.defaultConfig()
                .with(InformixConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(InformixConnectorConfig.TABLE_INCLUDE_LIST, getTableIncludeList());
    }

    private String getTableIncludeList() {
        switch (testMethodName) {
            case "stringTypes":
                return "testdb.informix.type_string";
            case "fpTypes":
            case "fpTypesAsString":
            case "fpTypesAsDouble":
                return "testdb.informix.type_fp";
            case "intTypes":
                return "testdb.informix.type_int";
            case "timeTypes":
            case "timeTypesAsAdaptiveMicroseconds":
            case "timeTypesAsConnect":
                return "testdb.informix.type_time";
            case "clobTypes":
                return "testdb.informix.type_clob";
            default:
                throw new IllegalArgumentException("Unexpected test method: " + testMethodName);
        }
    }

    @Override
    protected boolean insertRecordsDuringTest() {
        return true;
    }
}
