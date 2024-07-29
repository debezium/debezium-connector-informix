/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import java.sql.SQLException;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;

import io.debezium.config.Configuration;
import io.debezium.config.Configuration.Builder;
import io.debezium.connector.informix.util.TestHelper;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.util.Testing;

/**
 * Integration test to verify different Oracle datatypes as captured during initial snapshotting.
 *
 * @author Jiri Pechanec, Lars M Johansson
 */
public class SnapshotDatatypesIT extends AbstractInformixDatatypesTest {

    @Rule
    public TestName name = new TestName();

    @BeforeClass
    public static void beforeClass() throws SQLException {
        AbstractInformixDatatypesTest.beforeClass();
        createTables();

        insertStringTypes();
        insertFpTypes();
        insertIntTypes();
        insertTimeTypes();
        insertClobTypes();
    }

    @Before
    public void before() throws Exception {
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
                .with(InformixConnectorConfig.TABLE_INCLUDE_LIST, getTableIncludeList());
    }

    private String getTableIncludeList() {
        switch (name.getMethodName()) {
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
                throw new IllegalArgumentException("Unexpected test method: " + name.getMethodName());
        }
    }

    @Override
    protected boolean insertRecordsDuringTest() {
        return false;
    }
}
