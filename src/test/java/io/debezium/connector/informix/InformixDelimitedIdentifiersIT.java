/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.informix.InformixConnectorConfig.SnapshotMode;
import io.debezium.connector.informix.util.TestHelper;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;

public class InformixDelimitedIdentifiersIT extends AbstractAsyncEngineConnectorTest {

    private static final String DELIMIDENT = "DELIMIDENT";

    private InformixConnection connection;

    @Before
    public void before() throws SQLException {
        connection = new InformixConnection(TestHelper.defaultJdbcConfig().with(DELIMIDENT, 1).build());
        connection.execute(
                "DROP TABLE IF EXISTS lower_case_table",
                "DROP TABLE IF EXISTS \"UPPER_CASE_TABLE\"",
                "DROP TABLE IF EXISTS \"mixed_CASE_table\"",
                "DROP TABLE IF EXISTS \"TABLE\"",
                "CREATE TABLE lower_case_table (text varchar(255))",
                "CREATE TABLE \"UPPER_CASE_TABLE\" (\"TEXT\" varchar(255))",
                "CREATE TABLE \"mixed_CASE_table\" (one varchar(255), \"TWO\" varchar(255), \"ThReE\" varchar(255))",
                "CREATE TABLE \"TABLE\" (\"SELECT\" varchar(255))",
                "INSERT INTO lower_case_table VALUES('text')",
                "INSERT INTO \"UPPER_CASE_TABLE\" VALUES('TEXT')",
                "INSERT INTO \"mixed_CASE_table\" VALUES('one', 'TWO', 'ThReE')",
                "INSERT INTO \"TABLE\" VALUES('TEXT')");
        initializeConnectorTestFramework();
        Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
        Print.enable();
    }

    @After
    public void after() throws SQLException {
        /*
         * Since all DDL operations are forbidden during Informix CDC,
         * we have to ensure the connector is properly shut down before dropping tables.
         */
        stopConnector();
        waitForConnectorShutdown(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
        assertConnectorNotRunning();
        if (connection != null) {
            connection.rollback()
                    .execute(
                            "DROP TABLE IF EXISTS lower_case_table",
                            "DROP TABLE IF EXISTS \"UPPER_CASE_TABLE\"",
                            "DROP TABLE IF EXISTS \"mixed_CASE_table\"",
                            "DROP TABLE IF EXISTS \"TABLE\"")
                    .close();
        }
    }

    @Test
    public void testDelimitedIdentifiers() throws Exception {

        final Configuration config = TestHelper.defaultConfig()
                .with(CommonConnectorConfig.DRIVER_CONFIG_PREFIX + DELIMIDENT, 1)
                .with(InformixConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(InformixConnectorConfig.TABLE_INCLUDE_LIST, "testdb.informix.lower_case_table," +
                        "testdb.informix.UPPER_CASE_TABLE," +
                        "testdb.informix.mixed_CASE_table," +
                        "testdb.informix.TABLE")
                .build();

        start(InformixConnector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot completion
        waitForSnapshotToBeCompleted(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        SourceRecords sourceRecords = consumeRecordsByTopic(4);
        List<SourceRecord> lowerCaseTable = sourceRecords.recordsForTopic("testdb.informix.lower_case_table");
        List<SourceRecord> upperCaseTable = sourceRecords.recordsForTopic("testdb.informix.UPPER_CASE_TABLE");
        List<SourceRecord> mixedCaseTable = sourceRecords.recordsForTopic("testdb.informix.mixed_CASE_table");
        List<SourceRecord> table = sourceRecords.recordsForTopic("testdb.informix.TABLE");
        assertThat(lowerCaseTable).hasSize(1);
        assertThat(upperCaseTable).hasSize(1);
        assertThat(mixedCaseTable).hasSize(1);
        assertThat(table).hasSize(1);
        assertNoRecordsToConsume();

        // Wait for streaming to start
        waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        connection.execute(
                "INSERT INTO lower_case_table VALUES('TEXT')",
                "INSERT INTO \"UPPER_CASE_TABLE\" VALUES('text')",
                "INSERT INTO \"mixed_CASE_table\" VALUES('ONE', 'two', 'tHrEe')",
                "INSERT INTO \"TABLE\" VALUES('TEXT')");

        waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS);

        sourceRecords = consumeRecordsByTopic(4);
        lowerCaseTable = sourceRecords.recordsForTopic("testdb.informix.lower_case_table");
        upperCaseTable = sourceRecords.recordsForTopic("testdb.informix.UPPER_CASE_TABLE");
        mixedCaseTable = sourceRecords.recordsForTopic("testdb.informix.mixed_CASE_table");
        table = sourceRecords.recordsForTopic("testdb.informix.TABLE");
        assertThat(lowerCaseTable).hasSize(1);
        assertThat(upperCaseTable).hasSize(1);
        assertThat(mixedCaseTable).hasSize(1);
        assertThat(table).hasSize(1);
        assertNoRecordsToConsume();
    }
}
