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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.config.Configuration;
import io.debezium.connector.informix.InformixConnectorConfig.SnapshotMode;
import io.debezium.connector.informix.util.TestHelper;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.junit.ConditionalFail;
import io.debezium.junit.Flaky;

/**
 * Integration test for the user-facing history topic of the Debezium Informix Server connector.
 * <p>
 * The tests should verify the {@code CREATE} schema events from snapshot and the {@code CREATE} and
 * the {@code ALTER} schema events from streaming
 *
 */
public class SchemaHistoryTopicIT extends AbstractAsyncEngineConnectorTest {

    @Rule
    public TestRule conditionalFail = new ConditionalFail();

    private InformixConnection connection;

    @Before
    public void before() throws SQLException {
        connection = TestHelper.testConnection();
        connection.execute(
                "DROP TABLE IF EXISTS tablea",
                "DROP TABLE IF EXISTS tableb",
                "DROP TABLE IF EXISTS tablec",
                "CREATE TABLE tablea (id int not null, cola varchar(30), primary key(id))",
                "CREATE TABLE tableb (id int not null, colb varchar(30), primary key(id))",
                "CREATE TABLE tablec (id int not null, colc varchar(30), primary key(id))");

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
                            "DROP TABLE tablea",
                            "DROP TABLE tableb",
                            "DROP TABLE tablec")
                    .close();
        }
    }

    @Test
    @FixFor("DBZ-1904")
    @Flaky("DBZ-7556")
    public void snapshotSchemaChanges() throws Exception {
        final int RECORDS_PER_TABLE = 5;
        final int TABLES = 2;
        final int ID_START_1 = 10;
        final Configuration config = TestHelper.defaultConfig()
                .with(InformixConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(InformixConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .build();

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START_1 + i;
            connection.execute("INSERT INTO tablea VALUES(" + id + ", 'a')");
            connection.execute("INSERT INTO tableb VALUES(" + id + ", 'b')");
        }

        start(InformixConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
        waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        waitForAvailableRecords(10, TimeUnit.SECONDS);

        // DDL for 3 tables
        SourceRecords records = consumeRecordsByTopic(3);
        final List<SourceRecord> schemaRecords = records.allRecordsInOrder();
        assertThat(schemaRecords).hasSize(3);
        schemaRecords.forEach(record -> {
            assertThat(record.topic()).isEqualTo(TestHelper.TEST_DATABASE);
            assertThat(((Struct) record.key()).getString("databaseName")).isEqualTo(TestHelper.TEST_DATABASE);
            assertThat(record.sourceOffset().get("snapshot")).isEqualTo(true);
        });
        assertThat(((Struct) schemaRecords.get(0).value()).getStruct("source").getString("snapshot")).isEqualTo("true");
        assertThat(((Struct) schemaRecords.get(1).value()).getStruct("source").getString("snapshot")).isEqualTo("true");
        assertThat(((Struct) schemaRecords.get(2).value()).getStruct("source").getString("snapshot")).isEqualTo("true");

        final List<Struct> tableCanges = ((Struct) schemaRecords.get(0).value()).getArray("tableChanges");
        assertThat(tableCanges).hasSize(1);
        assertThat(tableCanges.get(0).get("type")).isEqualTo("CREATE");

        records = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);
        assertThat(records.recordsForTopic("testdb.informix.tablea")).hasSize(RECORDS_PER_TABLE);
        assertThat(records.recordsForTopic("testdb.informix.tableb")).hasSize(RECORDS_PER_TABLE);
        records.recordsForTopic("testdb.informix.tableb").forEach(record -> assertSchemaMatchesStruct(
                (Struct) ((Struct) record.value()).get("after"),
                SchemaBuilder.struct()
                        .optional()
                        .name("testdb.informix.tableb.Value")
                        .field("id", Schema.INT32_SCHEMA)
                        .field("colb", Schema.OPTIONAL_STRING_SCHEMA)
                        .build()));
    }
}
