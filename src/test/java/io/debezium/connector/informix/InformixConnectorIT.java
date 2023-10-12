/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import static io.debezium.connector.informix.util.TestHelper.TYPE_LENGTH_PARAMETER_KEY;
import static io.debezium.connector.informix.util.TestHelper.TYPE_NAME_PARAMETER_KEY;
import static io.debezium.connector.informix.util.TestHelper.TYPE_SCALE_PARAMETER_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.junit.Assert.assertNull;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.informix.InformixConnectorConfig.SnapshotMode;
import io.debezium.connector.informix.util.TestHelper;
import io.debezium.converters.CloudEventsConverterTest;
import io.debezium.converters.spi.CloudEventsMaker;
import io.debezium.data.Envelope;
import io.debezium.data.SchemaAndValueField;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.schema.DatabaseSchema;

import lombok.SneakyThrows;

/**
 * Integration test for the Debezium Informix connector.
 *
 */
public class InformixConnectorIT extends AbstractConnectorTest {

    private InformixConnection connection;

    @Before
    @SneakyThrows
    public void before() {
        connection = TestHelper.testConnection();
        connection.execute(
                "CREATE TABLE tablea (id int not null, cola varchar(30), primary key (id))",
                "CREATE TABLE tableb (id int not null, colb varchar(30), primary key (id))",
                "CREATE TABLE masked_hashed_column_table (id int not null, name varchar(255), name2 varchar(255), name3 varchar(20), primary key (id))",
                "CREATE TABLE truncated_column_table (id int not null, name varchar(20), primary key (id))",
                "CREATE TABLE truncate_table (id int not null, name varchar(20), primary key (id))",
                "CREATE TABLE dt_table (id int not null, c1 int, c2 int, c3a numeric(5,2), c3b varchar(128), f1 float(14), f2 decimal(8,4), primary key(id))")
                .execute("INSERT INTO tablea VALUES(1, 'a')");
        initializeConnectorTestFramework();
        Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
        Print.enable();
    }

    @After
    @SneakyThrows
    public void after() {
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
                            "DROP TABLE masked_hashed_column_table",
                            "DROP TABLE truncated_column_table",
                            "DROP TABLE truncate_table",
                            "DROP TABLE dt_table")
                    .close();
        }
    }

    @Test
    @SneakyThrows
    public void deleteWithoutTombstone() {
        final int RECORDS_PER_TABLE = 5;
        final int TABLES = 2;
        final int ID_START = 10;
        final Configuration config = TestHelper.defaultConfig()
                .with(InformixConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
                .with(InformixConnectorConfig.TOMBSTONES_ON_DELETE, false).build();

        start(InformixConnector.class, config);

        assertConnectorIsRunning();

        // Wait for snapshot completion
        waitForSnapshotToBeCompleted(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
        consumeRecordsByTopic(0);

        waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START + i;
            connection.execute("INSERT INTO tablea VALUES(" + id + ", 'a')");
            connection.execute("INSERT INTO tableb VALUES(" + id + ", 'b')");
        }
        consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);

        connection.execute("DELETE FROM tableB");

        waitForAvailableRecords(10, TimeUnit.SECONDS);

        final SourceRecords deleteRecords = consumeRecordsByTopic(RECORDS_PER_TABLE);
        final List<SourceRecord> deleteTableA = deleteRecords.recordsForTopic("testdb.informix.tablea");
        final List<SourceRecord> deleteTableB = deleteRecords.recordsForTopic("testdb.informix.tableb");
        assertThat(deleteTableA).isNullOrEmpty();
        assertThat(deleteTableB).hasSize(RECORDS_PER_TABLE);

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final SourceRecord deleteRecord = deleteTableB.get(i);
            VerifyRecord.isValidDelete(deleteRecord, true);

            final List<SchemaAndValueField> expectedValueBefore = Arrays.asList(
                    new SchemaAndValueField("id", Schema.INT32_SCHEMA, i + ID_START),
                    new SchemaAndValueField("colb", Schema.OPTIONAL_STRING_SCHEMA, "b"));

            final Struct deleteValue = (Struct) deleteRecord.value();
            assertRecord((Struct) deleteValue.get("before"), expectedValueBefore);
        }

    }

    @Test
    @SneakyThrows
    public void deleteWithTombstone() {
        final int RECORDS_PER_TABLE = 5;
        final int TABLES = 2;
        final int ID_START = 20;
        final Configuration config = TestHelper.defaultConfig()
                .with(InformixConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(InformixConnectorConfig.TOMBSTONES_ON_DELETE, true).build();

        start(InformixConnector.class, config);

        assertConnectorIsRunning();

        // Wait for snapshot completion
        waitForSnapshotToBeCompleted(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
        consumeRecordsByTopic(1);

        waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START + i;
            connection.execute("INSERT INTO tablea VALUES(" + id + ", 'a')");
            connection.execute("INSERT INTO tableb VALUES(" + id + ", 'b')");
        }
        consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);

        connection.execute("DELETE FROM tableB");

        waitForAvailableRecords(10, TimeUnit.SECONDS);

        final SourceRecords deleteRecords = consumeRecordsByTopic(RECORDS_PER_TABLE * 2);
        final List<SourceRecord> deleteTableA = deleteRecords.recordsForTopic("testdb.informix.tablea");
        final List<SourceRecord> deleteTableB = deleteRecords.recordsForTopic("testdb.informix.tableb");
        assertThat(deleteTableA).isNullOrEmpty();
        assertThat(deleteTableB).hasSize(RECORDS_PER_TABLE * 2);

        for (int i = 0; i < RECORDS_PER_TABLE * 2; i++) {
            final SourceRecord deleteRecord = deleteTableB.get(i);
            if (deleteRecord.value() == null) {
                VerifyRecord.isValidTombstone(deleteRecord);
            }
            else {
                VerifyRecord.isValidDelete(deleteRecord, true);
                final List<SchemaAndValueField> expectedValueBefore = Arrays.asList(
                        new SchemaAndValueField("id", Schema.INT32_SCHEMA, i / 2 + ID_START),
                        new SchemaAndValueField("colb", Schema.OPTIONAL_STRING_SCHEMA, "b"));

                final Struct deleteValue = (Struct) deleteRecord.value();
                assertRecord((Struct) deleteValue.get("before"), expectedValueBefore);
            }
        }

    }

    @Test
    @SneakyThrows
    public void testTruncateTable() {
        final int RECORDS_PER_TABLE = 5;
        final int ID_START = 30;
        final Configuration config = TestHelper.defaultConfig()
                .with(InformixConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
                .with(CommonConnectorConfig.SKIPPED_OPERATIONS, "none").build();

        start(InformixConnector.class, config);

        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
        consumeRecordsByTopic(0);

        waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START + i;
            connection.execute("INSERT INTO truncate_table VALUES(" + id + ", 'name')");
        }
        consumeRecordsByTopic(RECORDS_PER_TABLE);

        connection.execute("truncate table truncate_table");

        waitForAvailableRecords(10, TimeUnit.SECONDS);

        SourceRecords sourceRecords = consumeRecordsByTopic(1);
        List<SourceRecord> truncateTable = sourceRecords.recordsForTopic("testdb.informix.truncate_table");
        assertThat(truncateTable).isNotNull().hasSize(1);

        VerifyRecord.isValidTruncate(truncateTable.get(0));

    }

    @Test
    @SneakyThrows
    public void updatePrimaryKey() {

        final Configuration config = TestHelper.defaultConfig()
                .with(InformixConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL).build();

        start(InformixConnector.class, config);

        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
        consumeRecordsByTopic(1);

        waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        connection.execute("INSERT INTO tableb VALUES(1, 'b')");
        consumeRecordsByTopic(1);

        connection.execute("UPDATE tablea SET id=100 WHERE id=1", "UPDATE tableb SET id=100 WHERE id=1");

        waitForAvailableRecords(10, TimeUnit.SECONDS);

        final SourceRecords records = consumeRecordsByTopic(6);
        final List<SourceRecord> tableA = records.recordsForTopic("testdb.informix.tablea");
        final List<SourceRecord> tableB = records.recordsForTopic("testdb.informix.tableb");
        assertThat(tableA).hasSize(3);
        assertThat(tableB).hasSize(3);

        final List<SchemaAndValueField> expectedDeleteRowA = List.of(
                new SchemaAndValueField("id", Schema.INT32_SCHEMA, 1),
                new SchemaAndValueField("cola", Schema.OPTIONAL_STRING_SCHEMA, "a"));
        final List<SchemaAndValueField> expectedDeleteKeyA = List.of(
                new SchemaAndValueField("id", Schema.INT32_SCHEMA, 1));
        final List<SchemaAndValueField> expectedInsertRowA = List.of(
                new SchemaAndValueField("id", Schema.INT32_SCHEMA, 100),
                new SchemaAndValueField("cola", Schema.OPTIONAL_STRING_SCHEMA, "a"));
        final List<SchemaAndValueField> expectedInsertKeyA = List.of(
                new SchemaAndValueField("id", Schema.INT32_SCHEMA, 100));

        final SourceRecord deleteRecordA = tableA.get(0);
        final SourceRecord tombstoneRecordA = tableA.get(1);
        final SourceRecord insertRecordA = tableA.get(2);

        final Struct deleteKeyA = (Struct) deleteRecordA.key();
        final Struct deleteValueA = (Struct) deleteRecordA.value();
        assertRecord(deleteValueA.getStruct("before"), expectedDeleteRowA);
        assertRecord(deleteKeyA, expectedDeleteKeyA);
        assertNull(deleteValueA.get("after"));

        final Struct tombstoneKeyA = (Struct) tombstoneRecordA.key();
        final Struct tombstoneValueA = (Struct) tombstoneRecordA.value();
        assertRecord(tombstoneKeyA, expectedDeleteKeyA);
        assertNull(tombstoneValueA);

        final Struct insertKeyA = (Struct) insertRecordA.key();
        final Struct insertValueA = (Struct) insertRecordA.value();
        assertRecord(insertValueA.getStruct("after"), expectedInsertRowA);
        assertRecord(insertKeyA, expectedInsertKeyA);
        assertNull(insertValueA.get("before"));

        final List<SchemaAndValueField> expectedDeleteRowB = List.of(
                new SchemaAndValueField("id", Schema.INT32_SCHEMA, 1),
                new SchemaAndValueField("colb", Schema.OPTIONAL_STRING_SCHEMA, "b"));
        final List<SchemaAndValueField> expectedDeleteKeyB = List.of(
                new SchemaAndValueField("id", Schema.INT32_SCHEMA, 1));
        final List<SchemaAndValueField> expectedInsertRowB = List.of(
                new SchemaAndValueField("id", Schema.INT32_SCHEMA, 100),
                new SchemaAndValueField("colb", Schema.OPTIONAL_STRING_SCHEMA, "b"));
        final List<SchemaAndValueField> expectedInsertKeyB = List.of(
                new SchemaAndValueField("id", Schema.INT32_SCHEMA, 100));

        final SourceRecord deleteRecordB = tableB.get(0);
        final SourceRecord tombstoneRecordB = tableB.get(1);
        final SourceRecord insertRecordB = tableB.get(2);

        final Struct deletekeyB = (Struct) deleteRecordB.key();
        final Struct deleteValueB = (Struct) deleteRecordB.value();
        assertRecord(deleteValueB.getStruct("before"), expectedDeleteRowB);
        assertRecord(deletekeyB, expectedDeleteKeyB);
        assertNull(deleteValueB.get("after"));

        final Struct tombstonekeyB = (Struct) tombstoneRecordB.key();
        final Struct tombstoneValueB = (Struct) tombstoneRecordB.value();
        assertRecord(tombstonekeyB, expectedDeleteKeyB);
        assertNull(tombstoneValueB);

        final Struct insertkeyB = (Struct) insertRecordB.key();
        final Struct insertValueB = (Struct) insertRecordB.value();
        assertRecord(insertValueB.getStruct("after"), expectedInsertRowB);
        assertRecord(insertkeyB, expectedInsertKeyB);
        assertNull(insertValueB.get("before"));

    }

    @Test
    @FixFor("DBZ-1152")
    @SneakyThrows
    public void updatePrimaryKeyWithRestartInMiddle() {

        final Configuration config = TestHelper.defaultConfig()
                .with(InformixConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL).build();

        start(InformixConnector.class, config);

        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
        consumeRecordsByTopic(1);

        waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        connection.execute("INSERT INTO tableb VALUES(1, 'b')");
        consumeRecordsByTopic(1);

        connection.execute("UPDATE tablea SET id=100 WHERE id=1", "UPDATE tableb SET id=100 WHERE id=1");

        waitForAvailableRecords(10, TimeUnit.SECONDS);

        final SourceRecords records1 = consumeRecordsByTopic(2);

        stopConnector();

        assertConnectorNotRunning();

        waitForConnectorShutdown(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        start(InformixConnector.class, config);

        assertConnectorIsRunning();

        waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        waitForAvailableRecords(10, TimeUnit.SECONDS);

        final SourceRecords records2 = consumeRecordsByTopic(4);

        final List<SourceRecord> tableA = records1.recordsForTopic("testdb.informix.tablea");
        tableA.addAll(records2.recordsForTopic("testdb.informix.tablea"));
        final List<SourceRecord> tableB = records2.recordsForTopic("testdb.informix.tableb");
        assertThat(tableA).hasSize(3);
        assertThat(tableB).hasSize(3);

        final List<SchemaAndValueField> expectedDeleteRowA = List.of(
                new SchemaAndValueField("id", Schema.INT32_SCHEMA, 1),
                new SchemaAndValueField("cola", Schema.OPTIONAL_STRING_SCHEMA, "a"));
        final List<SchemaAndValueField> expectedDeleteKeyA = List.of(
                new SchemaAndValueField("id", Schema.INT32_SCHEMA, 1));
        final List<SchemaAndValueField> expectedInsertRowA = List.of(
                new SchemaAndValueField("id", Schema.INT32_SCHEMA, 100),
                new SchemaAndValueField("cola", Schema.OPTIONAL_STRING_SCHEMA, "a"));
        final List<SchemaAndValueField> expectedInsertKeyA = List.of(
                new SchemaAndValueField("id", Schema.INT32_SCHEMA, 100));

        final SourceRecord deleteRecordA = tableA.get(0);
        final SourceRecord tombstoneRecordA = tableA.get(1);
        final SourceRecord insertRecordA = tableA.get(2);

        final Struct deleteKeyA = (Struct) deleteRecordA.key();
        final Struct deleteValueA = (Struct) deleteRecordA.value();
        assertRecord(deleteValueA.getStruct("before"), expectedDeleteRowA);
        assertRecord(deleteKeyA, expectedDeleteKeyA);
        assertNull(deleteValueA.get("after"));

        final Struct tombstoneKeyA = (Struct) tombstoneRecordA.key();
        final Struct tombstoneValueA = (Struct) tombstoneRecordA.value();
        assertRecord(tombstoneKeyA, expectedDeleteKeyA);
        assertNull(tombstoneValueA);

        final Struct insertKeyA = (Struct) insertRecordA.key();
        final Struct insertValueA = (Struct) insertRecordA.value();
        assertRecord(insertValueA.getStruct("after"), expectedInsertRowA);
        assertRecord(insertKeyA, expectedInsertKeyA);
        assertNull(insertValueA.get("before"));

        final List<SchemaAndValueField> expectedDeleteRowB = List.of(
                new SchemaAndValueField("id", Schema.INT32_SCHEMA, 1),
                new SchemaAndValueField("colb", Schema.OPTIONAL_STRING_SCHEMA, "b"));
        final List<SchemaAndValueField> expectedDeleteKeyB = List.of(
                new SchemaAndValueField("id", Schema.INT32_SCHEMA, 1));
        final List<SchemaAndValueField> expectedInsertRowB = List.of(
                new SchemaAndValueField("id", Schema.INT32_SCHEMA, 100),
                new SchemaAndValueField("colb", Schema.OPTIONAL_STRING_SCHEMA, "b"));
        final List<SchemaAndValueField> expectedInsertKeyB = List.of(
                new SchemaAndValueField("id", Schema.INT32_SCHEMA, 100));

        final SourceRecord deleteRecordB = tableB.get(0);
        final SourceRecord tombstoneRecordB = tableB.get(1);
        final SourceRecord insertRecordB = tableB.get(2);

        final Struct deletekeyB = (Struct) deleteRecordB.key();
        final Struct deleteValueB = (Struct) deleteRecordB.value();
        assertRecord(deleteValueB.getStruct("before"), expectedDeleteRowB);
        assertRecord(deletekeyB, expectedDeleteKeyB);
        assertNull(deleteValueB.get("after"));

        final Struct tombstonekeyB = (Struct) tombstoneRecordB.key();
        final Struct tombstoneValueB = (Struct) tombstoneRecordB.value();
        assertRecord(tombstonekeyB, expectedDeleteKeyB);
        assertNull(tombstoneValueB);

        final Struct insertkeyB = (Struct) insertRecordB.key();
        final Struct insertValueB = (Struct) insertRecordB.value();
        assertRecord(insertValueB.getStruct("after"), expectedInsertRowB);
        assertRecord(insertkeyB, expectedInsertKeyB);
        assertNull(insertValueB.get("before"));
    }

    @Test
    @FixFor("DBZ-1069")
    @SneakyThrows
    public void verifyOffsets() {
        final int RECORDS_PER_TABLE = 5;
        final int TABLES = 2;
        final int ID_START = 40;
        final int ID_RESTART = 100;
        final Configuration config = TestHelper.defaultConfig()
                .with(InformixConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL).build();

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START + i;
            connection.execute("INSERT INTO tablea VALUES(" + id + ", 'a')");
            connection.execute("INSERT INTO tableb VALUES(" + id + ", 'b')");
        }

        start(InformixConnector.class, config);

        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        List<SourceRecord> records = consumeRecordsByTopic(1 + RECORDS_PER_TABLE * TABLES).allRecordsInOrder();
        records = records.subList(1, records.size());
        for (Iterator<SourceRecord> it = records.iterator(); it.hasNext();) {
            SourceRecord record = it.next();
            assertThat(record.sourceOffset().get("snapshot")).as("Snapshot phase").isEqualTo(true);
            if (it.hasNext()) {
                assertThat(record.sourceOffset().get("snapshot_completed")).as("Snapshot in progress").isEqualTo(false);
            }
            else {
                assertThat(record.sourceOffset().get("snapshot_completed")).as("Snapshot completed").isEqualTo(true);
            }
        }

        waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        connection.execute("INSERT INTO tablea VALUES(-1, 'a')"); // TODO: Snapshot should save LSN!
        consumeRecordsByTopic(1);

        stopConnector();

        assertConnectorNotRunning();

        waitForConnectorShutdown(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_RESTART + i;
            connection.execute("INSERT INTO tablea VALUES(" + id + ", 'a')");
            connection.execute("INSERT INTO tableb VALUES(" + id + ", 'b')");
        }

        start(InformixConnector.class, config);

        assertConnectorIsRunning();

        waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        connection.execute("INSERT INTO tableb VALUES(-1, 'b')");

        waitForAvailableRecords(1, TimeUnit.MINUTES);

        final SourceRecords sourceRecords = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);
        final List<SourceRecord> tableA = sourceRecords.recordsForTopic("testdb.informix.tablea");
        final List<SourceRecord> tableB = sourceRecords.recordsForTopic("testdb.informix.tableb");

        consumeRecordsByTopic(1);

        assertThat(tableA).hasSize(RECORDS_PER_TABLE);
        assertThat(tableB).hasSize(RECORDS_PER_TABLE);

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = i + ID_RESTART;
            final SourceRecord recordA = tableA.get(i);
            final SourceRecord recordB = tableB.get(i);
            final List<SchemaAndValueField> expectedRowA = List.of(
                    new SchemaAndValueField("id", Schema.INT32_SCHEMA, id),
                    new SchemaAndValueField("cola", Schema.OPTIONAL_STRING_SCHEMA, "a"));
            final List<SchemaAndValueField> expectedRowB = List.of(
                    new SchemaAndValueField("id", Schema.INT32_SCHEMA, id),
                    new SchemaAndValueField("colb", Schema.OPTIONAL_STRING_SCHEMA, "b"));

            final Struct valueA = (Struct) recordA.value();
            assertRecord((Struct) valueA.get("after"), expectedRowA);
            assertNull(valueA.get("before"));

            final Struct valueB = (Struct) recordB.value();
            assertRecord((Struct) valueB.get("after"), expectedRowB);
            assertNull(valueB.get("before"));

            assertThat(recordA.sourceOffset().get("snapshot")).as("Streaming phase").isNull();
            assertThat(recordA.sourceOffset().get("snapshot_completed")).as("Streaming phase").isNull();
            assertThat(recordA.sourceOffset().get("change_lsn")).as("LSN present").isNotNull();

            assertThat(recordB.sourceOffset().get("snapshot")).as("Streaming phase").isNull();
            assertThat(recordB.sourceOffset().get("snapshot_completed")).as("Streaming phase").isNull();
            assertThat(recordB.sourceOffset().get("change_lsn")).as("LSN present").isNotNull();
        }
    }

    @Test
    @SneakyThrows
    public void testTableIncludeList() {
        final int RECORDS_PER_TABLE = 5;
        final int ID_START = 50;
        final Configuration config = TestHelper.defaultConfig()
                .with(InformixConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
                .with(InformixConnectorConfig.TABLE_INCLUDE_LIST, "testdb.informix.tableb").build();
        connection.execute("INSERT INTO tableb VALUES(1, 'b')");

        start(InformixConnector.class, config);

        assertConnectorIsRunning();

        // Wait for snapshot completion
        waitForSnapshotToBeCompleted(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
        consumeRecordsByTopic(0);

        waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START + i;
            connection.execute("INSERT INTO tablea VALUES(" + id + ", 'a')");
            connection.execute("INSERT INTO tableb VALUES(" + id + ", 'b')");
        }

        waitForAvailableRecords(10, TimeUnit.SECONDS);

        final SourceRecords records = consumeRecordsByTopic(RECORDS_PER_TABLE);
        final List<SourceRecord> tableA = records.recordsForTopic("testdb.informix.tablea");
        final List<SourceRecord> tableB = records.recordsForTopic("testdb.informix.tableb");
        assertThat(tableA).isNullOrEmpty();
        assertThat(tableB).hasSize(RECORDS_PER_TABLE);
    }

    @Test
    @SneakyThrows
    public void testTableExcludeList() {
        final int RECORDS_PER_TABLE = 5;
        final int ID_START = 60;
        final Configuration config = TestHelper.defaultConfig()
                .with(InformixConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(InformixConnectorConfig.TABLE_EXCLUDE_LIST, "testdb.informix.tablea").build();
        connection.execute("INSERT INTO tableb VALUES(1, 'b')");

        start(InformixConnector.class, config);

        assertConnectorIsRunning();

        // Wait for snapshot completion
        waitForSnapshotToBeCompleted(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
        consumeRecordsByTopic(1);

        waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START + i;
            connection.execute("INSERT INTO tablea VALUES(" + id + ", 'a')");
            connection.execute("INSERT INTO tableb VALUES(" + id + ", 'b')");
        }

        waitForAvailableRecords(10, TimeUnit.SECONDS);

        final SourceRecords records = consumeRecordsByTopic(RECORDS_PER_TABLE);
        final List<SourceRecord> tableA = records.recordsForTopic("testdb.informix.tablea");
        final List<SourceRecord> tableB = records.recordsForTopic("testdb.informix.tableb");
        assertThat(tableA).isNullOrEmpty();
        assertThat(tableB).hasSize(RECORDS_PER_TABLE);
    }

    @SneakyThrows
    private void restartInTheMiddleOfTx(boolean restartJustAfterSnapshot, boolean afterStreaming) {
        final int RECORDS_PER_TABLE = 30;
        final int TABLES = 2;
        final int ID_START = 200;
        final int ID_RESTART = 1000;
        final int HALF_ID = ID_START + RECORDS_PER_TABLE / 2;
        final Configuration config = TestHelper.defaultConfig()
                .with(InformixConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL).build();

        if (restartJustAfterSnapshot) {
            start(InformixConnector.class, config);

            assertConnectorIsRunning();

            // Wait for snapshot to be completed
            waitForSnapshotToBeCompleted(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
            consumeRecordsByTopic(1);

            stopConnector();

            waitForConnectorShutdown(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

            assertConnectorNotRunning();

            connection.execute("INSERT INTO tablea VALUES(-1, '-a')");
        }

        start(InformixConnector.class, config, record -> {
            if (!"testdb.informix.tablea.Envelope".equals(record.valueSchema().name())) {
                return false;
            }
            final Struct envelope = (Struct) record.value();
            final Struct after = envelope.getStruct("after");
            final Integer id = after.getInt32("id");
            final String value = after.getString("cola");
            return id != null && id == HALF_ID && "a".equals(value);
        });

        assertConnectorIsRunning();

        // Wait for snapshot to be completed or a first streaming message delivered
        if (restartJustAfterSnapshot) {
            waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
        }
        else {
            waitForSnapshotToBeCompleted(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
        }
        consumeRecordsByTopic(1);

        if (afterStreaming) {
            connection.execute("INSERT INTO tablea VALUES(-2, '-a')");
            final SourceRecords records = consumeRecordsByTopic(1);
            final List<SchemaAndValueField> expectedRow = List.of(
                    new SchemaAndValueField("id", Schema.INT32_SCHEMA, -2),
                    new SchemaAndValueField("cola", Schema.OPTIONAL_STRING_SCHEMA, "-a"));
            assertRecord(((Struct) records.allRecordsInOrder().get(0).value()).getStruct(Envelope.FieldName.AFTER), expectedRow);
        }

        connection.setAutoCommit(false);
        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START + i;
            connection.executeWithoutCommitting("INSERT INTO tablea VALUES(" + id + ", 'a')");
            connection.executeWithoutCommitting("INSERT INTO tableb VALUES(" + id + ", 'b')");
        }
        connection.commit();

        waitForAvailableRecords(10, TimeUnit.SECONDS);

        List<SourceRecord> records = consumeRecordsByTopic(RECORDS_PER_TABLE).allRecordsInOrder();
        assertThat(records).hasSize(RECORDS_PER_TABLE);

        SourceRecord lastRecordForOffset = records.get(RECORDS_PER_TABLE - 1);
        Struct value = (Struct) lastRecordForOffset.value();
        final List<SchemaAndValueField> expectedLastRow = List.of(
                new SchemaAndValueField("id", Schema.INT32_SCHEMA, HALF_ID - 1),
                new SchemaAndValueField("colb", Schema.OPTIONAL_STRING_SCHEMA, "b"));
        assertRecord((Struct) value.get("after"), expectedLastRow);

        stopConnector();

        assertConnectorNotRunning();

        waitForConnectorShutdown(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        start(InformixConnector.class, config);

        assertConnectorIsRunning();

        waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        waitForAvailableRecords(10, TimeUnit.SECONDS);

        SourceRecords sourceRecords = consumeRecordsByTopic(RECORDS_PER_TABLE);
        List<SourceRecord> tableA = sourceRecords.recordsForTopic("testdb.informix.tablea");
        List<SourceRecord> tableB = sourceRecords.recordsForTopic("testdb.informix.tableb");

        assertThat(tableA).hasSize(RECORDS_PER_TABLE / 2);
        assertThat(tableB).hasSize(RECORDS_PER_TABLE / 2);

        for (int i = 0; i < RECORDS_PER_TABLE / 2; i++) {
            final int id = HALF_ID + i;
            final SourceRecord recordA = tableA.get(i);
            final SourceRecord recordB = tableB.get(i);
            final List<SchemaAndValueField> expectedRowA = List.of(
                    new SchemaAndValueField("id", Schema.INT32_SCHEMA, id),
                    new SchemaAndValueField("cola", Schema.OPTIONAL_STRING_SCHEMA, "a"));
            final List<SchemaAndValueField> expectedRowB = List.of(
                    new SchemaAndValueField("id", Schema.INT32_SCHEMA, id),
                    new SchemaAndValueField("colb", Schema.OPTIONAL_STRING_SCHEMA, "b"));

            final Struct valueA = (Struct) recordA.value();
            assertRecord((Struct) valueA.get("after"), expectedRowA);
            assertNull(valueA.get("before"));

            final Struct valueB = (Struct) recordB.value();
            assertRecord((Struct) valueB.get("after"), expectedRowB);
            assertNull(valueB.get("before"));
        }

        connection.setAutoCommit(false);
        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_RESTART + i;
            connection.executeWithoutCommitting("INSERT INTO tablea VALUES(" + id + ", 'a')");
            connection.executeWithoutCommitting("INSERT INTO tableb VALUES(" + id + ", 'b')");
        }
        connection.commit();

        waitForAvailableRecords(10, TimeUnit.SECONDS);

        sourceRecords = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);
        tableA = sourceRecords.recordsForTopic("testdb.informix.tablea");
        tableB = sourceRecords.recordsForTopic("testdb.informix.tableb");

        assertThat(tableA).hasSize(RECORDS_PER_TABLE);
        assertThat(tableB).hasSize(RECORDS_PER_TABLE);

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = i + ID_RESTART;
            final SourceRecord recordA = tableA.get(i);
            final SourceRecord recordB = tableB.get(i);
            final List<SchemaAndValueField> expectedRowA = List.of(
                    new SchemaAndValueField("id", Schema.INT32_SCHEMA, id),
                    new SchemaAndValueField("cola", Schema.OPTIONAL_STRING_SCHEMA, "a"));
            final List<SchemaAndValueField> expectedRowB = List.of(
                    new SchemaAndValueField("id", Schema.INT32_SCHEMA, id),
                    new SchemaAndValueField("colb", Schema.OPTIONAL_STRING_SCHEMA, "b"));

            final Struct valueA = (Struct) recordA.value();
            assertRecord((Struct) valueA.get("after"), expectedRowA);
            assertNull(valueA.get("before"));

            final Struct valueB = (Struct) recordB.value();
            assertRecord((Struct) valueB.get("after"), expectedRowB);
            assertNull(valueB.get("before"));
        }

    }

    @Test
    @FixFor("DBZ-1128")
    @SneakyThrows
    public void restartInTheMiddleOfTxAfterSnapshot() {
        restartInTheMiddleOfTx(true, false);
    }

    @Test
    @FixFor("DBZ-1128")
    @SneakyThrows
    public void restartInTheMiddleOfTxAfterCompletedTx() {
        restartInTheMiddleOfTx(false, true);
    }

    @Test
    @FixFor("DBZ-1128")
    @SneakyThrows
    public void restartInTheMiddleOfTx() {
        restartInTheMiddleOfTx(false, false);
    }

    @Test
    @FixFor("DBZ-1242")
    @SneakyThrows
    public void testEmptySchemaWarningAfterApplyingFilters() {
        // This captures all logged messages, allowing us to verify log message was written.
        final LogInterceptor logInterceptor = new LogInterceptor(RelationalDatabaseSchema.class);

        Configuration config = TestHelper.defaultConfig()
                .with(InformixConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(InformixConnectorConfig.TABLE_INCLUDE_LIST, "my_products").build();

        start(InformixConnector.class, config);

        assertConnectorIsRunning();

        waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        waitForAvailableRecords(1, TimeUnit.SECONDS);

        stopConnector(value -> assertThat(logInterceptor.containsWarnMessage(DatabaseSchema.NO_CAPTURED_DATA_COLLECTIONS_WARNING)).isTrue());
    }

    @Test
    @FixFor("DBZ-775")
    @SneakyThrows
    public void shouldConsumeEventsWithMaskedAndTruncatedColumns() {
        final Configuration config = TestHelper.defaultConfig()
                .with(InformixConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
                .with("column.mask.with.12.chars", "testdb.informix.masked_hashed_column_table.name")
                .with("column.mask.hash.SHA-256.with.salt.CzQMA0cB5K",
                        "testdb.informix.masked_hashed_column_table.name2,testdb.informix.masked_hashed_column_table.name3")
                .with("column.truncate.to.4.chars", "testdb.informix.truncated_column_table.name").build();

        start(InformixConnector.class, config);

        assertConnectorIsRunning();

        // Wait for snapshot completion
        waitForSnapshotToBeCompleted(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
        consumeRecordsByTopic(0);

        waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        connection.execute("INSERT INTO masked_hashed_column_table (id, name, name2, name3) VALUES (10, 'some_name', 'test', 'test')");
        connection.execute("INSERT INTO truncated_column_table VALUES(11, 'some_name')");

        waitForAvailableRecords(1, TimeUnit.MINUTES);

        final SourceRecords records = consumeRecordsByTopic(2);
        final List<SourceRecord> tableA = records.recordsForTopic("testdb.informix.masked_hashed_column_table");
        final List<SourceRecord> tableB = records.recordsForTopic("testdb.informix.truncated_column_table");

        assertThat(tableA).hasSize(1);
        SourceRecord record = tableA.get(0);
        VerifyRecord.isValidInsert(record, "id", 10);

        Struct value = (Struct) record.value();
        if (value.getStruct("after") != null) {
            Struct after = value.getStruct("after");
            assertThat(after.getString("name")).isEqualTo("************");
            assertThat(after.getString("name2")).isEqualTo("8e68c68edbbac316dfe2f6ada6b0d2d3e2002b487a985d4b7c7c82dd83b0f4d7");
            assertThat(after.getString("name3")).isEqualTo("8e68c68edbbac316dfe2");
        }

        assertThat(tableB).hasSize(1);
        record = tableB.get(0);
        VerifyRecord.isValidInsert(record, "id", 11);

        value = (Struct) record.value();
        if (value.getStruct("after") != null) {
            assertThat(value.getStruct("after").getString("name")).isEqualTo("some");
        }
    }

    @Test
    @FixFor("DBZ-775")
    @SneakyThrows
    public void shouldRewriteIdentityKey() {
        final Configuration config = TestHelper.defaultConfig()
                .with(InformixConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
                .with(InformixConnectorConfig.MSG_KEY_COLUMNS, "(.*).tablea:id,cola").build();

        start(InformixConnector.class, config);

        assertConnectorIsRunning();

        // Wait for snapshot completion
        waitForSnapshotToBeCompleted(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
        consumeRecordsByTopic(0);

        waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        connection.execute("INSERT INTO tablea (id, cola) values (100, 'hundred')");

        waitForAvailableRecords(1, TimeUnit.MINUTES);

        List<SourceRecord> records = consumeRecordsByTopic(1).recordsForTopic("testdb.informix.tablea");
        assertThat(records).isNotNull().isNotEmpty();
        assertThat(records.get(0).key()).isNotNull();
        Struct key = (Struct) records.get(0).key();
        assertThat(key.get("id")).isNotNull();
        assertThat(key.get("cola")).isNotNull();

    }

    @Test
    @FixFor({ "DBZ-1916", "DBZ-1830" })
    @SneakyThrows
    public void shouldPropagateSourceTypeByDatatype() {
        final Configuration config = TestHelper.defaultConfig()
                .with(InformixConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
                .with("datatype.propagate.source.type", ".+\\.NUMERIC,.+\\.VARCHAR,.+\\.DECIMAL,.+\\.FLOAT").build();

        start(InformixConnector.class, config);

        assertConnectorIsRunning();

        // Wait for snapshot completion
        waitForSnapshotToBeCompleted(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
        consumeRecordsByTopic(0);

        waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        connection.execute("INSERT INTO dt_table (id,c1,c2,c3a,c3b,f1,f2) values (1,123,456,789.01,'test',1.228,234.56)");

        waitForAvailableRecords(1, TimeUnit.MINUTES);

        final SourceRecords records = consumeRecordsByTopic(1);

        List<SourceRecord> recordsForTopic = records.recordsForTopic("testdb.informix.dt_table");
        assertThat(recordsForTopic).hasSize(1);

        final Field before = recordsForTopic.get(0).valueSchema().field("before");

        assertThat(before.schema().field("id").schema().parameters()).isNull();
        assertThat(before.schema().field("c1").schema().parameters()).isNull();
        assertThat(before.schema().field("c2").schema().parameters()).isNull();

        assertThat(before.schema().field("c3a").schema().parameters())
                .contains(entry(TYPE_NAME_PARAMETER_KEY, "DECIMAL"),
                        entry(TYPE_LENGTH_PARAMETER_KEY, "5"),
                        entry(TYPE_SCALE_PARAMETER_KEY, "2"));

        assertThat(before.schema().field("c3b").schema().parameters())
                .contains(entry(TYPE_NAME_PARAMETER_KEY, "VARCHAR"),
                        entry(TYPE_LENGTH_PARAMETER_KEY, "128"));

        assertThat(before.schema().field("f1").schema().parameters())
                .contains(entry(TYPE_NAME_PARAMETER_KEY, "FLOAT"),
                        entry(TYPE_LENGTH_PARAMETER_KEY, "17"));

        assertThat(before.schema().field("f2").schema().parameters())
                .contains(entry(TYPE_NAME_PARAMETER_KEY, "DECIMAL"),
                        entry(TYPE_LENGTH_PARAMETER_KEY, "8"),
                        entry(TYPE_SCALE_PARAMETER_KEY, "4"));

    }

    @Test
    @FixFor("DBZ-3668")
    @SneakyThrows
    public void shouldOutputRecordsInCloudEventsFormat() {
        final Configuration config = TestHelper.defaultConfig()
                .with(InformixConnectorConfig.TABLE_INCLUDE_LIST, "testdb.informix.tablea").build();

        connection.execute("INSERT INTO tablea (id,cola) values (1001, 'DBZ3668')");

        start(InformixConnector.class, config);

        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        SourceRecords records = consumeRecordsByTopic(1);

        List<SourceRecord> tablea = records.recordsForTopic("testdb.informix.tablea");
        assertThat(tablea).hasSize(1);

        for (SourceRecord record : tablea) {
            CloudEventsConverterTest.shouldConvertToCloudEventsInJson(record, false);
            CloudEventsConverterTest.shouldConvertToCloudEventsInJsonWithDataAsAvro(record, false);
            CloudEventsConverterTest.shouldConvertToCloudEventsInAvro(record, "informix", TestHelper.TEST_DATABASE, false);
        }

        waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        connection.execute("INSERT INTO tablea (id,cola) VALUES (1002, 'DBZ3668')");

        waitForAvailableRecords(10, TimeUnit.SECONDS);

        records = consumeRecordsByTopic(1);

        tablea = records.recordsForTopic("testdb.informix.tablea");
        assertThat(tablea).hasSize(1);

        for (SourceRecord record : tablea) {
            CloudEventsConverterTest.shouldConvertToCloudEventsInJson(record, false, jsonNode -> {
                assertThat(jsonNode.get(CloudEventsMaker.FieldName.ID).asText()).contains("commit_lsn:");
            });
            CloudEventsConverterTest.shouldConvertToCloudEventsInJsonWithDataAsAvro(record, false);
            CloudEventsConverterTest.shouldConvertToCloudEventsInAvro(record, "informix", TestHelper.TEST_DATABASE, false);
        }

    }

    private void assertRecord(Struct record, List<SchemaAndValueField> expected) {
        expected.forEach(schemaAndValueField -> schemaAndValueField.assertFor(record));
    }

}
