/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.junit.Assert.assertNull;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.SnapshotType;
import io.debezium.connector.informix.InformixConnectorConfig.SnapshotMode;
import io.debezium.connector.informix.util.TestHelper;
import io.debezium.converters.CloudEventsConverterTest;
import io.debezium.converters.spi.CloudEventsMaker;
import io.debezium.data.Envelope.FieldName;
import io.debezium.data.SchemaAndValueField;
import io.debezium.data.SourceRecordAssert;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.heartbeat.DatabaseHeartbeatImpl;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.jdbc.JdbcValueConverters.DecimalMode;
import io.debezium.junit.ConditionalFail;
import io.debezium.junit.Flaky;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.relational.history.MemorySchemaHistory;
import io.debezium.schema.DatabaseSchema;

import junit.framework.TestCase;

/**
 * Integration test for the Debezium Informix connector.
 *
 */
@Flaky("DBZ-8114")
public class InformixConnectorIT extends AbstractAsyncEngineConnectorTest {

    @Rule
    public TestRule conditionalFail = new ConditionalFail();

    private InformixConnection connection;

    @Before
    public void before() throws SQLException {
        connection = TestHelper.testConnection();
        connection.execute(
                "DROP TABLE IF EXISTS tablea",
                "DROP TABLE IF EXISTS tableb",
                "DROP TABLE IF EXISTS masked_hashed_column_table",
                "DROP TABLE IF EXISTS truncated_column_table",
                "DROP TABLE IF EXISTS truncate_table",
                "DROP TABLE IF EXISTS dt_table",
                "DROP TABLE IF EXISTS always_snapshot",
                "DROP TABLE IF EXISTS test_heartbeat_table",
                "CREATE TABLE tablea (id int not null, cola varchar(30), primary key (id))",
                "CREATE TABLE tableb (id int not null, colb varchar(30), primary key (id))",
                "CREATE TABLE masked_hashed_column_table (id int not null, name varchar(255), name2 varchar(255), name3 varchar(20), primary key (id))",
                "CREATE TABLE truncated_column_table (id int not null, name varchar(20), primary key (id))",
                "CREATE TABLE truncate_table (id int not null, name varchar(20), primary key (id))",
                "CREATE TABLE dt_table (id int not null, c1 int, c2 int, c3a numeric(5,2), c3b varchar(128), f1 float(14), f2 decimal(8,4), primary key(id))",
                "CREATE TABLE always_snapshot (id int primary key not null, data varchar(50) not null)",
                "CREATE TABLE test_heartbeat_table (text varchar(255))",
                "INSERT INTO tablea VALUES(1, 'a')");
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
                            "DROP TABLE masked_hashed_column_table",
                            "DROP TABLE truncated_column_table",
                            "DROP TABLE truncate_table",
                            "DROP TABLE dt_table",
                            "DROP TABLE always_snapshot",
                            "DROP TABLE test_heartbeat_table")
                    .close();
        }
    }

    @Test
    public void deleteWithoutTombstone() throws Exception {
        final int RECORDS_PER_TABLE = 5;
        final int TABLES = 2;
        final int ID_START = 10;
        final Configuration config = TestHelper.defaultConfig()
                .with(InformixConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(InformixConnectorConfig.TOMBSTONES_ON_DELETE, false)
                .build();

        start(InformixConnector.class, config);
        assertConnectorIsRunning();

        // Wait for streaming to start
        waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START + i;
            connection.execute("INSERT INTO tablea VALUES(" + id + ", 'a')");
            connection.execute("INSERT INTO tableb VALUES(" + id + ", 'b')");
        }
        waitForAvailableRecords();

        consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);

        connection.execute("DELETE FROM tableB");

        final SourceRecords deleteRecords = consumeRecordsByTopic(RECORDS_PER_TABLE);
        final List<SourceRecord> deleteTableA = deleteRecords.recordsForTopic("testdb.informix.tablea");
        final List<SourceRecord> deleteTableB = deleteRecords.recordsForTopic("testdb.informix.tableb");
        assertThat(deleteTableA).isNullOrEmpty();
        assertThat(deleteTableB).hasSize(RECORDS_PER_TABLE);
        assertNoRecordsToConsume();

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final SourceRecord deleteRecord = deleteTableB.get(i);
            VerifyRecord.isValidDelete(deleteRecord, true);

            final List<SchemaAndValueField> expectedValueBefore = Arrays.asList(
                    new SchemaAndValueField("id", Schema.INT32_SCHEMA, i + ID_START),
                    new SchemaAndValueField("colb", Schema.OPTIONAL_STRING_SCHEMA, "b"));

            final Struct deleteValue = (Struct) deleteRecord.value();
            assertRecord((Struct) deleteValue.get(FieldName.BEFORE), expectedValueBefore);
        }
    }

    @Test
    public void deleteWithTombstone() throws Exception {
        final int RECORDS_PER_TABLE = 5;
        final int TABLES = 2;
        final int ID_START = 20;
        final Configuration config = TestHelper.defaultConfig()
                .with(InformixConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(InformixConnectorConfig.TOMBSTONES_ON_DELETE, true)
                .build();

        start(InformixConnector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot completion
        waitForSnapshotToBeCompleted(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
        consumeRecordsByTopic(1);

        // Wait for streaming to start
        waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START + i;
            connection.execute("INSERT INTO tablea VALUES(" + id + ", 'a')");
            connection.execute("INSERT INTO tableb VALUES(" + id + ", 'b')");
        }
        waitForAvailableRecords();

        consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);

        connection.execute("DELETE FROM tableB");

        final SourceRecords deleteRecords = consumeRecordsByTopic(RECORDS_PER_TABLE * 2);
        final List<SourceRecord> deleteTableA = deleteRecords.recordsForTopic("testdb.informix.tablea");
        final List<SourceRecord> deleteTableB = deleteRecords.recordsForTopic("testdb.informix.tableb");
        assertThat(deleteTableA).isNullOrEmpty();
        assertThat(deleteTableB).hasSize(RECORDS_PER_TABLE * 2);
        assertNoRecordsToConsume();

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
                assertRecord((Struct) deleteValue.get(FieldName.BEFORE), expectedValueBefore);
            }
        }
    }

    @Test
    public void testTruncateTable() throws Exception {
        final int RECORDS_PER_TABLE = 5;
        final int ID_START = 30;
        final Configuration config = TestHelper.defaultConfig()
                .with(InformixConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(CommonConnectorConfig.SKIPPED_OPERATIONS, "none")
                .build();

        start(InformixConnector.class, config);
        assertConnectorIsRunning();

        // Wait for streaming to start
        waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START + i;
            connection.execute("INSERT INTO truncate_table VALUES(" + id + ", 'name')");
        }
        waitForAvailableRecords();

        consumeRecordsByTopic(RECORDS_PER_TABLE);

        connection.execute("truncate table truncate_table");

        SourceRecords sourceRecords = consumeRecordsByTopic(1);
        List<SourceRecord> truncateTable = sourceRecords.recordsForTopic("testdb.informix.truncate_table");
        assertThat(truncateTable).isNotNull().hasSize(1);
        assertNoRecordsToConsume();

        VerifyRecord.isValidTruncate(truncateTable.get(0));
    }

    @Test
    public void updatePrimaryKey() throws Exception {

        final Configuration config = TestHelper.defaultConfig()
                .with(InformixConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .build();

        start(InformixConnector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot completion
        waitForSnapshotToBeCompleted(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
        consumeRecordsByTopic(1);

        // Wait for streaming to start
        waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        connection.execute("INSERT INTO tableb VALUES(1, 'b')");
        consumeRecordsByTopic(1);

        connection.execute("UPDATE tablea SET id=100 WHERE id=1", "UPDATE tableb SET id=100 WHERE id=1");
        waitForAvailableRecords();

        final SourceRecords records = consumeRecordsByTopic(6);
        final List<SourceRecord> tableA = records.recordsForTopic("testdb.informix.tablea");
        final List<SourceRecord> tableB = records.recordsForTopic("testdb.informix.tableb");
        assertThat(tableA).hasSize(3);
        assertThat(tableB).hasSize(3);
        assertNoRecordsToConsume();

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
        assertRecord(deleteValueA.getStruct(FieldName.BEFORE), expectedDeleteRowA);
        assertRecord(deleteKeyA, expectedDeleteKeyA);
        assertNull(deleteValueA.get(FieldName.AFTER));

        final Struct tombstoneKeyA = (Struct) tombstoneRecordA.key();
        final Struct tombstoneValueA = (Struct) tombstoneRecordA.value();
        assertRecord(tombstoneKeyA, expectedDeleteKeyA);
        assertNull(tombstoneValueA);

        final Struct insertKeyA = (Struct) insertRecordA.key();
        final Struct insertValueA = (Struct) insertRecordA.value();
        assertRecord(insertValueA.getStruct(FieldName.AFTER), expectedInsertRowA);
        assertRecord(insertKeyA, expectedInsertKeyA);
        assertNull(insertValueA.get(FieldName.BEFORE));

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
        assertRecord(deleteValueB.getStruct(FieldName.BEFORE), expectedDeleteRowB);
        assertRecord(deletekeyB, expectedDeleteKeyB);
        assertNull(deleteValueB.get(FieldName.AFTER));

        final Struct tombstonekeyB = (Struct) tombstoneRecordB.key();
        final Struct tombstoneValueB = (Struct) tombstoneRecordB.value();
        assertRecord(tombstonekeyB, expectedDeleteKeyB);
        assertNull(tombstoneValueB);

        final Struct insertkeyB = (Struct) insertRecordB.key();
        final Struct insertValueB = (Struct) insertRecordB.value();
        assertRecord(insertValueB.getStruct(FieldName.AFTER), expectedInsertRowB);
        assertRecord(insertkeyB, expectedInsertKeyB);
        assertNull(insertValueB.get(FieldName.BEFORE));
    }

    @Test
    @FixFor("DBZ-1152")
    public void updatePrimaryKeyWithRestartInMiddle() throws Exception {

        final Configuration config = TestHelper.defaultConfig()
                .with(InformixConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .build();

        start(InformixConnector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot completion
        waitForSnapshotToBeCompleted(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
        consumeRecordsByTopic(1);

        // Wait for streaming to start
        waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        connection.execute("INSERT INTO tableb VALUES(1, 'b')");
        consumeRecordsByTopic(1);

        connection.execute("UPDATE tablea SET id=100 WHERE id=1", "UPDATE tableb SET id=100 WHERE id=1");

        waitForAvailableRecords();

        final SourceRecords records1 = consumeRecordsByTopic(2);

        stopConnector();
        assertConnectorNotRunning();

        waitForConnectorShutdown(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        start(InformixConnector.class, config);
        assertConnectorIsRunning();

        // Wait for streaming to start
        waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
        waitForAvailableRecords();

        final SourceRecords records2 = consumeRecordsByTopic(4);

        final List<SourceRecord> tableA = records1.recordsForTopic("testdb.informix.tablea");
        tableA.addAll(records2.recordsForTopic("testdb.informix.tablea"));
        final List<SourceRecord> tableB = records2.recordsForTopic("testdb.informix.tableb");
        assertThat(tableA).hasSize(3);
        assertThat(tableB).hasSize(3);
        assertNoRecordsToConsume();

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
        assertRecord(deleteValueA.getStruct(FieldName.BEFORE), expectedDeleteRowA);
        assertRecord(deleteKeyA, expectedDeleteKeyA);
        assertNull(deleteValueA.get(FieldName.AFTER));

        final Struct tombstoneKeyA = (Struct) tombstoneRecordA.key();
        final Struct tombstoneValueA = (Struct) tombstoneRecordA.value();
        assertRecord(tombstoneKeyA, expectedDeleteKeyA);
        assertNull(tombstoneValueA);

        final Struct insertKeyA = (Struct) insertRecordA.key();
        final Struct insertValueA = (Struct) insertRecordA.value();
        assertRecord(insertValueA.getStruct(FieldName.AFTER), expectedInsertRowA);
        assertRecord(insertKeyA, expectedInsertKeyA);
        assertNull(insertValueA.get(FieldName.BEFORE));

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
        assertRecord(deleteValueB.getStruct(FieldName.BEFORE), expectedDeleteRowB);
        assertRecord(deletekeyB, expectedDeleteKeyB);
        assertNull(deleteValueB.get(FieldName.AFTER));

        final Struct tombstonekeyB = (Struct) tombstoneRecordB.key();
        final Struct tombstoneValueB = (Struct) tombstoneRecordB.value();
        assertRecord(tombstonekeyB, expectedDeleteKeyB);
        assertNull(tombstoneValueB);

        final Struct insertkeyB = (Struct) insertRecordB.key();
        final Struct insertValueB = (Struct) insertRecordB.value();
        assertRecord(insertValueB.getStruct(FieldName.AFTER), expectedInsertRowB);
        assertRecord(insertkeyB, expectedInsertKeyB);
        assertNull(insertValueB.get(FieldName.BEFORE));
    }

    @Test
    @FixFor("DBZ-1069")
    public void verifyOffsetsWithoutOnlineUpd() throws Exception {
        verifyOffsets(false);
    }

    @Test
    @FixFor("DBZ-7531")
    public void verifyOffsetsWithOnlineUpd() throws Exception {
        verifyOffsets(true);
    }

    public void verifyOffsets(boolean withOnlineUpd) throws Exception {
        final int RECORDS_PER_TABLE = 5;
        final int TABLES = 2;
        final int ID_START = 40;
        final int ID_RESTART = 100;
        final Configuration config = TestHelper.defaultConfig()
                .with(InformixConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(InformixConnectorConfig.CDC_TIMEOUT, 5)
                .with(InformixConnectorConfig.CDC_BUFFERSIZE, 0x800)
                .build();

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START + i;
            connection.execute("INSERT INTO tablea VALUES(" + id + ", 'a')");
            connection.execute("INSERT INTO tableb VALUES(" + id + ", 'b')");
        }

        start(InformixConnector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot completion
        waitForSnapshotToBeCompleted(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        List<SourceRecord> records = consumeRecordsByTopic(1 + RECORDS_PER_TABLE * TABLES).allRecordsInOrder();
        records = records.subList(1, records.size());
        for (Iterator<SourceRecord> it = records.iterator(); it.hasNext();) {
            SourceRecord record = it.next();
            assertThat(record.sourceOffset().get("snapshot")).as("Snapshot phase").isEqualTo(SnapshotType.INITIAL.toString());
            if (it.hasNext()) {
                assertThat(record.sourceOffset().get("snapshot_completed")).as("Snapshot in progress").isEqualTo(false);
            }
            else {
                assertThat(record.sourceOffset().get("snapshot_completed")).as("Snapshot completed").isEqualTo(true);
            }
        }

        if (withOnlineUpd) {
            // Wait for streaming to start
            waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

            connection.execute("UPDATE tablea SET cola = 'aa' WHERE id > 1");
            connection.execute("UPDATE tableb SET colb = 'bb' WHERE id > 1");

            waitForAvailableRecords();

            final SourceRecords sourceRecords = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);
            final List<SourceRecord> tableA = sourceRecords.recordsForTopic("testdb.informix.tablea");
            final List<SourceRecord> tableB = sourceRecords.recordsForTopic("testdb.informix.tableb");
            assertThat(tableA).hasSize(RECORDS_PER_TABLE);
            assertThat(tableB).hasSize(RECORDS_PER_TABLE);
            assertNoRecordsToConsume();
        }

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

        // Wait for streaming to start
        waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
        waitForAvailableRecords();

        final SourceRecords sourceRecords = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);
        final List<SourceRecord> tableA = sourceRecords.recordsForTopic("testdb.informix.tablea");
        final List<SourceRecord> tableB = sourceRecords.recordsForTopic("testdb.informix.tableb");
        assertThat(tableA).hasSize(RECORDS_PER_TABLE);
        assertThat(tableB).hasSize(RECORDS_PER_TABLE);
        assertNoRecordsToConsume();

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
            assertRecord((Struct) valueA.get(FieldName.AFTER), expectedRowA);
            assertNull(valueA.get(FieldName.BEFORE));

            final Struct valueB = (Struct) recordB.value();
            assertRecord((Struct) valueB.get(FieldName.AFTER), expectedRowB);
            assertNull(valueB.get(FieldName.BEFORE));

            assertThat(recordA.sourceOffset().get("snapshot")).as("Streaming phase").isNull();
            assertThat(recordA.sourceOffset().get("snapshot_completed")).as("Streaming phase").isNull();
            assertThat(recordA.sourceOffset().get("change_lsn")).as("LSN present").isNotNull();

            assertThat(recordB.sourceOffset().get("snapshot")).as("Streaming phase").isNull();
            assertThat(recordB.sourceOffset().get("snapshot_completed")).as("Streaming phase").isNull();
            assertThat(recordB.sourceOffset().get("change_lsn")).as("LSN present").isNotNull();
        }
    }

    @Test
    public void testTableIncludeList() throws Exception {
        final int RECORDS_PER_TABLE = 5;
        final int ID_START = 50;
        final Configuration config = TestHelper.defaultConfig()
                .with(InformixConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(InformixConnectorConfig.TABLE_INCLUDE_LIST, "testdb.informix.tableb")
                .build();

        start(InformixConnector.class, config);
        assertConnectorIsRunning();

        // Wait for streaming to start
        waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START + i;
            connection.execute("INSERT INTO tablea VALUES(" + id + ", 'a')");
            connection.execute("INSERT INTO tableb VALUES(" + id + ", 'b')");
        }
        waitForAvailableRecords();

        final SourceRecords records = consumeRecordsByTopic(RECORDS_PER_TABLE);
        final List<SourceRecord> tableA = records.recordsForTopic("testdb.informix.tablea");
        final List<SourceRecord> tableB = records.recordsForTopic("testdb.informix.tableb");
        assertThat(tableA).isNullOrEmpty();
        assertThat(tableB).hasSize(RECORDS_PER_TABLE);
        assertNoRecordsToConsume();
    }

    @Test
    public void testTableExcludeList() throws Exception {
        final int RECORDS_PER_TABLE = 5;
        final int ID_START = 60;
        final Configuration config = TestHelper.defaultConfig()
                .with(InformixConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(InformixConnectorConfig.TABLE_EXCLUDE_LIST, "testdb.informix.tablea")
                .build();

        connection.execute("INSERT INTO tableb VALUES(1, 'b')");

        start(InformixConnector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot completion
        waitForSnapshotToBeCompleted(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
        consumeRecordsByTopic(1);

        // Wait for streaming to start
        waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START + i;
            connection.execute("INSERT INTO tablea VALUES(" + id + ", 'a')");
            connection.execute("INSERT INTO tableb VALUES(" + id + ", 'b')");
        }
        waitForAvailableRecords();

        final SourceRecords records = consumeRecordsByTopic(RECORDS_PER_TABLE);
        final List<SourceRecord> tableA = records.recordsForTopic("testdb.informix.tablea");
        final List<SourceRecord> tableB = records.recordsForTopic("testdb.informix.tableb");
        assertThat(tableA).isNullOrEmpty();
        assertThat(tableB).hasSize(RECORDS_PER_TABLE);
        assertNoRecordsToConsume();
    }

    @Test
    @FixFor("DBZ-7813")
    public void testColumnIncludeListWithInitialSnapshot() throws Exception {
        testColumnIncludeList(SnapshotMode.INITIAL);
    }

    @Test
    @FixFor("DBZ-7813")
    public void testColumnIncludeListWithNoData() throws Exception {
        testColumnIncludeList(SnapshotMode.NO_DATA);
    }

    public void testColumnIncludeList(SnapshotMode snapshotMode) throws Exception {
        final Configuration config = TestHelper.defaultConfig()
                .with(InformixConnectorConfig.SNAPSHOT_MODE, snapshotMode)
                .with(InformixConnectorConfig.TABLE_INCLUDE_LIST, "testdb.informix.dt_table")
                .with(InformixConnectorConfig.COLUMN_INCLUDE_LIST,
                        "informix.dt_table.id,informix.dt_table.c1,informix.dt_table.c2,informix.dt_table.c3a,informix.dt_table.c3b")
                .build();

        final int expectedRecords;
        if (snapshotMode == SnapshotMode.INITIAL) {
            expectedRecords = 2;
            connection.execute("INSERT INTO dt_table (id,c1,c2,c3a,c3b,f1,f2) values (0,123,456,789.01,'test',1.228,234.56)");
        }
        else {
            expectedRecords = 1;
        }

        start(InformixConnector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot completion
        waitForSnapshotToBeCompleted(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        // Wait for streaming to start
        waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        connection.execute("INSERT INTO dt_table (id,c1,c2,c3a,c3b,f1,f2) values (1,123,456,789.01,'test',1.228,234.56)");

        waitForAvailableRecords();

        final SourceRecords records = consumeRecordsByTopic(expectedRecords);
        final List<SourceRecord> table = records.recordsForTopic("testdb.informix.dt_table");
        assertThat(table).hasSize(expectedRecords);
        Schema aSchema = SchemaBuilder.struct().optional()
                .name("testdb.informix.dt_table.Value")
                .field("id", Schema.INT32_SCHEMA)
                .field("c1", Schema.OPTIONAL_INT32_SCHEMA)
                .field("c2", Schema.OPTIONAL_INT32_SCHEMA)
                .field("c3a", SpecialValueDecimal.builder(DecimalMode.PRECISE, 5, 2).optional().build())
                .field("c3b", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        Struct aStruct = new Struct(aSchema)
                .put("c1", 123)
                .put("c2", 456)
                .put("c3a", BigDecimal.valueOf(789.01))
                .put("c3b", "test");
        if (snapshotMode == SnapshotMode.INITIAL) {
            SourceRecordAssert.assertThat(table.get(0)).valueAfterFieldIsEqualTo(aStruct.put("id", 0));
            SourceRecordAssert.assertThat(table.get(1)).valueAfterFieldIsEqualTo(aStruct.put("id", 1));
        }
        else {
            SourceRecordAssert.assertThat(table.get(0)).valueAfterFieldIsEqualTo(aStruct.put("id", 1));
        }

        assertNoRecordsToConsume();
    }

    @Test
    @FixFor("DBZ-7813")
    public void testColumnExcludeListWithInitialSnapshot() throws Exception {
        testColumnExcludeList(SnapshotMode.INITIAL);
    }

    @Test
    @FixFor("DBZ-7813")
    public void testColumnExcludeListWithINoData() throws Exception {
        testColumnExcludeList(SnapshotMode.NO_DATA);
    }

    public void testColumnExcludeList(SnapshotMode snapshotMode) throws Exception {
        final Configuration config = TestHelper.defaultConfig()
                .with(InformixConnectorConfig.SNAPSHOT_MODE, snapshotMode)
                .with(InformixConnectorConfig.TABLE_EXCLUDE_LIST, "testdb.informix.tablea")
                .with(InformixConnectorConfig.COLUMN_EXCLUDE_LIST, "informix.dt_table.f1,informix.dt_table.f2")
                .build();

        final int expectedRecords;
        if (snapshotMode == SnapshotMode.INITIAL) {
            expectedRecords = 2;
            connection.execute("INSERT INTO dt_table (id,c1,c2,c3a,c3b,f1,f2) values (0,123,456,789.01,'test',1.228,234.56)");
        }
        else {
            expectedRecords = 1;
        }

        start(InformixConnector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot completion
        waitForSnapshotToBeCompleted(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        // Wait for streaming to start
        waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        connection.execute("INSERT INTO dt_table (id,c1,c2,c3a,c3b,f1,f2) values (1,123,456,789.01,'test',1.228,234.56)");

        waitForAvailableRecords();

        final SourceRecords records = consumeRecordsByTopic(expectedRecords);
        final List<SourceRecord> table = records.recordsForTopic("testdb.informix.dt_table");
        assertThat(table).hasSize(expectedRecords);
        Schema aSchema = SchemaBuilder.struct().optional()
                .name("testdb.informix.dt_table.Value")
                .field("id", Schema.INT32_SCHEMA)
                .field("c1", Schema.OPTIONAL_INT32_SCHEMA)
                .field("c2", Schema.OPTIONAL_INT32_SCHEMA)
                .field("c3a", SpecialValueDecimal.builder(DecimalMode.PRECISE, 5, 2).optional().build())
                .field("c3b", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        Struct aStruct = new Struct(aSchema)
                .put("c1", 123)
                .put("c2", 456)
                .put("c3a", BigDecimal.valueOf(789.01))
                .put("c3b", "test");
        if (snapshotMode == SnapshotMode.INITIAL) {
            SourceRecordAssert.assertThat(table.get(0)).valueAfterFieldIsEqualTo(aStruct.put("id", 0));
            SourceRecordAssert.assertThat(table.get(1)).valueAfterFieldIsEqualTo(aStruct.put("id", 1));
        }
        else {
            SourceRecordAssert.assertThat(table.get(0)).valueAfterFieldIsEqualTo(aStruct.put("id", 1));
        }

        assertNoRecordsToConsume();
    }

    private void restartInTheMiddleOfTx(boolean restartJustAfterSnapshot, boolean afterStreaming) throws Exception {
        final int RECORDS_PER_TABLE = 30;
        final int TABLES = 2;
        final int ID_START = 200;
        final int ID_RESTART = 1000;
        final int HALF_ID = ID_START + RECORDS_PER_TABLE / 2;
        final Configuration config = TestHelper.defaultConfig()
                .with(InformixConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(InformixConnectorConfig.CDC_TIMEOUT, 5)
                .with(InformixConnectorConfig.CDC_BUFFERSIZE, 0x800)
                .build();

        if (restartJustAfterSnapshot) {
            start(InformixConnector.class, config);
            assertConnectorIsRunning();

            // Wait for snapshot to be completed
            waitForSnapshotToBeCompleted(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
            consumeRecordsByTopic(1);

            stopConnector();
            assertConnectorNotRunning();
            waitForConnectorShutdown(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

            connection.execute("INSERT INTO tablea VALUES(-1, '-a')");
        }

        start(InformixConnector.class, config, record -> {
            if (!"testdb.informix.tablea.Envelope".equals(record.valueSchema().name())) {
                return false;
            }
            final Struct envelope = (Struct) record.value();
            final Struct after = envelope.getStruct(FieldName.AFTER);
            final Integer id = after.getInt32("id");
            final String value = after.getString("cola");
            return id != null && id == HALF_ID && "a".equals(value);
        });
        assertConnectorIsRunning();

        // Wait for snapshot to be completed or a first streaming message delivered
        if (restartJustAfterSnapshot) {
            waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
            waitForAvailableRecords();
        }
        else {
            waitForSnapshotToBeCompleted(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
        }
        consumeRecordsByTopic(1);

        if (afterStreaming) {
            waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
            connection.execute("INSERT INTO tablea VALUES(-2, '-a')");
            final SourceRecords records = consumeRecordsByTopic(1);
            final List<SchemaAndValueField> expectedRow = List.of(
                    new SchemaAndValueField("id", Schema.INT32_SCHEMA, -2),
                    new SchemaAndValueField("cola", Schema.OPTIONAL_STRING_SCHEMA, "-a"));
            assertRecord(((Struct) records.allRecordsInOrder().get(0).value()).getStruct(FieldName.AFTER), expectedRow);
        }

        connection.setAutoCommit(false);
        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START + i;
            connection.executeWithoutCommitting("INSERT INTO tablea VALUES(" + id + ", 'a')");
            connection.executeWithoutCommitting("INSERT INTO tableb VALUES(" + id + ", 'b')");
        }
        connection.commit();

        waitForAvailableRecords();

        List<SourceRecord> records = consumeRecordsByTopic(RECORDS_PER_TABLE).allRecordsInOrder();

        assertThat(records).hasSize(RECORDS_PER_TABLE);
        SourceRecord lastRecordForOffset = records.get(RECORDS_PER_TABLE - 1);
        Struct value = (Struct) lastRecordForOffset.value();
        final List<SchemaAndValueField> expectedLastRow = List.of(
                new SchemaAndValueField("id", Schema.INT32_SCHEMA, HALF_ID - 1),
                new SchemaAndValueField("colb", Schema.OPTIONAL_STRING_SCHEMA, "b"));
        assertRecord((Struct) value.get(FieldName.AFTER), expectedLastRow);

        waitForConnectorShutdown(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
        stopConnector();
        assertConnectorNotRunning();

        start(InformixConnector.class, config);
        assertConnectorIsRunning();
        waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS);

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
            assertRecord((Struct) valueA.get(FieldName.AFTER), expectedRowA);
            assertNull(valueA.get(FieldName.BEFORE));

            final Struct valueB = (Struct) recordB.value();
            assertRecord((Struct) valueB.get(FieldName.AFTER), expectedRowB);
            assertNull(valueB.get(FieldName.BEFORE));
        }

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_RESTART + i;
            connection.executeWithoutCommitting("INSERT INTO tablea VALUES(" + id + ", 'a')");
            connection.executeWithoutCommitting("INSERT INTO tableb VALUES(" + id + ", 'b')");
            connection.commit();
        }
        waitForAvailableRecords();

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
            assertRecord((Struct) valueA.get(FieldName.AFTER), expectedRowA);
            assertNull(valueA.get(FieldName.BEFORE));

            final Struct valueB = (Struct) recordB.value();
            assertRecord((Struct) valueB.get(FieldName.AFTER), expectedRowB);
            assertNull(valueB.get(FieldName.BEFORE));
        }

        assertNoRecordsToConsume();
    }

    @Test
    @FixFor("DBZ-1128")
    public void restartInTheMiddleOfTxAfterSnapshot() throws Exception {
        restartInTheMiddleOfTx(true, false);
    }

    @Test
    @FixFor("DBZ-1128")
    public void restartInTheMiddleOfTxAfterCompletedTx() throws Exception {
        restartInTheMiddleOfTx(false, true);
    }

    @Test
    @FixFor("DBZ-1128")
    public void restartInTheMiddleOfTx() throws Exception {
        restartInTheMiddleOfTx(false, false);
    }

    @Test
    @FixFor("DBZ-1242")
    public void testEmptySchemaWarningAfterApplyingFilters() throws Exception {
        // This captures all logged messages, allowing us to verify log message was written.
        final LogInterceptor logInterceptor = new LogInterceptor(RelationalDatabaseSchema.class);

        Configuration config = TestHelper.defaultConfig()
                .with(InformixConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(InformixConnectorConfig.TABLE_INCLUDE_LIST, "my_products")
                .build();

        start(InformixConnector.class, config);
        assertConnectorIsRunning();

        // Wait for streaming to start
        waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        stopConnector(value -> assertThat(logInterceptor.containsWarnMessage(DatabaseSchema.NO_CAPTURED_DATA_COLLECTIONS_WARNING)).isTrue());
    }

    @Test
    @FixFor("DBZ-775")
    public void shouldConsumeEventsWithMaskedAndTruncatedColumns() throws Exception {
        final Configuration config = TestHelper.defaultConfig()
                .with(InformixConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with("column.mask.with.12.chars", "testdb.informix.masked_hashed_column_table.name")
                .with("column.mask.hash.SHA-256.with.salt.CzQMA0cB5K",
                        "testdb.informix.masked_hashed_column_table.name2,testdb.informix.masked_hashed_column_table.name3")
                .with("column.truncate.to.4.chars", "testdb.informix.truncated_column_table.name")
                .build();

        start(InformixConnector.class, config);
        assertConnectorIsRunning();

        // Wait for streaming to start
        waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
        waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS);

        connection.execute("INSERT INTO masked_hashed_column_table (id, name, name2, name3) VALUES (10, 'some_name', 'test', 'test')");
        connection.execute("INSERT INTO truncated_column_table VALUES(11, 'some_name')");

        waitForAvailableRecords();

        final SourceRecords records = consumeRecordsByTopic(2);
        final List<SourceRecord> tableA = records.recordsForTopic("testdb.informix.masked_hashed_column_table");
        final List<SourceRecord> tableB = records.recordsForTopic("testdb.informix.truncated_column_table");
        assertThat(tableA).hasSize(1);
        assertNoRecordsToConsume();

        SourceRecord record = tableA.get(0);
        VerifyRecord.isValidInsert(record, "id", 10);

        Struct value = (Struct) record.value();
        if (value.getStruct(FieldName.AFTER) != null) {
            Struct after = value.getStruct(FieldName.AFTER);
            assertThat(after.getString("name")).isEqualTo("************");
            assertThat(after.getString("name2")).isEqualTo("8e68c68edbbac316dfe2f6ada6b0d2d3e2002b487a985d4b7c7c82dd83b0f4d7");
            assertThat(after.getString("name3")).isEqualTo("8e68c68edbbac316dfe2");
        }

        assertThat(tableB).hasSize(1);
        record = tableB.get(0);
        VerifyRecord.isValidInsert(record, "id", 11);

        value = (Struct) record.value();
        if (value.getStruct(FieldName.AFTER) != null) {
            assertThat(value.getStruct(FieldName.AFTER).getString("name")).isEqualTo("some");
        }
    }

    @Test
    @FixFor("DBZ-775")
    public void shouldRewriteIdentityKey() throws Exception {
        final Configuration config = TestHelper.defaultConfig()
                .with(InformixConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(InformixConnectorConfig.MSG_KEY_COLUMNS, "(.*).tablea:id,cola")
                .build();

        start(InformixConnector.class, config);
        assertConnectorIsRunning();

        // Wait for streaming to start
        waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        connection.execute("INSERT INTO tablea (id, cola) values (100, 'hundred')");

        waitForAvailableRecords();

        List<SourceRecord> records = consumeRecordsByTopic(1).recordsForTopic("testdb.informix.tablea");
        assertThat(records).isNotNull().isNotEmpty();
        assertThat(records.get(0).key()).isNotNull();
        Struct key = (Struct) records.get(0).key();
        assertThat(key.get("id")).isNotNull();
        assertThat(key.get("cola")).isNotNull();
        assertNoRecordsToConsume();
    }

    @Test
    @FixFor({ "DBZ-1916", "DBZ-1830" })
    public void shouldPropagateSourceTypeByDatatype() throws Exception {
        final Configuration config = TestHelper.defaultConfig()
                .with(InformixConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with("datatype.propagate.source.type", ".+\\.NUMERIC,.+\\.VARCHAR,.+\\.DECIMAL,.+\\.FLOAT")
                .build();

        start(InformixConnector.class, config);
        assertConnectorIsRunning();

        // Wait for streaming to start
        waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        connection.execute("INSERT INTO dt_table (id,c1,c2,c3a,c3b,f1,f2) values (1,123,456,789.01,'test',1.228,234.56)");

        waitForAvailableRecords();

        final SourceRecords records = consumeRecordsByTopic(1);

        List<SourceRecord> recordsForTopic = records.recordsForTopic("testdb.informix.dt_table");
        assertThat(recordsForTopic).hasSize(1);
        assertNoRecordsToConsume();

        final Field before = recordsForTopic.get(0).valueSchema().field(FieldName.BEFORE);

        assertThat(before.schema().field("id").schema().parameters()).isNull();
        assertThat(before.schema().field("c1").schema().parameters()).isNull();
        assertThat(before.schema().field("c2").schema().parameters()).isNull();

        assertThat(before.schema().field("c3a").schema().parameters())
                .contains(entry(TestHelper.TYPE_NAME_PARAMETER_KEY, "DECIMAL"),
                        entry(TestHelper.TYPE_LENGTH_PARAMETER_KEY, "5"),
                        entry(TestHelper.TYPE_SCALE_PARAMETER_KEY, "2"));

        assertThat(before.schema().field("c3b").schema().parameters())
                .contains(entry(TestHelper.TYPE_NAME_PARAMETER_KEY, "VARCHAR"),
                        entry(TestHelper.TYPE_LENGTH_PARAMETER_KEY, "128"));

        assertThat(before.schema().field("f1").schema().parameters())
                .contains(entry(TestHelper.TYPE_NAME_PARAMETER_KEY, "FLOAT"),
                        entry(TestHelper.TYPE_LENGTH_PARAMETER_KEY, "17"));

        assertThat(before.schema().field("f2").schema().parameters())
                .contains(entry(TestHelper.TYPE_NAME_PARAMETER_KEY, "DECIMAL"),
                        entry(TestHelper.TYPE_LENGTH_PARAMETER_KEY, "8"),
                        entry(TestHelper.TYPE_SCALE_PARAMETER_KEY, "4"));
    }

    @Test
    @FixFor("DBZ-3668")
    public void shouldOutputRecordsInCloudEventsFormat() throws Exception {
        final Configuration config = TestHelper.defaultConfig()
                .with(InformixConnectorConfig.TABLE_INCLUDE_LIST, "testdb.informix.tablea")
                .build();

        start(InformixConnector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot completion
        waitForSnapshotToBeCompleted(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        SourceRecords records = consumeRecordsByTopic(1);

        List<SourceRecord> tablea = records.recordsForTopic("testdb.informix.tablea");
        assertThat(tablea).hasSize(1);

        for (SourceRecord record : tablea) {
            CloudEventsConverterTest.shouldConvertToCloudEventsInJson(record, false);
            CloudEventsConverterTest.shouldConvertToCloudEventsInJsonWithDataAsAvro(record, false);
            CloudEventsConverterTest.shouldConvertToCloudEventsInAvro(record, "informix", TestHelper.TEST_DATABASE, false);
        }

        // Wait for streaming to start
        waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        connection.execute("INSERT INTO tablea (id,cola) VALUES (3668, 'DBZ3668')");

        waitForAvailableRecords();

        records = consumeRecordsByTopic(1);

        tablea = records.recordsForTopic("testdb.informix.tablea");
        assertThat(tablea).hasSize(1);
        assertNoRecordsToConsume();

        for (SourceRecord record : tablea) {
            CloudEventsConverterTest.shouldConvertToCloudEventsInJson(record, false, jsonNode -> {
                assertThat(jsonNode.get(CloudEventsMaker.FieldName.ID).asText()).contains("commit_lsn:");
            });
            CloudEventsConverterTest.shouldConvertToCloudEventsInJsonWithDataAsAvro(record, false);
            CloudEventsConverterTest.shouldConvertToCloudEventsInAvro(record, "informix", TestHelper.TEST_DATABASE, false);
        }
    }

    @Test
    public void shouldNotUseOffsetWhenSnapshotIsAlways() throws Exception {

        Configuration config = TestHelper.defaultConfig()
                .with(InformixConnectorConfig.SNAPSHOT_MODE, SnapshotMode.ALWAYS)
                .with(InformixConnectorConfig.TABLE_INCLUDE_LIST, "testdb.informix.always_snapshot")
                .with(InformixConnectorConfig.SNAPSHOT_MODE_TABLES, "testdb.informix.always_snapshot")
                .with(InformixConnectorConfig.STORE_ONLY_CAPTURED_TABLES_DDL, true)
                .with(InformixConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .build();

        connection.execute("INSERT INTO always_snapshot VALUES (1,'Test1');");
        connection.execute("INSERT INTO always_snapshot VALUES (2,'Test2');");

        start(InformixConnector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot completion
        waitForSnapshotToBeCompleted(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        int expectedRecordCount = 2;
        SourceRecords sourceRecords = consumeRecordsByTopic(expectedRecordCount);
        assertThat(sourceRecords.recordsForTopic("testdb.informix.always_snapshot")).hasSize(expectedRecordCount);
        Struct struct = (Struct) ((Struct) sourceRecords.allRecordsInOrder().get(0).value()).get(FieldName.AFTER);
        TestCase.assertEquals(1, struct.get("id"));
        TestCase.assertEquals("Test1", struct.get("data"));
        struct = (Struct) ((Struct) sourceRecords.allRecordsInOrder().get(1).value()).get(FieldName.AFTER);
        TestCase.assertEquals(2, struct.get("id"));
        TestCase.assertEquals("Test2", struct.get("data"));

        stopConnector();
        assertConnectorNotRunning();

        waitForConnectorShutdown(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        connection.execute("DELETE FROM ALWAYS_SNAPSHOT WHERE id=1;");
        connection.execute("INSERT INTO ALWAYS_SNAPSHOT VALUES (3,'Test3');");

        start(InformixConnector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot completion
        waitForSnapshotToBeCompleted(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        sourceRecords = consumeRecordsByTopic(expectedRecordCount);

        // Check we get up-to-date data in the snapshot.
        assertThat(sourceRecords.recordsForTopic("testdb.informix.always_snapshot")).hasSize(expectedRecordCount);
        struct = (Struct) ((Struct) sourceRecords.recordsForTopic("testdb.informix.always_snapshot").get(0).value()).get(FieldName.AFTER);
        TestCase.assertEquals(3, struct.get("id"));
        TestCase.assertEquals("Test3", struct.get("data"));
        struct = (Struct) ((Struct) sourceRecords.recordsForTopic("testdb.informix.always_snapshot").get(1).value()).get(FieldName.AFTER);
        TestCase.assertEquals(2, struct.get("id"));
        TestCase.assertEquals("Test2", struct.get("data"));
    }

    @Test
    @FixFor("DBZ-7699")
    public void shouldCreateSnapshotSchemaOnlyRecovery() throws Exception {

        Configuration.Builder builder = TestHelper.defaultConfig()
                .with(InformixConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(InformixConnectorConfig.TABLE_INCLUDE_LIST, "testdb.informix.tablea")
                .with(InformixConnectorConfig.SCHEMA_HISTORY, MemorySchemaHistory.class.getName());

        Configuration config = builder.build();
        // Start the connector ...
        start(InformixConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        int recordCount = 1;
        SourceRecords sourceRecords = consumeRecordsByTopic(recordCount);
        assertThat(sourceRecords.allRecordsInOrder()).hasSize(recordCount);

        stopConnector();
        assertConnectorNotRunning();

        waitForConnectorShutdown(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        builder.with(InformixConnectorConfig.SNAPSHOT_MODE, SnapshotMode.RECOVERY);
        config = builder.build();
        start(InformixConnector.class, config);
        assertConnectorIsRunning();

        waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        connection.execute("INSERT INTO tablea VALUES (100,'100')");
        connection.execute("INSERT INTO tablea VALUES (200,'200')");

        waitForAvailableRecords();

        recordCount = 2;
        sourceRecords = consumeRecordsByTopic(recordCount);
        assertThat(sourceRecords.allRecordsInOrder()).hasSize(recordCount);
    }

    @Test
    public void shouldAllowForCustomSnapshot() throws InterruptedException, SQLException {

        final String pkField = "id";

        Configuration config = TestHelper.defaultConfig()
                .with(InformixConnectorConfig.SNAPSHOT_MODE, SnapshotMode.CUSTOM)
                .with(InformixConnectorConfig.SNAPSHOT_MODE_CUSTOM_NAME, CustomTestSnapshot.class.getName())
                .with(CommonConnectorConfig.SNAPSHOT_MODE_TABLES, "testdb.informix.tablea,testdb.informix.tableb")
                .with(CommonConnectorConfig.SNAPSHOT_QUERY_MODE, CommonConnectorConfig.SnapshotQueryMode.CUSTOM)
                .with(CommonConnectorConfig.SNAPSHOT_QUERY_MODE_CUSTOM_NAME, CustomTestSnapshot.class.getName())
                .build();

        connection.execute("INSERT INTO tableb VALUES (1, '1');");

        start(InformixConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        SourceRecords actualRecords = consumeRecordsByTopic(2);

        List<SourceRecord> s1recs = actualRecords.recordsForTopic("testdb.informix.tablea");
        List<SourceRecord> s2recs = actualRecords.recordsForTopic("testdb.informix.tableb");

        if (s2recs != null) { // Sometimes the record is processed by the stream so filtering it out
            s2recs = s2recs.stream().filter(r -> "r".equals(((Struct) r.value()).get("op")))
                    .collect(Collectors.toList());
        }
        assertThat(s1recs.size()).isEqualTo(1);
        assertThat(s2recs).isNull();

        SourceRecord record = s1recs.get(0);
        VerifyRecord.isValidRead(record, pkField, 1);

        waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        connection.execute("INSERT INTO tablea VALUES (2, '1');");
        connection.execute("INSERT INTO tableb VALUES (2, '1');");

        waitForAvailableRecords();

        actualRecords = consumeRecordsByTopic(2);

        s1recs = actualRecords.recordsForTopic("testdb.informix.tablea");
        s2recs = actualRecords.recordsForTopic("testdb.informix.tableb");
        assertThat(s1recs.size()).isEqualTo(1);
        assertThat(s2recs.size()).isEqualTo(1);
        record = s1recs.get(0);
        VerifyRecord.isValidInsert(record, pkField, 2);
        record = s2recs.get(0);
        VerifyRecord.isValidInsert(record, pkField, 2);

        stopConnector();
        assertConnectorNotRunning();

        waitForConnectorShutdown(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        config = TestHelper.defaultConfig()
                .with(InformixConnectorConfig.SNAPSHOT_MODE, SnapshotMode.CUSTOM)
                .with(InformixConnectorConfig.SNAPSHOT_MODE_CUSTOM_NAME, CustomTestSnapshot.class.getName())
                .with(CommonConnectorConfig.SNAPSHOT_QUERY_MODE, CommonConnectorConfig.SnapshotQueryMode.CUSTOM)
                .with(CommonConnectorConfig.SNAPSHOT_QUERY_MODE_CUSTOM_NAME, CustomTestSnapshot.class.getName())
                .build();

        start(InformixConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        actualRecords = consumeRecordsByTopic(4);

        s1recs = actualRecords.recordsForTopic("testdb.informix.tablea");
        s2recs = actualRecords.recordsForTopic("testdb.informix.tableb");
        assertThat(s1recs.size()).isEqualTo(2);
        assertThat(s2recs.size()).isEqualTo(2);
        VerifyRecord.isValidRead(s1recs.get(0), pkField, 1);
        VerifyRecord.isValidRead(s1recs.get(1), pkField, 2);
        VerifyRecord.isValidRead(s2recs.get(0), pkField, 1);
        VerifyRecord.isValidRead(s2recs.get(1), pkField, 2);
    }

    @Test()
    @FixFor("DBZ-9081")
    public void testHeartbeatExecuted() throws Exception {
        final Configuration config = TestHelper.defaultConfig()
                .with(InformixConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(InformixConnectorConfig.TABLE_INCLUDE_LIST, "testdb.informix.tablea")
                // A low heartbeat interval should make sure that a heartbeat message is emitted at least once during the test.
                .with(Heartbeat.HEARTBEAT_INTERVAL, "100")
                .build();

        start(InformixConnector.class, config);
        assertConnectorIsRunning();

        // Wait for streaming to start
        waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        connection.execute("INSERT INTO tablea VALUES (3,'aaa')");

        waitForAvailableRecords();

        SourceRecords sourceRecords = consumeAvailableRecordsByTopic();
        assertThat(sourceRecords.recordsForTopic("testdb.informix.tablea")).hasSize(1);
        assertThat(sourceRecords.recordsForTopic("__debezium-heartbeat.testdb")).hasSizeGreaterThanOrEqualTo(1);
    }

    @Test()
    @FixFor("DBZ-9081")
    public void testHeartbeatActionQueryExecuted() throws Exception {
        final Configuration config = TestHelper.defaultConfig()
                .with(InformixConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(InformixConnectorConfig.TABLE_INCLUDE_LIST, "testdb.informix.tablea")
                // A low heartbeat interval should make sure that a heartbeat message is emitted at least once during the test.
                .with(Heartbeat.HEARTBEAT_INTERVAL, "100")
                .with(DatabaseHeartbeatImpl.HEARTBEAT_ACTION_QUERY, "INSERT INTO test_heartbeat_table (text) VALUES ('test_heartbeat');")
                .build();

        start(InformixConnector.class, config);
        assertConnectorIsRunning();

        // Wait for streaming to start
        waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        connection.execute("INSERT INTO tablea VALUES (3,'aaa')");

        waitForAvailableRecords();

        SourceRecords sourceRecords = consumeAvailableRecordsByTopic();
        assertThat(sourceRecords.recordsForTopic("testdb.informix.tablea")).hasSize(1);
        int numOfHeartbeats = sourceRecords.recordsForTopic("__debezium-heartbeat.testdb").size();

        // Confirm that the heartbeat.action.query was executed with the heartbeat. It is difficult to determine the
        // exact amount of times the heartbeat will fire because the run time of the test will vary, but if there is
        // anything in test_heartbeat_table then this test is confirmed.
        int numOfHeartbeatActions = connection.queryAndMap("SELECT COUNT(*) FROM test_heartbeat_table;", rs -> rs.next() ? rs.getInt(1) : 0);

        assertThat(numOfHeartbeatActions).isGreaterThanOrEqualTo(numOfHeartbeats);
    }

    private void assertRecord(Struct record, List<SchemaAndValueField> expected) {
        expected.forEach(schemaAndValueField -> schemaAndValueField.assertFor(record));
    }
}
