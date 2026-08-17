/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.sql.SQLException;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.informix.InformixConnectorConfig.SnapshotMode;
import io.debezium.connector.informix.util.TestHelper;
import io.debezium.data.Envelope.FieldName;
import io.debezium.data.SchemaAndValueField;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;

public class ConcurrentTransactionChangeLsnIT extends AbstractAsyncEngineConnectorTest {

    private InformixConnection connection;

    @BeforeEach
    public void before() throws SQLException {
        connection = TestHelper.testConnection();
        connection.execute(
                "DROP TABLE IF EXISTS concurrent_lsn",
                "CREATE TABLE concurrent_lsn (id int not null, val varchar(30), primary key (id))");

        Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
    }

    @AfterEach
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
                    .execute("DROP TABLE concurrent_lsn")
                    .close();
        }
    }

    /**
     * Regression test for a bug where a transaction's own change position gets stuck reporting a
     * leftover {@code source.change_lsn} left behind by a different, concurrently-committed
     * transaction that was processed earlier.
     *
     * <p>Scenario: {@code conn1} opens first and holds an X-statement transaction open -- its own
     * operations are logged before {@code conn2}'s even start -- while {@code conn2} opens later,
     * issues its own X statements, and commits first. {@code conn1} commits last. Every one of the
     * 2X captured events must get its own distinct {@code source.change_lsn}, regardless of how the
     * two transactions overlap in time.
     */
    @Test
    @FixFor("dbz#2336")
    public void changeLsnIsUniqueAcrossConcurrentTransactions() throws Exception {

        final int RECORDS_PER_TABLE = 50;
        final int ID_START = 100;
        final int ID_RESTART = 200;
        final Configuration config = TestHelper.defaultConfig()
                .with(InformixConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(InformixConnectorConfig.TABLE_INCLUDE_LIST, "testdb.informix.concurrent_lsn")
                .build();

        start(InformixConnector.class, config);
        assertConnectorIsRunning();

        // Wait for streaming to start
        waitForSnapshotToBeCompleted(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
        waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        try (InformixConnection connection1 = TestHelper.newConnection();
                InformixConnection connection2 = TestHelper.newConnection()) {

            connection1.setAutoCommit(false);
            for (int i = 0; i < RECORDS_PER_TABLE; i++) {
                final int id = ID_START + i;
                connection1.executeWithoutCommitting("INSERT INTO concurrent_lsn VALUES(%d, 'value')".formatted(id));
            }

            connection2.setAutoCommit(false);
            for (int i = 0; i < RECORDS_PER_TABLE; i++) {
                final int id = ID_RESTART + i;
                connection2.executeWithoutCommitting("INSERT INTO concurrent_lsn VALUES(%d, 'value')".formatted(id));
            }
            connection2.executeWithoutCommitting("INSERT INTO concurrent_lsn VALUES(%d, 'value')".formatted(ID_RESTART + RECORDS_PER_TABLE));

            connection1.commit();

            waitForAvailableRecords();

            SourceRecords sourceRecords = consumeRecordsByTopic(RECORDS_PER_TABLE);
            List<SourceRecord> records = sourceRecords.recordsForTopic("testdb.informix.concurrent_lsn");

            assertThat(records).hasSize(RECORDS_PER_TABLE);

            Long commitLsn = Long.valueOf(((String) records.get(0).sourceOffset().get("commit_lsn")));

            for (int i = 0; i < RECORDS_PER_TABLE; i++) {
                final int id = i + ID_START;
                final SourceRecord record = records.get(i);
                final List<SchemaAndValueField> expectedRowA = List.of(
                        new SchemaAndValueField("id", Schema.INT32_SCHEMA, id),
                        new SchemaAndValueField("val", Schema.OPTIONAL_STRING_SCHEMA, "value"));

                final Struct valueA = (Struct) record.value();
                assertRecord((Struct) valueA.get(FieldName.AFTER), expectedRowA);
                assertNull(valueA.get(FieldName.BEFORE));
            }

            connection2.commit();

            waitForAvailableRecords();

            sourceRecords = consumeRecordsByTopic(RECORDS_PER_TABLE + 1);
            records = sourceRecords.recordsForTopic("testdb.informix.concurrent_lsn");

            assertThat(records).hasSize(RECORDS_PER_TABLE + 1);

            for (int i = 0; i < RECORDS_PER_TABLE; i++) {
                final int id = i + ID_RESTART;
                final SourceRecord recordA = records.get(i);

                assertThat(Long.valueOf((String) recordA.sourceOffset().get("change_lsn"))).isLessThan(commitLsn);

                final List<SchemaAndValueField> expectedRowA = List.of(
                        new SchemaAndValueField("id", Schema.INT32_SCHEMA, id),
                        new SchemaAndValueField("val", Schema.OPTIONAL_STRING_SCHEMA, "value"));

                final Struct valueA = (Struct) recordA.value();
                assertRecord((Struct) valueA.get(FieldName.AFTER), expectedRowA);
                assertNull(valueA.get(FieldName.BEFORE));
            }

            // Final record with change_lsn = commit_lsn
            final SourceRecord recordA = records.get(RECORDS_PER_TABLE);

            assertThat(recordA.sourceOffset().get("change_lsn")).isEqualTo(recordA.sourceOffset().get("commit_lsn"));

            final List<SchemaAndValueField> expectedRowA = List.of(
                    new SchemaAndValueField("id", Schema.INT32_SCHEMA, RECORDS_PER_TABLE + ID_RESTART),
                    new SchemaAndValueField("val", Schema.OPTIONAL_STRING_SCHEMA, "value"));

            final Struct valueA = (Struct) recordA.value();
            assertRecord((Struct) valueA.get(FieldName.AFTER), expectedRowA);
            assertNull(valueA.get(FieldName.BEFORE));
        }

        assertNoRecordsToConsume();
    }

    /**
     * Regression test guarding the restart/recovery side of the same fix: {@code changeLsn} no
     * longer needs to be monotonically increasing across transactions, only within the same one.
     * That means a checkpoint taken mid-way through a still-open transaction can now legitimately
     * persist a {@code changeLsn} that is numerically lower than records already dispatched for a
     * different, earlier-committed transaction. If the recovery-time "already processed" skip
     * logic in {@code InformixStreamingChangeEventSource} relied on that old, transaction-unaware
     * monotonicity, this would cause the earlier-committed transaction's records to be replayed
     * (and duplicated) after a restart. It doesn't: whole-transaction skip on replay is gated by
     * {@code commitLsn} (which is bumped to the current transaction's own final commit position
     * before any of its own records are dispatched, and stays globally monotonic), so an
     * already-fully-committed transaction with a lower commitLsn is always skipped wholesale,
     * regardless of changeLsn. This test proves that end to end against a live connector restart.
     *
     * <p>Scenario: {@code conn1} opens first and buffers X statements but does not commit yet.
     * {@code conn2} then opens, buffers x statements, and commits -- fully before {@code conn1}.
     * Only then does {@code conn1} commit. The connector is killed (via a stop-on-record predicate)
     * mid-way through conn1's own X-record transaction. Note that the underlying CDC engine DOES
     * guarantee conn2's (fully independent, already-committed) transaction is delivered before conn1's.
     */
    @Test
    @FixFor("dbz#2336")
    public void restartWhileProcessingInterleavedTransactionDoesNotDuplicateEvents() throws Exception {

        final int RECORDS_PER_TABLE = 50;
        final int ID_START = 300;
        final int ID_RESTART = 400;
        final int HALF_ID = ID_START + RECORDS_PER_TABLE / 2;
        final Configuration config = TestHelper.defaultConfig()
                .with(InformixConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(InformixConnectorConfig.TABLE_INCLUDE_LIST, "testdb.informix.concurrent_lsn")
                .build();

        // Stop the connector halfway through dispatching conn1's operations, i.e. after
        // conn2's whole transaction -- have already gone out.
        start(InformixConnector.class, config, record -> {
            if (!"testdb.informix.concurrent_lsn.Envelope".equals(record.valueSchema().name())) {
                return false;
            }
            final Integer id = ((Struct) record.key()).getInt32("id");
            final String value = ((Struct) record.value()).getStruct(FieldName.AFTER).getString("val");
            return id != null && id == HALF_ID && "value".equals(value);
        });
        assertConnectorIsRunning();

        // Wait for streaming to start
        waitForSnapshotToBeCompleted(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
        waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        try (InformixConnection connection1 = TestHelper.newConnection();
                InformixConnection connection2 = TestHelper.newConnection()) {

            // connection1 opens first and stays open through X operations -- none visible to CDC yet.
            connection1.setAutoCommit(false);
            for (int i = 0; i < RECORDS_PER_TABLE; i++) {
                final int id = ID_START + i;
                connection1.executeWithoutCommitting("INSERT INTO concurrent_lsn VALUES(%d, 'value')".formatted(id));
            }

            // connection2 opens later, does its own X statements, and fully commits before connection1.
            connection2.setAutoCommit(false);
            for (int i = 0; i < RECORDS_PER_TABLE; i++) {
                final int id = ID_RESTART + i;
                connection2.executeWithoutCommitting("INSERT INTO concurrent_lsn VALUES(%d, 'value')".formatted(id));
            }
            connection2.commit();

            // connection1 commits last -- its already-buffered operations become visible to CDC after connection2's, all in one go.
            connection1.commit();

            waitForAvailableRecords();

            List<SourceRecord> records = consumeRecordsByTopic(RECORDS_PER_TABLE).recordsForTopic("testdb.informix.concurrent_lsn");

            assertThat(records).hasSize(RECORDS_PER_TABLE);

            for (int i = 0; i < RECORDS_PER_TABLE; i++) {
                final int id = i + ID_RESTART;
                final SourceRecord record = records.get(i);
                final List<SchemaAndValueField> expectedRow = List.of(
                        new SchemaAndValueField("id", Schema.INT32_SCHEMA, id),
                        new SchemaAndValueField("val", Schema.OPTIONAL_STRING_SCHEMA, "value"));

                final Struct value = (Struct) record.value();
                assertRecord((Struct) value.get(FieldName.AFTER), expectedRow);
                assertNull(value.get(FieldName.BEFORE));
            }

            // connection1's second half of operations must not have been delivered yet -- that's the whole point of the stop predicate.
            records = consumeRecordsByTopic(RECORDS_PER_TABLE / 2).recordsForTopic("testdb.informix.concurrent_lsn");

            assertThat(records).hasSize(RECORDS_PER_TABLE / 2);

            for (int i = 0; i < RECORDS_PER_TABLE / 2; i++) {
                final int id = i + ID_START;
                final SourceRecord record = records.get(i);
                final List<SchemaAndValueField> expectedRow = List.of(
                        new SchemaAndValueField("id", Schema.INT32_SCHEMA, id),
                        new SchemaAndValueField("val", Schema.OPTIONAL_STRING_SCHEMA, "value"));

                final Struct value = (Struct) record.value();
                assertRecord((Struct) value.get(FieldName.AFTER), expectedRow);
                assertNull(value.get(FieldName.BEFORE));
            }

            assertNoRecordsToConsume();

            // Wait for the stop predicate to actually halt the engine before draining whatever it managed to dispatch
            waitForConnectorShutdown(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
            assertConnectorNotRunning();
            cleanupTestFwkState();

            // Restart from the persisted offset, with no stop predicate this time.
            start(InformixConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
            waitForAvailableRecords();

            records = consumeRecordsByTopic(RECORDS_PER_TABLE / 2).recordsForTopic("testdb.informix.concurrent_lsn");
            assertThat(records).hasSize(RECORDS_PER_TABLE / 2);

            // connection1's second half of operations must show up now, having been held back before the restart.
            for (int i = 0; i < RECORDS_PER_TABLE / 2; i++) {
                final int id = i + HALF_ID;
                final SourceRecord record = records.get(i);
                final List<SchemaAndValueField> expectedRow = List.of(
                        new SchemaAndValueField("id", Schema.INT32_SCHEMA, id),
                        new SchemaAndValueField("val", Schema.OPTIONAL_STRING_SCHEMA, "value"));

                final Struct value = (Struct) record.value();
                assertRecord((Struct) value.get(FieldName.AFTER), expectedRow);
                assertNull(value.get(FieldName.BEFORE));
            }
        }

        assertNoRecordsToConsume();
    }

    private void assertRecord(Struct record, List<SchemaAndValueField> expected) {
        expected.forEach(schemaAndValueField -> schemaAndValueField.assertFor(record));
    }
}
