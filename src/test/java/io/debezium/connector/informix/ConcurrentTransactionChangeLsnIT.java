/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.informix.InformixConnectorConfig.SnapshotMode;
import io.debezium.connector.informix.util.TestHelper;
import io.debezium.data.Envelope.FieldName;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.util.Testing;

/**
 * Regression test for a bug where a transaction's own change position gets stuck reporting a
 * leftover {@code source.change_lsn} left behind by a different, concurrently-committed
 * transaction that was processed earlier.
 *
 * <p>Scenario: {@code conn1} opens first and holds a 3-statement transaction open -- its own
 * operations are logged before {@code conn2}'s even start -- while {@code conn2} opens later,
 * issues its own 3 statements, and commits first. {@code conn1} commits last. Every one of the
 * 6 captured events must get its own distinct {@code source.change_lsn}, regardless of how the
 * two transactions overlap in time.
 */
public class ConcurrentTransactionChangeLsnIT extends AbstractAsyncEngineConnectorTest {

    private InformixConnection connection;

    @BeforeEach
    public void before() throws SQLException {
        connection = TestHelper.testConnection();
        connection.execute(
                "DROP TABLE IF EXISTS concurrent_lsn",
                "CREATE TABLE concurrent_lsn (id int not null, val varchar(30), primary key (id))");

        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
        Testing.Debug.enable();
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

    @Test
    public void changeLsnIsUniqueAcrossConcurrentTransactions() throws Exception {
        final Configuration config = TestHelper.defaultConfig()
                .with(InformixConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(InformixConnectorConfig.TABLE_INCLUDE_LIST, "testdb.informix.concurrent_lsn")
                .build();

        start(InformixConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
        waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        try (InformixConnection conn1 = TestHelper.testConnection();
                InformixConnection conn2 = TestHelper.testConnection()) {

            // conn1 opens first and stays open -- its own operations are logged before conn2's,
            // but it commits last.
            conn1.setAutoCommit(false);
            conn1.executeWithoutCommitting("INSERT INTO concurrent_lsn VALUES(1, 'row1_v1')");
            conn1.executeWithoutCommitting("UPDATE concurrent_lsn SET val = 'row1_v2' WHERE id = 1");
            conn1.executeWithoutCommitting("UPDATE concurrent_lsn SET val = 'row1_v3' WHERE id = 1");

            // conn2 opens later, does its own 3 statements, and commits before conn1.
            conn2.setAutoCommit(false);
            conn2.executeWithoutCommitting("INSERT INTO concurrent_lsn VALUES(2, 'row2_v1')");
            conn2.executeWithoutCommitting("UPDATE concurrent_lsn SET val = 'row2_v2' WHERE id = 2");
            conn2.executeWithoutCommitting("UPDATE concurrent_lsn SET val = 'row2_v3' WHERE id = 2");
            conn2.commit();

            // conn1 commits last -- its 3 already-buffered operations become visible to CDC
            // after conn2's.
            conn1.commit();
        }

        waitForAvailableRecords();

        final SourceRecords records = consumeRecordsByTopic(6);
        final List<SourceRecord> all = records.recordsForTopic("testdb.informix.concurrent_lsn");
        assertThat(all).hasSize(6);

        // Group every captured event by its change_lsn. Desired behavior: 6 groups of 1.
        final Map<String, SourceRecord> byChangeLsn = new LinkedHashMap<>();
        for (SourceRecord record : all) {
            final Struct source = ((Struct) record.value()).getStruct("source");
            final String changeLsn = source.getString(SourceInfo.CHANGE_LSN_KEY);
            assertThat(changeLsn).isNotNull();

            final SourceRecord previous = byChangeLsn.putIfAbsent(changeLsn, record);
            if (previous != null) {
                fail("Duplicate change_lsn '" + changeLsn + "' detected across concurrent transactions:"
                        + "\n  first:  " + previous.value()
                        + "\n  second: " + record.value());
            }
        }
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
     * <p>Scenario: {@code conn1} opens first and buffers 4 statements (insert + 3 updates on the
     * same row) but does not commit yet. {@code conn2} then opens, buffers 2 statements, and
     * commits -- fully before {@code conn1}. Only then does {@code conn1} commit. The connector is
     * killed (via a stop-on-record predicate) right after conn1's own first 2 operations have been
     * dispatched, i.e. mid-way through conn1's own 4-record transaction. Note that the underlying
     * CDC engine does not guarantee conn2's (fully independent, already-committed) transaction is
     * delivered before or after conn1's -- either order is legitimate, so this test doesn't assume
     * one. What it does assert: across the pre-restart and post-restart phases combined, every one
     * of the 6 operations is delivered exactly once, and conn1's 3rd/4th operations -- held back by
     * the stop predicate -- only ever appear after the restart.
     */
    @Test
    public void restartWhileProcessingInterleavedTransactionDoesNotDuplicateEvents() throws Exception {
        final Configuration config = TestHelper.defaultConfig()
                .with(InformixConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(InformixConnectorConfig.TABLE_INCLUDE_LIST, "testdb.informix.concurrent_lsn")
                .build();

        // Stop the connector right before it would dispatch conn1's 3rd operation, i.e. after
        // conn1's first 2 operations -- and, in whichever relative order the engine chooses,
        // conn2's whole transaction -- have already gone out.
        start(InformixConnector.class, config, record -> {
            if (!"testdb.informix.concurrent_lsn.Envelope".equals(record.valueSchema().name())) {
                return false;
            }
            final Struct after = ((Struct) record.value()).getStruct(FieldName.AFTER);
            return after != null && "row1_v3".equals(after.getString("val"));
        });
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
        waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        try (InformixConnection conn1 = TestHelper.testConnection();
                InformixConnection conn2 = TestHelper.testConnection()) {

            // conn1 opens first and stays open through 4 operations -- none visible to CDC yet.
            conn1.setAutoCommit(false);
            conn1.executeWithoutCommitting("INSERT INTO concurrent_lsn VALUES(1, 'row1_v1')");
            conn1.executeWithoutCommitting("UPDATE concurrent_lsn SET val = 'row1_v2' WHERE id = 1");
            conn1.executeWithoutCommitting("UPDATE concurrent_lsn SET val = 'row1_v3' WHERE id = 1");
            conn1.executeWithoutCommitting("UPDATE concurrent_lsn SET val = 'row1_v4' WHERE id = 1");

            // conn2 opens later, does its own 2 statements, and fully commits before conn1.
            conn2.setAutoCommit(false);
            conn2.executeWithoutCommitting("INSERT INTO concurrent_lsn VALUES(2, 'row2_v1')");
            conn2.executeWithoutCommitting("UPDATE concurrent_lsn SET val = 'row2_v2' WHERE id = 2");
            conn2.commit();

            // conn1 commits last -- its 4 already-buffered operations become visible to CDC
            // after conn2's, all in one go.
            conn1.commit();
        }

        // Wait for the stop predicate to actually halt the engine before draining whatever it
        // managed to dispatch -- the exact count depends on the (unspecified) relative delivery
        // order of conn1's and conn2's transactions, so we don't assume one.
        waitForConnectorShutdown(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
        stopConnectorTolerantOfConcurrentShutdown();
        assertConnectorNotRunning();

        final List<SourceRecord> beforeRestartRecords = consumeAvailableRecordsByTopic()
                .recordsForTopic("testdb.informix.concurrent_lsn");
        final List<String> beforeRestartValues = valuesOf(beforeRestartRecords);

        // conn1's 3rd/4th operations must never have been delivered yet -- that's the whole point
        // of the stop predicate.
        assertThat(beforeRestartValues).doesNotContain("row1_v3", "row1_v4");

        // Restart from the persisted offset, with no stop predicate this time.
        start(InformixConnector.class, config);
        assertConnectorIsRunning();

        waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
        waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS);

        final int remaining = 6 - beforeRestartValues.size();
        final SourceRecords afterRestart = consumeRecordsByTopic(remaining);
        final List<SourceRecord> afterRestartRecords = afterRestart.recordsForTopic("testdb.informix.concurrent_lsn");
        assertThat(afterRestartRecords).hasSize(remaining);
        assertNoRecordsToConsume();

        // conn1's 3rd/4th operations must show up now, having been held back before the restart.
        final List<String> afterRestartValues = valuesOf(afterRestartRecords);
        assertThat(afterRestartValues).contains("row1_v3", "row1_v4");

        // Combined across both phases: every one of the 6 operations exactly once, no duplicates
        // and no omissions -- regardless of how the two transactions were interleaved by the
        // engine or split across the restart.
        final List<String> allValues = new ArrayList<>(beforeRestartValues);
        allValues.addAll(afterRestartValues);
        assertThat(allValues).containsExactlyInAnyOrder(
                "row1_v1", "row1_v2", "row1_v3", "row1_v4", "row2_v1", "row2_v2");
    }

    private static List<String> valuesOf(List<SourceRecord> records) {
        return records.stream()
                .map(record -> ((Struct) record.value()).getStruct(FieldName.AFTER).getString("val"))
                .collect(Collectors.toList());
    }

    /**
     * A stop-on-record predicate causes the engine to begin shutting itself down as soon as it
     * fires, racing with an explicit {@link #stopConnector()} call immediately afterwards: if the
     * engine hasn't fully torn itself down yet, {@code stopConnector()} throws
     * {@code IllegalStateException("Engine is already being shutting down.")} instead of the usual
     * no-op. Either way the end state is the same (fully stopped), so this just waits it out.
     */
    private void stopConnectorTolerantOfConcurrentShutdown() throws InterruptedException {
        try {
            stopConnector();
        }
        catch (IllegalStateException e) {
            for (int i = 0; i < 100 && isEngineRunning.get(); i++) {
                Thread.sleep(100);
            }
        }
    }
}
