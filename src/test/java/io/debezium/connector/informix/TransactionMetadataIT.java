/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.informix.InformixConnectorConfig.SnapshotMode;
import io.debezium.connector.informix.util.TestHelper;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.util.Collect;

/**
 * Transaction metadata test for the Debezium Informix Server connector.
 *
 */
public class TransactionMetadataIT extends AbstractAsyncEngineConnectorTest {

    private InformixConnection connection;

    @Before
    public void before() throws SQLException {
        connection = TestHelper.testConnection();
        connection.execute(
                "DROP TABLE IF EXISTS tablea",
                "DROP TABLE IF EXISTS tableb",
                "CREATE TABLE tablea (id int not null, cola varchar(30), primary key (id))",
                "CREATE TABLE tableb (id int not null, colb varchar(30), primary key (id))",
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
                            "DROP TABLE tableb")
                    .close();
        }
    }

    @Test
    public void transactionMetadata() throws Exception {
        final int RECORDS_PER_TABLE = 5;
        final int ID_START = 10;
        final Configuration config = TestHelper.defaultConfig()
                .with(InformixConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(InformixConnectorConfig.PROVIDE_TRANSACTION_METADATA, true)
                .build();

        start(InformixConnector.class, config);

        assertConnectorIsRunning();

        // Wait for snapshot completion
        waitForSnapshotToBeCompleted(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        consumeRecordsByTopic(1);

        waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        connection.setAutoCommit(false);
        for (int i = ID_START; i < RECORDS_PER_TABLE + ID_START; i++) {
            connection.executeWithoutCommitting(
                    "INSERT INTO tablea VALUES(" + i + ", 'a')",
                    "INSERT INTO tableb VALUES(" + i + ", 'b')");
        }
        connection.commit();

        connection.setAutoCommit(true);
        connection.execute("INSERT INTO tableb VALUES(1000, 'b')");

        waitForAvailableRecords();

        // BEGIN, data, END, BEGIN, data, END
        final SourceRecords records = consumeRecordsByTopic(1 + RECORDS_PER_TABLE * 2 + 1 + 3);
        final List<SourceRecord> tableA = records.recordsForTopic("testdb.informix.tablea");
        final List<SourceRecord> tableB = records.recordsForTopic("testdb.informix.tableb");
        final List<SourceRecord> tx = records.recordsForTopic("testdb.transaction");
        assertThat(tableA).hasSize(RECORDS_PER_TABLE);
        assertThat(tableB).hasSize(RECORDS_PER_TABLE + 1);
        assertThat(tx).hasSize(4); // 4

        final List<SourceRecord> all = records.allRecordsInOrder();
        final String txId = assertBeginTransaction(all.get(0));

        for (int i = 1; i <= 2 * RECORDS_PER_TABLE; i++) {
            assertRecordTransactionMetadata(all.get(i), txId, i, (i + 1) / 2);
        }

        assertEndTransaction(all.get(2 * RECORDS_PER_TABLE + 1), txId, 2 * RECORDS_PER_TABLE,
                Collect.hashMapOf("testdb.informix.tablea", RECORDS_PER_TABLE, "testdb.informix.tableb", RECORDS_PER_TABLE));
    }

    @Test
    public void transactionMetadataWithRollback() throws Exception {

        final Configuration config = TestHelper.defaultConfig()
                .with(InformixConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(InformixConnectorConfig.PROVIDE_TRANSACTION_METADATA, true)
                .build();

        start(InformixConnector.class, config);

        assertConnectorIsRunning();

        // Wait for snapshot completion
        waitForSnapshotToBeCompleted(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        consumeRecordsByTopic(1);

        waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        connection.setAutoCommit(false);

        connection.executeWithoutCommitting("INSERT INTO tablea VALUES(1000, 'a')");
        connection.executeWithoutCommitting("INSERT INTO tableb VALUES(1000, 'b')");
        connection.rollback();

        connection.executeWithoutCommitting("INSERT INTO tablea VALUES(1001, 'a')");
        connection.executeWithoutCommitting("INSERT INTO tableb VALUES(1001, 'b')");
        connection.commit();

        waitForAvailableRecords();

        // BEGIN, data, END
        final SourceRecords records = consumeRecordsByTopic(4);
        final List<SourceRecord> tableA = records.recordsForTopic("testdb.informix.tablea");
        final List<SourceRecord> tableB = records.recordsForTopic("testdb.informix.tableb");
        final List<SourceRecord> tx = records.recordsForTopic("testdb.transaction");
        assertThat(tableA).hasSize(1);
        assertThat(tableB).hasSize(1);
        assertThat(tx).hasSize(2);

        final List<SourceRecord> all = records.allRecordsInOrder();
        final String txId = assertBeginTransaction(all.get(0));
        assertEndTransaction(all.get(3), txId, 2, Collect.hashMapOf("testdb.informix.tablea", 1, "testdb.informix.tableb", 1));
    }

    @Override
    protected String assertBeginTransaction(SourceRecord record) {
        final Struct begin = (Struct) record.value();
        final Struct beginKey = (Struct) record.key();
        final Map<String, ?> offset = record.sourceOffset();

        assertThat(begin.getString("status")).isEqualTo("BEGIN");
        assertThat(begin.getInt64("event_count")).isNull();
        final String txId = begin.getString("id");
        assertThat(beginKey.getString("id")).isEqualTo(txId);

        final String expectedId = Arrays.stream(txId.split(":")).findFirst().get();
        assertThat(offset.get("transaction_id")).isEqualTo(expectedId);
        return txId;
    }

    @Override
    protected void assertEndTransaction(SourceRecord record, String beginTxId, long expectedEventCount, Map<String, Number> expectedPerTableCount) {
        final Struct end = (Struct) record.value();
        final Struct endKey = (Struct) record.key();
        final Map<String, ?> offset = record.sourceOffset();
        final String expectedId = Arrays.stream(beginTxId.split(":")).findFirst().get();
        final String expectedTxId = String.format("%s:%s", expectedId, offset.get("commit_lsn"));

        assertThat(end.getString("status")).isEqualTo("END");
        assertThat(end.getString("id")).isEqualTo(expectedTxId);
        assertThat(end.getInt64("event_count")).isEqualTo(expectedEventCount);
        assertThat(endKey.getString("id")).isEqualTo(expectedTxId);

        assertThat(end.getArray("data_collections").stream().map(x -> (Struct) x)
                .collect(Collectors.toMap(x -> x.getString("data_collection"), x -> x.getInt64("event_count"))))
                .isEqualTo(expectedPerTableCount.entrySet().stream().collect(Collectors.toMap(x -> x.getKey(), x -> x.getValue().longValue())));
        assertThat(offset.get("transaction_id")).isEqualTo(expectedId);
    }

    @Override
    protected void assertRecordTransactionMetadata(SourceRecord record, String beginTxId, long expectedTotalOrder, long expectedCollectionOrder) {
        final Struct change = ((Struct) record.value()).getStruct("transaction");
        final Map<String, ?> offset = record.sourceOffset();
        final String expectedId = Arrays.stream(beginTxId.split(":")).findFirst().get();
        final String expectedTxId = String.format("%s:%s", expectedId, offset.get("commit_lsn"));

        assertThat(change.getString("id")).isEqualTo(expectedTxId);
        assertThat(change.getInt64("total_order")).isEqualTo(expectedTotalOrder);
        assertThat(change.getInt64("data_collection_order")).isEqualTo(expectedCollectionOrder);
        assertThat(offset.get("transaction_id")).isEqualTo(expectedId);
    }
}
