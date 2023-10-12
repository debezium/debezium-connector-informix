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

import io.debezium.config.Configuration;
import io.debezium.connector.informix.InformixConnectorConfig.SnapshotMode;
import io.debezium.connector.informix.util.TestHelper;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.util.Collect;

/**
 * Transaction metadata test for the Debezium Informix Server connector.
 *
 */
public class TransactionMetadataIT extends AbstractConnectorTest {

    private InformixConnection connection;

    @Before
    public void before() throws SQLException {
        connection = TestHelper.testConnection();
        connection.execute(
                "CREATE TABLE tablea (id int not null, cola varchar(30), primary key (id))",
                "CREATE TABLE tableb (id int not null, colb varchar(30), primary key (id))")
                .execute("INSERT INTO tablea VALUES(1, 'a')");

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
        final Configuration config = TestHelper.defaultConfig().with(InformixConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(InformixConnectorConfig.PROVIDE_TRANSACTION_METADATA, true).build();

        start(InformixConnector.class, config);

        assertConnectorIsRunning();

        // Wait for snapshot completion
        waitForSnapshotToBeCompleted(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        consumeRecordsByTopic(1);

        connection.setAutoCommit(false);
        for (int i = ID_START; i < RECORDS_PER_TABLE + ID_START; i++) {
            connection.executeWithoutCommitting("INSERT INTO tablea VALUES(" + i + ", 'a')", "INSERT INTO tableb VALUES(" + i + ", 'b')");
        }
        connection.commit();

        connection.setAutoCommit(true);
        connection.execute("INSERT INTO tableb VALUES(1000, 'b')");

        waitForAvailableRecords(30, TimeUnit.SECONDS);

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
}
