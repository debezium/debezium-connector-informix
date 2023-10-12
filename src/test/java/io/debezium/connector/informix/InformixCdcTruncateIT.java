/*
 * Copyright Debezium-Informix-Connector Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.informix;

import static io.debezium.connector.informix.InformixConnectorConfig.SNAPSHOT_MODE;
import static io.debezium.connector.informix.InformixConnectorConfig.SnapshotMode.INITIAL_SCHEMA_ONLY;
import static org.fest.assertions.Assertions.assertThat;

import java.sql.SQLException;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.data.Envelope;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.util.Testing;

import io.debezium.connector.informix.util.TestHelper;

public class InformixCdcTruncateIT extends AbstractConnectorTest {

    private InformixConnection connection;

    private static final String testTruncateTableName = "test_truncate";

    @Before
    public void before() throws SQLException {
        connection = TestHelper.testConnection();

        connection.execute(String.format("create table if not exists %s(a varchar)", testTruncateTableName));
        connection.execute(String.format("truncate table %s", testTruncateTableName));

        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.DB_HISTORY_PATH);
        Testing.Print.enable();
    }

    @After
    public void after() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    public void testTableTruncate() throws Exception {

        // prepare some data for truncate table
        String[] initValues = { "a", "b", "c" };
        for (String insertValue : initValues) {
            connection.execute(String.format("insert into %s values(\"%s\")", testTruncateTableName, insertValue));
        }

        final Configuration config = TestHelper.defaultConfig()
                .with(SNAPSHOT_MODE, INITIAL_SCHEMA_ONLY)
                .build();

        start(InformixConnector.class, config);

        /*
         * Wait InformixStreamingChangeEventSource.execute() is running.
         */
        Thread.sleep(60_000);

        truncateAndValidate(testTruncateTableName);

        stopConnector();
    }

    private void truncateAndValidate(String tableName) throws SQLException, InterruptedException {
        String topicName = String.format("testdb.informix.%s", tableName);
        connection.execute(String.format("truncate table %s", tableName));

        SourceRecords sourceRecords = consumeRecordsByTopic(1);
        List<SourceRecord> truncateTable = sourceRecords.recordsForTopic(topicName);
        assertThat(truncateTable).isNotNull();
        assertThat(truncateTable).hasSize(1);

        final SourceRecord truncateTableRecord = truncateTable.get(0);

        Schema schema = truncateTableRecord.keySchema();
        // Note: VerifyRecord.isValidTruncate with bug. See issue(https://github.com/debezium/debezium/pull/2587)
        // VerifyRecord.isValidTruncate(truncateTableRecord);
        isValidTruncate(truncateTableRecord);
    }

    /**
     * Verify that the given {@link SourceRecord} is a {@link Envelope.Operation#TRUNCATE TRUNCATE} record.
     *
     * @param record the source record; may not be null
     */
    private static void isValidTruncate(SourceRecord record) {
        assertThat(record.key()).isNull();
        assertThat(record.keySchema()).isNull();
        Struct value = (Struct) record.value();
        assertThat(value).isNotNull();
        assertThat(value.getString(Envelope.FieldName.OPERATION)).isEqualTo(Envelope.Operation.TRUNCATE.code());
        assertThat(value.get(Envelope.FieldName.BEFORE)).isNull();
        assertThat(value.get(Envelope.FieldName.AFTER)).isNull();
    }

}
