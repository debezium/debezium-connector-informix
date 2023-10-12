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
import java.util.Arrays;
import java.util.List;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.data.Envelope;
import io.debezium.data.VerifyRecord;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.util.Testing;

import io.debezium.connector.informix.util.TestHelper;

public class InformixCdcDeleteIT extends AbstractConnectorTest {

    private InformixConnection connection;

    private static final Logger LOGGER = LoggerFactory.getLogger(InformixCdcDeleteIT.class);

    private static final String testDeleteTableName = "test_delete";
    private static final String[][] testTableInitValues = {
            { "A", "a" }, { "B", "b" },
            { "C", "c1" }, { "C", "c2" },
            { "C", "c3" }, { "C", "c4" }
    };

    @Before
    public void before() throws SQLException {
        connection = TestHelper.testConnection();

        connection.execute(String.format("create table if not exists %s(col1 varchar, col2 varchar)", testDeleteTableName));
        connection.execute(String.format("truncate table %s", testDeleteTableName));

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
    public void testTableDeleteByCondition() throws Exception {

        prepareTestTableData();

        final Configuration config = TestHelper.defaultConfig()
                .with(SNAPSHOT_MODE, INITIAL_SCHEMA_ONLY)
                .build();

        start(InformixConnector.class, config);

        /*
         * Wait InformixStreamingChangeEventSource.execute() is running.
         */
        Thread.sleep(60_000);

        // delete row by row
        deleteByConditionAndValidate(testDeleteTableName, "col1", "A");
        deleteByConditionAndValidate(testDeleteTableName, "col1", "B");
        // delete with multiply records
        deleteByConditionAndValidate(testDeleteTableName, "col1", "C");

        stopConnector();
    }

    @Test
    public void testTableDeleteAll() throws Exception {

        prepareTestTableData();

        final Configuration config = TestHelper.defaultConfig()
                .with(SNAPSHOT_MODE, INITIAL_SCHEMA_ONLY)
                .build();

        start(InformixConnector.class, config);

        /*
         * Wait InformixStreamingChangeEventSource.execute() is running.
         */
        Thread.sleep(60_000);

        deleteAllAndValidate(testDeleteTableName);

        stopConnector();
    }

    // prepare data for test delete table
    private void prepareTestTableData() throws Exception {
        connection.execute(String.format("truncate table %s", testDeleteTableName));

        for (String[] tuple : testTableInitValues) {
            connection.execute(String.format("insert into %s values(\"%s\", \"%s\")", testDeleteTableName, tuple[0], tuple[1]));
        }
    }

    // Caution: "A database DELETE operation causes Debezium to generate two Kafka records"
    // @see https://debezium.io/documentation/reference/nightly/transformations/event-flattening.html#event-flattening-behavior
    private SourceRecords consumeDeletedRecordsByTopic(int numRecords, String topicName) throws InterruptedException {
        SourceRecords records = consumeRecordsByTopic(0);

        while (numRecords > 0) {
            records.add(consumeRecordsByTopic(1).recordsForTopic(topicName).get(0));
            consumeRecordsByTopic(1, false);
            numRecords--;
        }

        return records;
    }

    private void deleteByConditionAndValidate(String tableName, String column, String deleteValue) throws SQLException, InterruptedException {
        String topicName = String.format("testdb.informix.%s", tableName);
        connection.execute(String.format("delete from %s where %s = \"%s\"", tableName, column, deleteValue));

        // count tuples count should be deleted
        int deletedNum = (int) Arrays.stream(testTableInitValues).filter(tuple -> tuple[0].equals(deleteValue)).count();

        SourceRecords sourceRecords = consumeDeletedRecordsByTopic(deletedNum, topicName);
        List<SourceRecord> deletedRecords = sourceRecords.recordsForTopic(topicName);

        assertThat(deletedRecords).isNotNull();
        assertThat(deletedRecords).hasSize(deletedNum);

        assertDeletedRecords(deletedRecords, column, deleteValue);
    }

    private static void assertDeletedRecords(List<SourceRecord> deletedRecords, String column, String deleteValue) {
        deletedRecords.forEach(deletedRecord -> {
            Struct value = (Struct) deletedRecord.value();
            // valid deleted record struct
            VerifyRecord.isValidDelete(deletedRecord);
            // valid deleted column and value
            assertThat(value.getStruct(Envelope.FieldName.BEFORE).getString(column)).isEqualTo(deleteValue);
        });
    }

    private void deleteAllAndValidate(String tableName) throws SQLException, InterruptedException {
        String topicName = String.format("testdb.informix.%s", tableName);
        connection.execute(String.format("delete from %s where 1 = 1", tableName));

        // count tuples count should be deleted
        int deletedNum = (int) Arrays.stream(testTableInitValues).count();

        SourceRecords sourceRecords = consumeDeletedRecordsByTopic(deletedNum, topicName);
        List<SourceRecord> deletedRecords = sourceRecords.recordsForTopic(topicName);

        assertThat(deletedRecords).isNotNull();
        assertThat(deletedRecords).hasSize(deletedNum);

        // valid each record
        deletedRecords.forEach(deletedRecord -> {
            VerifyRecord.isValidDelete(deletedRecord);
        });
    }
}
