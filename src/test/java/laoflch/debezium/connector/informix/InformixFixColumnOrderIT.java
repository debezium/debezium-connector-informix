/*
 * Copyright Debezium-Informix-Connector Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package laoflch.debezium.connector.informix;

import static laoflch.debezium.connector.informix.InformixConnectorConfig.SNAPSHOT_MODE;
import static laoflch.debezium.connector.informix.InformixConnectorConfig.SnapshotMode.INITIAL_SCHEMA_ONLY;
import static laoflch.debezium.connector.informix.util.TestHelper.TEST_DATABASE;
import static org.fest.assertions.Assertions.assertThat;

import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.data.VerifyRecord;
import io.debezium.embedded.AbstractConnectorTest;

import laoflch.debezium.connector.informix.util.TestHelper;

public class InformixFixColumnOrderIT extends AbstractConnectorTest {

    private InformixConnection connection;
    private static final String testTableName = "test_table";
    private static final Map<String, String> testTableColumns = new LinkedHashMap<String, String>() {
        {
            put("id", "int");
            put("name", "varchar(50)");
            put("age", "int");
            put("gender", "char(10)");
            put("address", "varchar(50)");
        }
    };

    private static final Logger LOGGER = LoggerFactory.getLogger(InformixFixColumnOrderIT.class);

    @Before
    public void before() throws SQLException {

        connection = TestHelper.testConnection();

        List<String> columnStringList = new LinkedList<String>() {
            {
                testTableColumns.forEach((column, type) -> add(column + " " + type));
            }
        };
        connection.execute(String.format("create table if not exists %s(%s)", testTableName,
                String.join(", ", columnStringList)));
        connection.execute(String.format("truncate table %s", testTableName));

        initializeConnectorTestFramework();
        Files.delete(TestHelper.DB_HISTORY_PATH);
        Print.enable();
    }

    @After
    public void after() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    public void testColumnOrderWhileInsert() throws Exception {

        final Configuration config = TestHelper.defaultConfig()
                .with(SNAPSHOT_MODE, INITIAL_SCHEMA_ONLY)
                .build();

        start(InformixConnector.class, config);

        /*
         * Wait InformixStreamingChangeEventSource.execute() is running.
         */
        Thread.sleep(60_000);

        // insert a record
        Map<String, String> recordToBeInsert = new LinkedHashMap<String, String>() {
            {
                put("id", "1");
                put("name", "cc");
                put("age", "18");
                put("gender", "male");
                put("address", "ff:ff:ff:ff:ff:ff");
            }
        };
        connection.execute(String.format("insert into %s(%s) values(\"%s\")", testTableName,
                String.join(", ", recordToBeInsert.keySet()),
                String.join("\", \"", recordToBeInsert.values())));

        String topicName = String.format("%s.informix.%s", TEST_DATABASE, testTableName);
        SourceRecords sourceRecords = consumeRecordsByTopic(1);
        List<SourceRecord> insertOne = sourceRecords.recordsForTopic(topicName);
        assertThat(insertOne).isNotNull();
        assertThat(insertOne).hasSize(1);

        final SourceRecord insertedOneRecord = insertOne.get(0);
        final Struct insertedOneValue = (Struct) insertedOneRecord.value();

        VerifyRecord.isValidInsert(insertedOneRecord);
        assertRecordInRightOrder((Struct) insertedOneValue.get("after"), recordToBeInsert);

        stopConnector();
    }

    @Test
    public void testColumnOrderWhileUpdate() throws Exception {

        // insert a record for testing update
        Map<String, String> recordToBeUpdate = new LinkedHashMap<String, String>() {
            {
                put("id", "2");
                put("name", "cc");
                put("age", "18");
                put("gender", "male");
                put("address", "ff:ff:ff:ff:ff:ff");
            }
        };
        connection.execute(String.format("insert into %s(%s) values(\"%s\")", testTableName,
                String.join(", ", recordToBeUpdate.keySet()),
                String.join("\", \"", recordToBeUpdate.values())));

        final Configuration config = TestHelper.defaultConfig()
                .with(SNAPSHOT_MODE, INITIAL_SCHEMA_ONLY)
                .build();

        start(InformixConnector.class, config);

        /*
         * Wait InformixStreamingChangeEventSource.execute() is running.
         */
        Thread.sleep(60_000);

        Map<String, String> recordAfterUpdate = new LinkedHashMap<>(recordToBeUpdate);
        // new value
        recordAfterUpdate.put("address", "00:00:00:00:00:00");

        // update
        connection.execute(String.format("update %s set address = \"%s\" where id = \"%s\"", testTableName,
                recordAfterUpdate.get("address"), recordToBeUpdate.get("id")));

        String topicName = String.format("%s.informix.%s", TEST_DATABASE, testTableName);
        SourceRecords sourceRecords = consumeRecordsByTopic(1);
        List<SourceRecord> updateOne = sourceRecords.recordsForTopic(topicName);
        assertThat(updateOne).isNotNull();
        assertThat(updateOne).hasSize(1);

        final SourceRecord updatedOneRecord = updateOne.get(0);
        final Struct updatedOneValue = (Struct) updatedOneRecord.value();

        VerifyRecord.isValidUpdate(updatedOneRecord);

        // assert in order
        assertRecordInRightOrder((Struct) updatedOneValue.get("before"), recordToBeUpdate);
        assertRecordInRightOrder((Struct) updatedOneValue.get("after"), recordAfterUpdate);

        stopConnector();
    }

    @Test
    public void testColumnOrderWhileDelete() throws Exception {

        // insert a record to delete
        Map<String, String> recordToBeDelete = new LinkedHashMap<String, String>() {
            {
                put("id", "3");
                put("name", "cc");
                put("age", "18");
                put("gender", "male");
                put("address", "ff:ff:ff:ff:ff:ff");
            }
        };
        connection.execute(String.format("insert into %s(%s) values(\"%s\")", testTableName,
                String.join(", ", recordToBeDelete.keySet()),
                String.join("\", \"", recordToBeDelete.values())));

        final Configuration config = TestHelper.defaultConfig()
                .with(SNAPSHOT_MODE, INITIAL_SCHEMA_ONLY)
                .build();

        start(InformixConnector.class, config);

        /*
         * Wait InformixStreamingChangeEventSource.execute() is running.
         */
        Thread.sleep(60_000);

        connection.execute(String.format("delete from %s where id = \"%s\"", testTableName, recordToBeDelete.get("id")));

        String topicName = String.format("%s.informix.%s", TEST_DATABASE, testTableName);
        SourceRecords sourceRecords = consumeDeletedRecordsByTopic(1, topicName);
        List<SourceRecord> deletedRecords = sourceRecords.recordsForTopic(topicName);

        assertThat(deletedRecords).isNotNull();
        assertThat(deletedRecords).hasSize(1);

        final SourceRecord deletedOneRecord = deletedRecords.get(0);
        final Struct deletedOneValue = (Struct) deletedOneRecord.value();

        VerifyRecord.isValidDelete(deletedOneRecord);

        // assert in order
        assertRecordInRightOrder((Struct) deletedOneValue.get("before"), recordToBeDelete);

        stopConnector();
    }

    /**
     * ref: InformixCdcDelete.java -> consumeDeletedRecordsByTopic
     * TODO: Follow DRY principle.
     */
    private SourceRecords consumeDeletedRecordsByTopic(int numRecords, String topicName)
            throws InterruptedException {
        SourceRecords records = consumeRecordsByTopic(0);

        while (numRecords > 0) {
            records.add(consumeRecordsByTopic(1).recordsForTopic(topicName).get(0));
            consumeRecordsByTopic(1, false);
            numRecords--;
        }

        return records;
    }

    public static void assertRecordInRightOrder(Struct record, Map<String, String> recordToBeCheck) {
        recordToBeCheck.keySet().forEach(field -> assertThat(record.get(field).toString().trim()).isEqualTo(recordToBeCheck.get(field)));
    }
}
