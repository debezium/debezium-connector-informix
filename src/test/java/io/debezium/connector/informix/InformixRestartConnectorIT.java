/*
 * Copyright Debezium-Informix-Connector Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.informix;

import static io.debezium.connector.informix.InformixConnectorConfig.SNAPSHOT_MODE;
import static io.debezium.connector.informix.InformixConnectorConfig.SnapshotMode.INITIAL_SCHEMA_ONLY;
import static io.debezium.connector.informix.util.TestHelper.assertRecord;
import static org.fest.assertions.Assertions.assertThat;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.data.SchemaAndValueField;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.util.Testing;

import io.debezium.connector.informix.util.TestHelper;

public class InformixRestartConnectorIT extends AbstractConnectorTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(InformixRestartConnectorIT.class);

    private InformixConnection connection;

    @Before
    public void before() throws SQLException {
        connection = TestHelper.testConnection();

        connection.execute("create table if not exists hello(a integer, b varchar(200))");
        connection.execute("truncate table hello");

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
    public void restartNormal() throws Exception {
        final Configuration config = TestHelper.defaultConfig()
                .with(SNAPSHOT_MODE, INITIAL_SCHEMA_ONLY)
                .build();

        start(InformixConnector.class, config);
        // Wait InformixStreamingChangeEventSource.execute() is running.
        Thread.sleep(60_000);

        connection.execute("insert into testdb:hello values(0, 'hello-0')");

        SourceRecords sourceRecordsBefore = consumeRecordsByTopic(1);
        List<SourceRecord> recordsBefore = sourceRecordsBefore.recordsForTopic("testdb.informix.hello");
        assertThat(recordsBefore).isNotNull();
        assertThat(recordsBefore).hasSize(1);

        stopConnector();

        // After stop the connector
        connection.execute("insert into testdb:hello values(1, 'hello-1')");

        /*
         * Restart the connector and consume data
         */
        start(InformixConnector.class, config);

        // Wait InformixStreamingChangeEventSource.execute() is running.
        Thread.sleep(60_000);

        /*
         * TODO: This extra insertion will help informix push all records immediately. Don't know why.
         * Add this will make testcase more stable.
         */
        connection.execute("insert into testdb:hello values(2, 'hello-2')");

        SourceRecords sourceRecords = consumeRecordsByTopic(1);
        List<SourceRecord> insertOne = sourceRecords.recordsForTopic("testdb.informix.hello");
        assertThat(insertOne).isNotNull();
        assertThat(insertOne).hasSize(1);

        final List<SchemaAndValueField> expectedDeleteRow = Arrays.asList(
                new SchemaAndValueField("a", Schema.OPTIONAL_INT32_SCHEMA, 1),
                new SchemaAndValueField("b", Schema.OPTIONAL_STRING_SCHEMA, "hello-1"));

        final SourceRecord insertOneRecord = insertOne.get(0);
        final Struct insertOneValue = (Struct) insertOneRecord.value();

        assertRecord((Struct) insertOneValue.get("after"), expectedDeleteRow);

        stopConnector();
    }

    @Test
    public void restartWithinTransaction() throws Exception {
        final int RECORD_PER_TABLE = 10;

        final Configuration config = TestHelper.defaultConfig()
                .with(SNAPSHOT_MODE, INITIAL_SCHEMA_ONLY)
                .build();

        connection.setAutoCommit(false);

        start(InformixConnector.class, config);
        // Wait InformixStreamingChangeEventSource.execute() is running.
        Thread.sleep(60_000);

        connection.setAutoCommit(true);
        connection.execute("insert into testdb:hello values(0, 'hello-0')");

        assertRecords("testdb.informix.hello", Collections.singletonList(new TestRecord(0, "hello-0")));

        connection.setAutoCommit(false);
        for (int i = 0; i < RECORD_PER_TABLE; i++) {
            String insertSql = String.format("INSERT INTO testdb:hello VALUES (%d, 'hello-2-%d')", i, i);

            connection.executeWithoutCommitting(insertSql);
        }

        long msBefore = System.currentTimeMillis();
        LOGGER.info("{} uncommitted records is executed, sleep 60 seconds..., ts={}, {}", RECORD_PER_TABLE, new Timestamp(msBefore), msBefore);
        Thread.sleep(60_000);
        long msAfter = System.currentTimeMillis();
        LOGGER.info("Sleep completed, ts={}, {}", new Timestamp(msAfter), msAfter);

        stopConnector();

        // After stop the connector
        connection.executeWithoutCommitting("insert into testdb:hello values(2, 'hello-2')");
        connection.connection().commit();
        connection.setAutoCommit(true);

        /*
         * Restart the connector and consume data
         */
        start(InformixConnector.class, config);

        // Wait InformixStreamingChangeEventSource.execute() is running.
        Thread.sleep(60_000);

        /*
         * TODO: This extra insertion will help informix push all records immediately. Don't know why.
         * Add this will make testcase more stable.
         */
        connection.execute("insert into testdb:hello values(999, 'hello-999')");
        List<TestRecord> expectRecordValue = new ArrayList<>();
        for (int i = 0; i < RECORD_PER_TABLE; i++) {
            expectRecordValue.add(new TestRecord(i, String.format("hello-2-%d", i)));
        }
        expectRecordValue.add(new TestRecord(2, "hello-2"));
        assertRecords("testdb.informix.hello", expectRecordValue);

        stopConnector();
    }

    public void assertRecords(String topicName, List<TestRecord> records) throws InterruptedException {
        SourceRecords sourceRecords = consumeRecordsByTopic(records.size());
        List<SourceRecord> insertOne = sourceRecords.recordsForTopic(topicName);
        assertThat(insertOne).isNotNull();
        assertThat(insertOne).hasSize(records.size());

        for (int i = 0; i < records.size(); i++) {
            final List<SchemaAndValueField> expectedRow = Arrays.asList(
                    new SchemaAndValueField("a", Schema.OPTIONAL_INT32_SCHEMA, records.get(i).a),
                    new SchemaAndValueField("b", Schema.OPTIONAL_STRING_SCHEMA, records.get(i).b));

            final SourceRecord record = insertOne.get(i);
            final Struct recordValue = (Struct) record.value();

            assertRecord((Struct) recordValue.get("after"), expectedRow);
        }
    }

    public static class TestRecord {
        final public int a;
        final public String b;

        public TestRecord(int a, String b) {
            this.a = a;
            this.b = b;
        }

        public int getA() {
            return a;
        }

        public String getB() {
            return b;
        }
    }
}
