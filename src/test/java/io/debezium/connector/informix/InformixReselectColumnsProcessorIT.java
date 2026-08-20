/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.informix.util.TestHelper;
import io.debezium.data.Envelope;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.processors.AbstractReselectProcessorTest;
import io.debezium.processors.reselect.ReselectColumnsPostProcessor;
import io.debezium.util.Testing;

/**
 * @author Chris Cranford
 */
public class InformixReselectColumnsProcessorIT extends AbstractReselectProcessorTest<InformixConnector> {

    private static InformixConnection connection;

    @BeforeAll
    public static void beforeAll() {
        connection = TestHelper.testConnection();
    }

    @BeforeEach
    public void beforeEach() throws Exception {
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
        super.beforeEach();
    }

    @AfterEach
    public void afterEach() throws Exception {
        stopConnector(TestHelper.getLoggingCleanupCallback("dbz4321", "dbz4321_byte", "dbz4321_text"));
        waitForConnectorShutdown(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
        cleanupTestFwkState();
        dropTable();
    }

    @AfterAll
    public static void afterAll() throws SQLException {
        if (connection != null) {
            connection.rollback().close();
        }
    }

    @Override
    protected Class<InformixConnector> getConnectorClass() {
        return InformixConnector.class;
    }

    @Override
    protected JdbcConnection databaseConnection() {
        return connection;
    }

    @Override
    protected Configuration.Builder getConfigurationBuilder() {
        return TestHelper.defaultConfig()
                .with(InformixConnectorConfig.CUSTOM_POST_PROCESSORS, "reselector")
                .with("post.processors.reselector.type", ReselectColumnsPostProcessor.class.getName());
    }

    @Override
    protected String topicName() {
        return "testdb.informix.dbz4321";
    }

    @Override
    protected String tableName() {
        return "dbz4321";
    }

    @Override
    protected String reselectColumnsList() {
        return "informix.dbz4321:data";
    }

    @Override
    protected void createTable() throws Exception {
        connection.execute("CREATE TABLE dbz4321 (id int not null, data varchar(50), data2 int, primary key(id));");
    }

    @Override
    protected void dropTable() throws Exception {
        TestHelper.dropTables("dbz4321", "dbz4321_byte", "dbz4321_text");
    }

    @Override
    protected String getInsertWithValue() {
        return "INSERT INTO dbz4321 (id,data,data2) values (1,'one',1)";
    }

    @Override
    protected String getInsertWithNullValue() {
        return "INSERT INTO dbz4321 (id,data,data2) values (1,null,1)";
    }

    @Override
    protected void waitForStreamingStarted() throws InterruptedException {
        waitForSnapshotToBeCompleted(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
        waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
        waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS);
    }

    @Override
    protected SourceRecords consumeRecordsByTopicReselectWhenNotNullSnapshot() throws InterruptedException {
        waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS);
        return super.consumeRecordsByTopicReselectWhenNotNullSnapshot();
    }

    @Override
    protected SourceRecords consumeRecordsByTopicReselectWhenNotNullStreaming() throws InterruptedException {
        waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS);
        return super.consumeRecordsByTopicReselectWhenNotNullStreaming();
    }

    @Override
    protected SourceRecords consumeRecordsByTopicReselectWhenNullSnapshot() throws InterruptedException {
        waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS);
        return super.consumeRecordsByTopicReselectWhenNullSnapshot();
    }

    @Override
    protected SourceRecords consumeRecordsByTopicReselectWhenNullStreaming() throws InterruptedException {
        waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS);
        return super.consumeRecordsByTopicReselectWhenNullStreaming();
    }

    @Test
    @FixFor("dbz#1766")
    public void testColumnReselectedWhenTextValueIsUnavailable() throws Exception {
        connection.execute("CREATE TABLE dbz4321_text (id int primary key, data text, data2 int);");

        final LogInterceptor reselectLogInterceptor = getReselectLogInterceptor();

        Configuration config = getConfigurationBuilder()
                .with(InformixConnectorConfig.TABLE_INCLUDE_LIST, "testdb.informix.dbz4321_text")
                .build();

        start(getConnectorClass(), config);
        assertConnectorIsRunning();

        waitForStreamingStarted();

        final String text = RandomStringUtils.randomAlphabetic(10000);
        final Clob clob = connection.connection().createClob();
        clob.setString(1, text);

        connection.prepareUpdate("INSERT INTO dbz4321_text (id, data, data2) values (1, ?, 1);",
                ps -> ps.setClob(1, clob)).commit();
        connection.execute("UPDATE dbz4321_text SET data2 = 2 where id = 1;");

        final SourceRecords sourceRecords = consumeRecordsByTopic(2);
        final List<SourceRecord> tableRecords = sourceRecords.recordsForTopic("testdb.informix.dbz4321_text");

        assertThat(tableRecords).isNotNull().hasSize(2);

        // Check insert
        SourceRecord record = tableRecords.get(0);
        Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        VerifyRecord.isValidInsert(record, "id", 1);
        assertThat(after.get("id")).isEqualTo(1);
        assertThat(after.get("data")).isEqualTo(text);
        assertThat(after.get("data2")).isEqualTo(1);

        // Check update
        record = tableRecords.get(1);
        after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        VerifyRecord.isValidUpdate(record, "id", 1);
        assertThat(after.get("id")).isEqualTo(1);
        assertThat(after.get("data")).isEqualTo(text);
        assertThat(after.get("data2")).isEqualTo(2);

        assertColumnReselectedForUnavailableValue(reselectLogInterceptor, "testdb.informix.dbz4321_text", "data");
    }

    @Test
    @FixFor("dbz#1766")
    public void testColumnReselectedWhenByteValueIsUnavailable() throws Exception {
        connection.execute("CREATE TABLE dbz4321_byte (id int primary key, data byte, data2 int);");

        final LogInterceptor reselectLogInterceptor = getReselectLogInterceptor();

        Configuration config = getConfigurationBuilder()
                .with(InformixConnectorConfig.TABLE_INCLUDE_LIST, "testdb.informix.dbz4321_byte")
                .build();

        start(getConnectorClass(), config);
        assertConnectorIsRunning();

        waitForStreamingStarted();

        final String text = RandomStringUtils.randomAlphabetic(10000);
        final Blob blob = connection.connection().createBlob();
        blob.setBytes(0, text.getBytes());
        ByteBuffer wrapped = ByteBuffer.wrap(text.getBytes());

        connection.prepareUpdate("INSERT INTO dbz4321_byte (id, data, data2) values (1, ?, 1);",
                ps -> ps.setBlob(1, blob)).commit();
        connection.execute("UPDATE dbz4321_byte SET data2 = 2 where id = 1;");

        final SourceRecords sourceRecords = consumeRecordsByTopic(2);
        final List<SourceRecord> tableRecords = sourceRecords.recordsForTopic("testdb.informix.dbz4321_byte");

        assertThat(tableRecords).isNotNull().hasSize(2);

        // Check insert
        SourceRecord record = tableRecords.get(0);
        Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        VerifyRecord.isValidInsert(record, "id", 1);
        assertThat(after.get("id")).isEqualTo(1);
        assertThat(after.get("data")).isEqualTo(wrapped);
        assertThat(after.get("data2")).isEqualTo(1);

        // Check update
        record = tableRecords.get(1);
        after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        VerifyRecord.isValidUpdate(record, "id", 1);
        assertThat(after.get("id")).isEqualTo(1);
        assertThat(after.get("data")).isEqualTo(wrapped);
        assertThat(after.get("data2")).isEqualTo(2);

        assertColumnReselectedForUnavailableValue(reselectLogInterceptor, "testdb.informix.dbz4321_byte", "data");
    }
}
