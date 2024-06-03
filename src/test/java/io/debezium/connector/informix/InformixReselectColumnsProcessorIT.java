/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import org.junit.After;
import org.junit.Before;

import io.debezium.config.Configuration;
import io.debezium.connector.informix.util.TestHelper;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.processors.AbstractReselectProcessorTest;
import io.debezium.processors.reselect.ReselectColumnsPostProcessor;
import io.debezium.util.Testing;

/**
 * @author Chris Cranford
 */
public class InformixReselectColumnsProcessorIT extends AbstractReselectProcessorTest<InformixConnector> {

    private InformixConnection connection;

    @Before
    public void beforeEach() throws Exception {
        connection = TestHelper.testConnection();
        connection.setAutoCommit(false);

        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
        super.beforeEach();
    }

    @After
    public void afterEach() throws Exception {
        stopConnector();
        waitForConnectorShutdown(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
        assertConnectorNotRunning();

        super.afterEach();
        if (connection != null) {
            connection.rollback()
                    .execute("DROP TABLE dbz4321")
                    .close();
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
                .with("reselector.type", ReselectColumnsPostProcessor.class.getName());
    }

    @Override
    protected String topicName() {
        return "testdb.informix.dbz4321";
    }

    @Override
    protected String tableName() {
        return "informix.dbz4321";
    }

    @Override
    protected String reselectColumnsList() {
        return "informix.dbz4321:data";
    }

    @Override
    protected void createTable() throws Exception {
        connection.execute("DROP TABLE IF EXISTS DBZ4321");
        connection.execute("CREATE TABLE DBZ4321 (id int not null, data varchar(50), data2 int, primary key(id))");
    }

    @Override
    protected void dropTable() throws Exception {
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
    }

    protected SourceRecords consumeRecordsByTopicReselectWhenNullStreaming() throws InterruptedException {
        waitForAvailableRecords();
        return super.consumeRecordsByTopicReselectWhenNullStreaming();
    }

    protected SourceRecords consumeRecordsByTopicReselectWhenNotNullStreaming() throws InterruptedException {
        waitForAvailableRecords();
        return super.consumeRecordsByTopicReselectWhenNotNullStreaming();
    }
}
