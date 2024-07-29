/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import org.apache.kafka.connect.data.Schema;
import org.junit.After;
import org.junit.Before;

import io.debezium.config.Configuration;
import io.debezium.connector.informix.InformixConnectorConfig.SnapshotMode;
import io.debezium.connector.informix.util.TestHelper;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.transforms.outbox.AbstractEventRouterTest;
import io.debezium.transforms.outbox.EventRouter;

/**
 * An integration test for Informix and the {@link EventRouter} for outbox.
 *
 * @author Chris Cranford, Lars M Johansson
 */
public class OutboxEventRouterIT extends AbstractEventRouterTest<InformixConnector> {

    private static final String SETUP_OUTBOX_TABLE = "CREATE TABLE outbox (" +
            "id varchar(64) not null primary key, " +
            "aggregatetype varchar(255) not null, " +
            "aggregateid varchar(255) not null, " +
            "type varchar(255) not null, " +
            "payload lvarchar(4000))";

    private InformixConnection connection;

    @Before
    @Override
    public void beforeEach() throws Exception {
        connection = TestHelper.testConnection();

        initializeConnectorTestFramework();
        Files.delete(TestHelper.SCHEMA_HISTORY_PATH);

        super.beforeEach();
    }

    @After
    @Override
    public void afterEach() throws Exception {
        super.afterEach();
        waitForConnectorShutdown(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
        assertConnectorNotRunning();
        if (connection != null && connection.isConnected()) {
            connection.rollback();
            TestHelper.dropTable(connection, tableName());
            connection.close();
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
    protected Configuration.Builder getConfigurationBuilder(boolean initialSnapshot) {
        final SnapshotMode snapshotMode = initialSnapshot ? SnapshotMode.INITIAL : SnapshotMode.NO_DATA;
        return TestHelper.defaultConfig()
                .with(InformixConnectorConfig.SNAPSHOT_MODE, snapshotMode.getValue())
                .with(InformixConnectorConfig.TABLE_INCLUDE_LIST, TestHelper.TEST_DATABASE + "." + tableName());
    }

    @Override
    protected String getSchemaNamePrefix() {
        return "testdb.informix.outbox.";
    }

    @Override
    protected Schema getPayloadSchema() {
        return Schema.OPTIONAL_STRING_SCHEMA;
    }

    @Override
    protected String tableName() {
        return "informix.outbox";
    }

    @Override
    protected String topicName() {
        return TestHelper.TEST_DATABASE + ".informix.outbox";
    }

    @Override
    protected void createTable() throws Exception {
        TestHelper.dropTable(connection, tableName());
        connection.execute(SETUP_OUTBOX_TABLE);
    }

    @Override
    protected String createInsert(String eventId,
                                  String eventType,
                                  String aggregateType,
                                  String aggregateId,
                                  String payloadJson,
                                  String additional) {
        StringBuilder insert = new StringBuilder();
        insert.append("INSERT INTO informix.outbox VALUES (");
        insert.append("'").append(eventId).append("', ");
        insert.append("'").append(aggregateType).append("', ");
        insert.append("'").append(aggregateId).append("', ");
        insert.append("'").append(eventType).append("', ");
        if (payloadJson != null) {
            insert.append("'").append(payloadJson).append("'");
        }
        else {
            insert.append("NULL");
        }
        if (additional != null) {
            insert.append(additional);
        }
        insert.append(")");
        return insert.toString();
    }

    @Override
    protected void waitForSnapshotCompleted() throws InterruptedException {
        waitForSnapshotToBeCompleted(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
    }

    @Override
    protected void waitForStreamingStarted() throws InterruptedException {
        waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
        waitForAvailableRecords();
    }

    @Override
    protected void alterTableWithExtra4Fields() throws Exception {
        stopConnector();
        waitForConnectorShutdown(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        connection.execute("ALTER TABLE informix.outbox add version integer not null");
        connection.execute("ALTER TABLE informix.outbox add somebooltype boolean not null");
        connection.execute("ALTER TABLE informix.outbox add createdat datetime year to fraction not null");
        connection.execute("ALTER TABLE informix.outbox add is_deleted boolean default 'f'");

        startConnectorWithNoSnapshot();
    }

    @Override
    protected void alterTableWithTimestampField() throws Exception {
        stopConnector();
        waitForConnectorShutdown(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
        connection.execute("ALTER TABLE informix.outbox add createdat datetime year to fraction not null");
        startConnectorWithNoSnapshot();
    }

    @Override
    protected void alterTableModifyPayload() throws Exception {
        stopConnector();
        waitForConnectorShutdown(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
        connection.execute("ALTER TABLE informix.outbox modify (payload lvarchar(1000))");
        startConnectorWithNoSnapshot();
    }

    @Override
    protected String getAdditionalFieldValues(boolean deleted) {
        return ", 1, 't', DATETIME(2019-03-24 20:52:59) YEAR TO FRACTION, " + (deleted ? "'t'" : "'f'");
    }

    @Override
    protected String getAdditionalFieldValuesTimestampOnly() {
        return ", DATETIME(2019-03-24 20:52:59) YEAR TO FRACTION";
    }

    private void startConnectorWithNoSnapshot() throws Exception {
        start(getConnectorClass(), getConfigurationBuilder(false).build());
        assertConnectorIsRunning();
        waitForStreamingStarted();
        assertNoRecordsToConsume();
    }
}
