/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;

import io.debezium.config.Configuration;
import io.debezium.connector.informix.InformixConnectorConfig.SnapshotMode;
import io.debezium.connector.informix.util.TestHelper;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.ConditionalFailExtension;
import io.debezium.junit.Flaky;
import io.debezium.transforms.outbox.AbstractEventRouterTest;
import io.debezium.transforms.outbox.EventRouter;

/**
 * An integration test for Informix and the {@link EventRouter} for outbox.
 *
 * @author Chris Cranford, Lars M Johansson
 */
@Flaky("DBZ-8114")
@ExtendWith(ConditionalFailExtension.class)
public class OutboxEventRouterIT extends AbstractEventRouterTest<InformixConnector> {

    private static final String SETUP_OUTBOX_TABLE = "CREATE TABLE outbox (" +
            "id varchar(64) not null primary key, " +
            "aggregatetype varchar(255) not null, " +
            "aggregateid varchar(255) not null, " +
            "type varchar(255) not null, " +
            "payload lvarchar(4000))";

    private InformixConnection connection;

    @BeforeEach
    @Override
    public void beforeEach() throws Exception {
        connection = TestHelper.testConnection();

        initializeConnectorTestFramework();
        Files.delete(TestHelper.SCHEMA_HISTORY_PATH);

        super.beforeEach();
    }

    @AfterEach
    public void afterEach() throws Exception {
        stopConnector();
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
        waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS);
    }

    @Override
    protected void alterTableWithExtra4Fields() throws Exception {
        connection.execute("ALTER TABLE informix.outbox add version integer not null");
        connection.execute("ALTER TABLE informix.outbox add somebooltype boolean not null");
        connection.execute("ALTER TABLE informix.outbox add createdat datetime year to fraction not null");
        connection.execute("ALTER TABLE informix.outbox add is_deleted boolean default 'f'");
    }

    @Override
    protected void alterTableWithTimestampField() throws Exception {
        connection.execute("ALTER TABLE informix.outbox add createdat datetime year to fraction not null");
    }

    @Override
    protected void alterTableModifyPayload() throws Exception {
        connection.execute("ALTER TABLE informix.outbox modify (payload lvarchar(1000))");
    }

    @Override
    protected String getAdditionalFieldValues(boolean deleted) {
        return ", 1, 't', DATETIME(2019-03-24 20:52:59) YEAR TO FRACTION, " + (deleted ? "'t'" : "'f'");
    }

    @Override
    protected String getAdditionalFieldValuesTimestampOnly() {
        return ", DATETIME(2019-03-24 20:52:59) YEAR TO FRACTION";
    }

}
