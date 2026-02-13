/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.config.Configuration.Builder;
import io.debezium.connector.informix.InformixConnectorConfig.SnapshotMode;
import io.debezium.connector.informix.util.TestHelper;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.AbstractBlockingSnapshotTest;
import io.debezium.relational.history.SchemaHistory;

public class BlockingSnapshotIT extends AbstractBlockingSnapshotTest {

    private InformixConnection connection;

    @BeforeEach
    public void before() throws SQLException {
        connection = TestHelper.testConnection();
        TestHelper.dropTables(connection, "a", "b", "debezium_signal");
        connection.execute(
                "CREATE TABLE a (pk int not null, aa int, primary key (pk))",
                "CREATE TABLE b (pk int not null, aa int, primary key (pk))",
                "CREATE TABLE debezium_signal (id varchar(64), type varchar(32), data lvarchar(2048))");
        initializeConnectorTestFramework();
        Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
        Print.disable();
    }

    @AfterEach
    public void after() throws SQLException {
        /*
         * Since all DDL operations are forbidden during Informix CDC,
         * we have to ensure the connector is properly shut down before dropping tables.
         */
        stopConnector();
        waitForConnectorShutdown(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
        assertConnectorNotRunning();
        if (connection != null) {
            connection.rollback();
            TestHelper.dropTables(connection, "a", "b", "debezium_signal");
            connection.close();
        }
    }

    @Override
    protected void waitForConnectorToStart() {
        super.waitForConnectorToStart();
        try {
            waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
        }
        catch (InterruptedException e) {
            throw new DebeziumException(e);
        }
    }

    @Override
    protected Class<InformixConnector> connectorClass() {
        return InformixConnector.class;
    }

    @Override
    protected JdbcConnection databaseConnection() {
        return connection;
    }

    @Override
    protected String topicName() {
        return "testdb.informix.a";
    }

    @Override
    protected List<String> topicNames() {
        return List.of(topicName(), "testdb.informix.b");
    }

    @Override
    protected String tableName() {
        return "informix.a";
    }

    @Override
    protected List<String> tableNames() {
        return List.of(tableName(), "informix.b");
    }

    @Override
    protected String tableDataCollectionId() {
        return TestHelper.TEST_DATABASE + '.' + tableName();
    }

    @Override
    protected String escapedTableDataCollectionId() {
        return "\\\"" + TestHelper.TEST_DATABASE + "\\\".\\\"informix\\\".\\\"a\\\"";
    }

    @Override
    protected List<String> tableDataCollectionIds() {
        return tableNames().stream().map(name -> TestHelper.TEST_DATABASE + '.' + name).collect(Collectors.toList());
    }

    @Override
    protected String signalTableName() {
        return "informix.debezium_signal";
    }

    @Override
    protected String signalTableNameSanitized() {
        return TestHelper.TEST_DATABASE + '.' + signalTableName();
    }

    @Override
    protected Builder config() {
        return TestHelper.defaultConfig()
                .with(InformixConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(InformixConnectorConfig.SIGNAL_DATA_COLLECTION, this::signalTableNameSanitized)
                .with(InformixConnectorConfig.SNAPSHOT_MODE_TABLES, this::tableDataCollectionId);
    }

    @Override
    protected Builder mutableConfig(boolean signalTableOnly, boolean storeOnlyCapturedDdl) {
        return config()
                .with(SchemaHistory.STORE_ONLY_CAPTURED_TABLES_DDL, storeOnlyCapturedDdl);
    }

    @Override
    protected Configuration.Builder historizedMutableConfig(boolean signalTableOnly, boolean storeOnlyCapturedDdl) {
        return mutableConfig(signalTableOnly, storeOnlyCapturedDdl)
                .with(InformixConnectorConfig.INCLUDE_SCHEMA_CHANGES, true);
    }

    @Override
    protected String connector() {
        return TestHelper.TEST_CONNECTOR;
    }

    @Override
    protected String server() {
        return TestHelper.TEST_DATABASE;
    }

    @Override
    protected int insertMaxSleep() {
        return 100;
    }

    @Test
    @Disabled("Informix does not support DDL operations on tables defined for replication")
    @Override
    public void readsSchemaOnlyForSignaledTables() throws Exception {
        super.readsSchemaOnlyForSignaledTables();
    }
}
