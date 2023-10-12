/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import io.debezium.config.Configuration.Builder;
import io.debezium.connector.informix.InformixConnectorConfig.SnapshotMode;
import io.debezium.connector.informix.util.TestHelper;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.SkipTestRule;
import io.debezium.pipeline.AbstractBlockingSnapshotTest;
import io.debezium.util.Testing;

import lombok.SneakyThrows;

public class BlockingSnapshotIT extends AbstractBlockingSnapshotTest {

    private InformixConnection connection;

    @Rule
    public SkipTestRule skipRule = new SkipTestRule();

    @Before
    public void before() throws SQLException {
        connection = TestHelper.testConnection();
        connection.execute(
                "CREATE TABLE a (pk int not null, aa int, primary key (pk))",
                "CREATE TABLE b (pk int not null, aa int, primary key (pk))",
                "CREATE TABLE debezium_signal (id varchar(64), type varchar(32), data varchar(255))");
        initializeConnectorTestFramework();
        Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
        Testing.Print.enable();
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
                            "DROP TABLE a",
                            "DROP TABLE b",
                            "DROP TABLE debezium_signal")
                    .close();
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
    protected String tableDataCollectionId() {
        return TestHelper.TEST_DATABASE + '.' + tableName();
    }

    @Override
    protected List<String> tableDataCollectionIds() {
        return tableNames().stream().map(name -> TestHelper.TEST_DATABASE + '.' + name).collect(Collectors.toList());
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
    protected String signalTableName() {
        return "debezium_signal";
    }

    @Override
    protected Builder config() {
        return TestHelper.defaultConfig()
                .with(InformixConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(InformixConnectorConfig.SIGNAL_DATA_COLLECTION, "testdb.informix.debezium_signal")
                .with(InformixConnectorConfig.TABLE_INCLUDE_LIST, "testdb.informix.a,testdb.informix.b")
                .with(InformixConnectorConfig.SNAPSHOT_MODE_TABLES, "testdb.informix.a")
                .with(InformixConnectorConfig.INCREMENTAL_SNAPSHOT_CHUNK_SIZE, 250);
    }

    @Override
    protected Builder mutableConfig(boolean signalTableOnly, boolean storeOnlyCapturedDdl) {
        return config();
    }

    @Override
    protected String connector() {
        return "informix_server";
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
    @Ignore
    @Override
    @SneakyThrows
    public void readsSchemaOnlyForSignaledTables() {
        super.readsSchemaOnlyForSignaledTables();
    }
}
