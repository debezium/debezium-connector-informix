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
import org.junit.Test;

import io.debezium.config.Configuration.Builder;
import io.debezium.connector.informix.InformixConnectorConfig.SnapshotMode;
import io.debezium.connector.informix.util.TestHelper;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.source.snapshot.incremental.AbstractIncrementalSnapshotTest;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.history.SchemaHistory;

public class IncrementalSnapshotIT extends AbstractIncrementalSnapshotTest<InformixConnector> {

    private InformixConnection connection;

    @Before
    public void before() throws SQLException {
        connection = TestHelper.testConnection();
        connection.execute(
                "DROP TABLE IF EXISTS a",
                "DROP TABLE IF EXISTS b",
                "DROP TABLE IF EXISTS c",
                "DROP TABLE IF EXISTS debezium_signal",
                "CREATE TABLE a (pk int not null, aa int, primary key (pk))",
                "CREATE TABLE b (pk int not null, aa int, primary key (pk))",
                "CREATE TABLE c (pk1 int, pk2 int, pk3 int, pk4 int, aa int)",
                "CREATE TABLE debezium_signal (id varchar(64), type varchar(32), data varchar(255))");
        initializeConnectorTestFramework();
        Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
        Print.disable();
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
                            "DROP TABLE c",
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
    protected String noPKTopicName() {
        return "testdb.informix.c";
    }

    @Override
    protected String tableDataCollectionId() {
        return TestHelper.TEST_DATABASE + '.' + tableName();
    }

    @Override
    protected List<String> tableDataCollectionIds() {
        return tableNames().stream().map(name -> TestHelper.TEST_DATABASE + '.' + name).collect(Collectors.toList());
    }

    protected String tableIncludeList() {
        return String.join(",", tableDataCollectionIds());
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
    protected String noPKTableName() {
        return "informix.c";
    }

    @Override
    protected String noPKTableDataCollectionId() {
        return TestHelper.TEST_DATABASE + "." + noPKTableName();
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
                .with(InformixConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(InformixConnectorConfig.SIGNAL_DATA_COLLECTION, this::signalTableNameSanitized)
                .with(InformixConnectorConfig.INCREMENTAL_SNAPSHOT_CHUNK_SIZE, 250)
                .with(RelationalDatabaseConnectorConfig.MSG_KEY_COLUMNS, noPKTableDataCollectionId() + ":pk1,pk2,pk3,pk4");
    }

    @Override
    protected Builder mutableConfig(boolean signalTableOnly, boolean storeOnlyCapturedDdl) {
        Builder config = config()
                .with(InformixConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(SchemaHistory.STORE_ONLY_CAPTURED_TABLES_DDL, storeOnlyCapturedDdl)
                .with(RelationalDatabaseConnectorConfig.MSG_KEY_COLUMNS, noPKTableDataCollectionId() + ":pk1,pk2,pk3,pk4");
        return signalTableOnly
                ? config.with(InformixConnectorConfig.TABLE_EXCLUDE_LIST, this::tableDataCollectionId)
                : config.with(InformixConnectorConfig.TABLE_INCLUDE_LIST, this::tableIncludeList);
    }

    @Override
    protected String connector() {
        return TestHelper.TEST_CONNECTOR;
    }

    @Override
    protected String server() {
        return TestHelper.TEST_DATABASE;
    }

    @Test
    @Ignore("Informix does not support DDL operations on tables defined for replication")
    @Override
    public void snapshotPreceededBySchemaChange() throws Exception {
        super.snapshotPreceededBySchemaChange();
    }
}
