/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;

import io.debezium.config.Configuration;
import io.debezium.connector.informix.InformixConnectorConfig.SnapshotIsolationMode;
import io.debezium.connector.informix.InformixConnectorConfig.SnapshotLockingMode;
import io.debezium.connector.informix.util.TestHelper;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.ConditionalFailExtension;
import io.debezium.junit.Flaky;
import io.debezium.pipeline.AbstractChunkedSnapshotTest;

/**
 * Informix-specific chunked table snapshot integration tests.
 *
 * @author Chris Cranford
 */
@Flaky("dbz#1220")
@ExtendWith(ConditionalFailExtension.class)
public class InformixChunkedSnapshotIT extends AbstractChunkedSnapshotTest<InformixConnector> {

    private InformixConnection connection;

    @BeforeEach
    public void beforeEach() throws Exception {
        connection = TestHelper.testConnection();
        TestHelper.dropTables(getAllTableNamesAsArray());
        Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
        super.beforeEach();
    }

    @AfterEach
    public void afterEach() throws Exception {
        super.afterEach();
        stopConnector(b -> TestHelper.forceLoggingOff(getAllTableNamesAsArray()));
        waitForConnectorShutdown(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        if (connection != null) {
            connection.rollback();
            TestHelper.dropTables(getAllTableNamesAsArray());
            connection.close();
        }
    }

    @Override
    protected Class<InformixConnector> getConnectorClass() {
        return InformixConnector.class;
    }

    @Override
    protected JdbcConnection getConnection() {
        return connection;
    }

    @Override
    protected Configuration.Builder getConfig() {
        return TestHelper.defaultConfig()
                // todo: using default of repeatable_read blocks, despite locks being released?
                .with(InformixConnectorConfig.SNAPSHOT_ISOLATION_MODE, SnapshotIsolationMode.READ_COMMITTED)
                .with(InformixConnectorConfig.SNAPSHOT_LOCKING_MODE, SnapshotLockingMode.SHARE)
                .with(InformixConnectorConfig.SNAPSHOT_LOCK_TIMEOUT_MS, 30_000L)
                .with(InformixConnectorConfig.CDC_BUFFERSIZE, 0x10_0000)
                .with(InformixConnectorConfig.CDC_TIMEOUT, 1)
                .with(InformixConnectorConfig.CDC_MAX_RECORDS, 256);
    }

    @Override
    protected void waitForSnapshotToBeCompleted() throws InterruptedException {
        waitForSnapshotToBeCompleted(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
    }

    @Override
    protected void waitForStreamingRunning() throws InterruptedException {
        waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
        waitForAvailableRecords(waitTimeForRecords(), TimeUnit.MINUTES);
    }

    @Override
    protected String connector() {
        return TestHelper.TEST_CONNECTOR;
    }

    @Override
    protected String server() {
        return TestHelper.TEST_DATABASE;
    }

    protected List<String> getAllTableNames() {
        List<String> tableNames = new ArrayList<>(getMultipleSingleKeyTableNames());
        tableNames.add(getSingleKeyTableName());
        return Collections.unmodifiableList(tableNames);
    }

    protected String[] getAllTableNamesAsArray() {
        return getAllTableNames().toArray(new String[0]);
    }

    @Override
    protected String getSingleKeyCollectionName() {
        return getFullyQualifiedTableName(getSingleKeyTableName());
    }

    @Override
    protected String getCompositeKeyCollectionName() {
        return getFullyQualifiedTableName(getCompositeKeyTableName());
    }

    @Override
    protected String getMultipleSingleKeyCollectionNames() {
        return getMultipleSingleKeyTableNames().stream().map(this::getFullyQualifiedTableName).collect(Collectors.joining(","));
    }

    @Override
    protected void createSingleKeyTable(String tableName) throws SQLException {
        connection.execute("CREATE TABLE %s (id int not null, data varchar(50), primary key(id)) LOCK MODE ROW".formatted(tableName));
    }

    @Override
    protected void createCompositeKeyTable(String tableName) throws SQLException {
        connection.execute(
                "CREATE TABLE %s (id int not null, org_name varchar(50) not null, data varchar(50), primary key(id, org_name)) LOCK MODE ROW".formatted(tableName));
    }

    @Override
    protected void createKeylessTable(String tableName) throws SQLException {
        connection.execute("CREATE TABLE %s (id int, data varchar(50)) LOCK MODE ROW".formatted(tableName.toUpperCase()));
    }

    @Override
    protected String getSingleKeyTableKeyColumnName() {
        return "id";
    }

    @Override
    protected List<String> getCompositeKeyTableKeyColumnNames() {
        return List.of("id", "org_name");
    }

    @Override
    protected String getTableTopicName(String tableName) {
        return getFullyQualifiedTableName(tableName);
    }

    @Override
    protected String getFullyQualifiedTableName(String tableName) {
        return "testdb.informix.%s".formatted(tableName);
    }

    protected String quotedTableIdString(String tableName) {
        return "testdb:informix.%s".formatted(tableName);
    }

    @Override
    protected String getSnapshotSelectOverrideQuery() {
        return "SELECT * FROM %s WHERE id = 0".formatted(quotedTableIdString(getSingleKeyTableName()));
    }

    @Override
    protected int getMaximumEnqueuedRecordCount() {
        return 30_000 * 5 + 1;
    }
}