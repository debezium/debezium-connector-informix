/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import java.sql.SQLException;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestMethodOrder;

import io.debezium.config.Configuration;
import io.debezium.connector.informix.InformixConnectorConfig.SnapshotIsolationMode;
import io.debezium.connector.informix.InformixConnectorConfig.SnapshotLockingMode;
import io.debezium.connector.informix.util.TestHelper;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.AbstractChunkedSnapshotTest;
import io.debezium.util.Testing;

/**
 * Informix-specific chunked table snapshot integration tests.
 *
 * @author Chris Cranford
 */
@TestMethodOrder(MethodOrderer.Random.class)
public class InformixChunkedSnapshotIT extends AbstractChunkedSnapshotTest<InformixConnector> {

    private InformixConnection connection;

    @BeforeEach
    public void beforeEach() throws Exception {
        connection = TestHelper.testConnection();
        TestHelper.dropTables(connection, "dbz1220a", "dbz1220b", "dbz1220c", "dbz1220d", "dbz1220");

        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);

        super.beforeEach();
    }

    @AfterEach
    public void afterEach() throws Exception {
        stopConnector();
        waitForConnectorShutdown(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
        assertConnectorNotRunning();

        if (connection != null) {
            connection.rollback();
            TestHelper.dropTables(connection, "dbz1220a", "dbz1220b", "dbz1220c", "dbz1220d", "dbz1220");
            connection.close();
        }
        super.afterEach();
    }

    @Override
    protected void populateSingleKeyTable(String tableName, int rowCount) throws SQLException {
        super.populateSingleKeyTable(tableName, rowCount);
    }

    @Override
    protected void populateCompositeKeyTable(String tableName, int rowCount) throws SQLException {
        super.populateCompositeKeyTable(tableName, rowCount);
    }

    @Override
    protected String getSingleKeyTableName() {
        return "dbz1220";
    }

    @Override
    protected String getCompositeKeyTableName() {
        return "dbz1220";
    }

    @Override
    protected List<String> getMultipleSingleKeyTableNames() {
        return List.of("dbz1220a", "dbz1220b", "dbz1220c", "dbz1220d");
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
                .with(InformixConnectorConfig.SNAPSHOT_LOCK_TIMEOUT_MS, 30_000L);
    }

    @Override
    protected void waitForSnapshotToBeCompleted() throws InterruptedException {
        waitForSnapshotToBeCompleted(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
        waitForAvailableRecords();
    }

    @Override
    protected String getSingleKeyCollectionName() {
        return "testdb.informix.dbz1220";
    }

    @Override
    protected String getCompositeKeyCollectionName() {
        return getSingleKeyCollectionName();
    }

    @Override
    protected String getMultipleSingleKeyCollectionNames() {
        return String.join(",", List.of("testdb.informix.dbz1220a", "testdb.informix.dbz1220b", "testdb.informix.dbz1220c", "testdb.informix.dbz1220d"));
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
        return "testdb.informix.%s".formatted(tableName);
    }

    @Override
    protected String getFullyQualifiedTableName(String tableName) {
        return "testdb.informix.%s".formatted(tableName);
    }

}
