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

import io.debezium.config.Configuration;
import io.debezium.connector.informix.InformixConnectorConfig.SnapshotMode;
import io.debezium.connector.informix.util.TestHelper;
import io.debezium.pipeline.notification.AbstractNotificationsIT;

public class NotificationsIT extends AbstractNotificationsIT<InformixConnector> {

    private InformixConnection connection;

    @BeforeEach
    public void before() throws SQLException {
        connection = TestHelper.testConnection();

        TestHelper.dropTable(connection, "tablea");
        connection.execute("CREATE TABLE tablea (id int not null, cola varchar(30), primary key(id))");

        initializeConnectorTestFramework();
        Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
        Print.enable();
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
            TestHelper.dropTable(connection, "tablea");
            connection.close();
        }
    }

    @Override
    protected Class<InformixConnector> connectorClass() {
        return InformixConnector.class;
    }

    @Override
    protected Configuration.Builder config() {
        return TestHelper.defaultConfig().with(InformixConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL);
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
    protected String snapshotStatusResult() {
        return "COMPLETED";
    }

    @Override
    protected List<String> collections() {
        return List.of("testdb.informix.tablea");
    }
}
