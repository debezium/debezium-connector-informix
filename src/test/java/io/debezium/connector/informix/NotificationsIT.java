/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import java.sql.SQLException;
import java.util.List;

import org.junit.After;
import org.junit.Before;

import io.debezium.config.Configuration;
import io.debezium.connector.informix.InformixConnectorConfig.SnapshotMode;
import io.debezium.connector.informix.util.TestHelper;
import io.debezium.pipeline.notification.AbstractNotificationsIT;

public class NotificationsIT extends AbstractNotificationsIT<InformixConnector> {

    private InformixConnection connection;

    @Before
    public void before() throws SQLException {
        connection = TestHelper.testConnection();
        connection.execute(
                "DROP TABLE IF EXISTS tablea",
                "CREATE TABLE tablea (id int not null, cola varchar(30), primary key(id))");

        initializeConnectorTestFramework();
        Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
        Print.enable();
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
                            "DROP TABLE tablea")
                    .close();
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

    protected List<String> collections() {
        return List.of("testdb.informix.tablea");
    }
}
