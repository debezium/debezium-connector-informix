/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import org.junit.After;
import org.junit.Before;

import io.debezium.config.Configuration;
import io.debezium.connector.informix.InformixConnectorConfig.SnapshotMode;
import io.debezium.connector.informix.util.TestHelper;
import io.debezium.pipeline.notification.AbstractNotificationsIT;

import lombok.SneakyThrows;

public class NotificationsIT extends AbstractNotificationsIT<InformixConnector> {

    private InformixConnection connection;

    @Before
    @SneakyThrows
    public void before() {
        connection = TestHelper.testConnection();
        initializeConnectorTestFramework();
        Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
        Print.enable();
    }

    @After
    @SneakyThrows
    public void after() {
        if (connection != null) {
            connection.rollback().close();
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
}
