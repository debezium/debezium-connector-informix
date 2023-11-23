/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import org.junit.Before;

import io.debezium.config.Configuration;
import io.debezium.connector.informix.util.TestHelper;
import io.debezium.data.VerifyRecord;
import io.debezium.relational.TableId;

/**
 * Default value handling integration tests using offline schema evolution processes.
 *
 * @author Lars M Johansson, Chris Cranford
 */
public class InformixOfflineDefaultValueIT extends AbstractInformixDefaultValueIT {

    @Before
    public void before() throws SQLException {
        super.before();
        if (VerifyRecord.isApucurioAvailable()) {
            skipAvroValidation(); // https://github.com/Apicurio/apicurio-registry/issues/2980
        }
    }

    @Override
    protected void performSchemaChange(Configuration config, InformixConnection connection, String alterStatement) throws Exception {
        /*
         * Since all DDL operations are forbidden during Informix CDC,
         * we have to ensure the connector is properly shut down before dropping tables.
         */
        stopConnector();

        waitForConnectorShutdown(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        assertConnectorNotRunning();

        final TableId tableId = TableId.parse("informix.dv_test", false);
        final String sourceTable = alterStatement.replace("%table%", tableId.table());

        connection.execute(sourceTable);

        start(InformixConnector.class, config);

        waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        assertConnectorIsRunning();

        waitForAvailableRecords(1, TimeUnit.SECONDS);
    }
}
