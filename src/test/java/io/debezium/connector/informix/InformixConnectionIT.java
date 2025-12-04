/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import org.junit.jupiter.api.Test;

import io.debezium.connector.informix.util.TestHelper;
import io.debezium.util.Testing;

/**
 * Integration test for {@link InformixConnection}
 */
public class InformixConnectionIT implements Testing {

    @Test
    public void shouldEnableDatabaseLogging() throws Exception {
        try (InformixConnection connection = TestHelper.adminConnection()) {
            connection.connect();
            TestHelper.assertCdcEnabled(connection);
        }
    }
}
