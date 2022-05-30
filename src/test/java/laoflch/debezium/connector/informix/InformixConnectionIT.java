/*
 * Copyright Debezium-Informix-Connector Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package laoflch.debezium.connector.informix;

import org.junit.Test;

import laoflch.debezium.connector.informix.util.TestHelper;

/**
 * Integration test for {@link InformixConnection}
 *
 * @author Xiaolin Zhang (leoncamel@gmail.com)
 */
public class InformixConnectionIT {

    @Test
    public void shouldEnableDatabaseLogging() throws Exception {
        try (InformixConnection conn = TestHelper.adminConnection()) {
            conn.connect();

            TestHelper.isCdcEnabled(conn);
        }
    }
}
