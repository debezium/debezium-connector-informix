package laoflch.debezium.connector.informix;

import laoflch.debezium.connector.informix.util.TestHelper;
import org.junit.Test;

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
