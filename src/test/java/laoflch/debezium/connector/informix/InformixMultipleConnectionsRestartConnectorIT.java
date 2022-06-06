/*
 * Copyright Debezium-Informix-Connector Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package laoflch.debezium.connector.informix;

import static laoflch.debezium.connector.informix.InformixConnectorConfig.SNAPSHOT_MODE;
import static laoflch.debezium.connector.informix.InformixConnectorConfig.SnapshotMode.INITIAL_SCHEMA_ONLY;
import static laoflch.debezium.connector.informix.util.TestHelper.assertRecord;
import static org.fest.assertions.Assertions.assertThat;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.data.SchemaAndValueField;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.util.Testing;

import laoflch.debezium.connector.informix.util.TestHelper;

public class InformixMultipleConnectionsRestartConnectorIT extends AbstractConnectorTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(InformixRestartConnectorIT.class);

    private InformixConnection conn1;
    private InformixConnection conn2;

    @Before
    public void before() throws SQLException {
        conn1 = TestHelper.testConnection();
        conn2 = TestHelper.testConnection();

        conn1.execute("create table if not exists test_restart(b varchar(200))");
        conn1.execute("truncate table test_restart");

        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.DB_HISTORY_PATH);
        Testing.Print.enable();
    }

    @After
    public void after() throws SQLException {
        if (conn1 != null) {
            conn1.close();
        }
        if (conn2 != null) {
            conn2.close();
        }
    }

    @Test
    public void restartWithinTransaction() throws Exception {
        final int RECORD_PER_TABLE = 10;

        final Configuration config = TestHelper.defaultConfig()
                .with(SNAPSHOT_MODE, INITIAL_SCHEMA_ONLY)
                .build();

        conn1.setAutoCommit(false);

        /*
         * Stage 1:
         */
        start(InformixConnector.class, config);
        assertConnectorIsRunning();
        // Wait InformixStreamingChangeEventSource.execute() is running.
        Thread.sleep(60_000);

        conn1.setAutoCommit(true);
        conn1.executeWithoutCommitting("insert into testdb:test_restart values('conn-1-stage-1-0')");
        assertRecordValues("testdb.informix.test_restart", Collections.singletonList("conn-1-stage-1-0"));

        conn1.setAutoCommit(false);
        for (int i = 0; i < RECORD_PER_TABLE; i++) {
            String insertSql = String.format("INSERT INTO testdb:informix.test_restart VALUES ('conn-1-stage-1-nonautocommit-%d')", i);

            conn1.executeWithoutCommitting(insertSql);
        }

        conn2.execute("insert into testdb:informix.test_restart values('conn-2-0')");
        assertRecordValues("testdb.informix.test_restart", Collections.singletonList("conn-2-0"));

        // long msBefore = System.currentTimeMillis();
        // LOGGER.info("{} uncommitted records is executed, sleep 60 seconds..., ts={}, {}", RECORD_PER_TABLE, new Timestamp(msBefore), msBefore);
        // Thread.sleep(60_000);
        // long msAfter = System.currentTimeMillis();
        // LOGGER.info("Sleep completed, ts={}, {}", new Timestamp(msAfter), msAfter);

        // TODO: this testcase should restart the connector
        stopConnector();

        /*
         * Stage 2: the gap between two informix-connector active.
         */
        // After stop the connector, commit previous records
        conn1.connection().commit();

        conn1.executeWithoutCommitting("insert into testdb:informix.test_restart values('conn-1-stage-2-2')");
        conn1.connection().commit();
        conn1.setAutoCommit(true);

        /*
         * Stage 3: Restart the connector and consume data
         */
        // TODO: this testcase should restart the connector
        start(InformixConnector.class, config);
        assertConnectorIsRunning();

        // Wait InformixStreamingChangeEventSource.execute() is running.
        Thread.sleep(60_000);

        /*
         * TODO: This extra insertion will help informix push all records immediately. Don't know why.
         * Add this will make testcase more stable.
         */
        conn1.execute("insert into testdb:informix.test_restart values('conn-1-stage-2-autocommit-999')");
        List<String> expectValueList = new ArrayList<>();
        for (int i = 0; i < RECORD_PER_TABLE; i++) {
            expectValueList.add(String.format("conn-1-stage-1-nonautocommit-%d", i));
        }
        expectValueList.add("conn-1-stage-2-2");
        expectValueList.add("conn-1-stage-2-autocommit-999");
        assertRecordValues("testdb.informix.test_restart", expectValueList);

        stopConnector();
    }

    public void assertRecordValues(String topicName, List<String> expectRows) throws InterruptedException {
        SourceRecords sourceRecords = consumeRecordsByTopic(expectRows.size());
        List<SourceRecord> records = sourceRecords.recordsForTopic(topicName);

        assertThat(records).isNotNull();
        assertThat(records).hasSize(expectRows.size());

        for (int i = 0; i < expectRows.size(); i++) {
            final List<SchemaAndValueField> expectedDeleteRow = Collections.singletonList(
                    new SchemaAndValueField("b", Schema.OPTIONAL_STRING_SCHEMA, expectRows.get(i)));

            final SourceRecord record = records.get(i);
            final Struct oneValue = (Struct) record.value();

            assertRecord((Struct) oneValue.get("after"), expectedDeleteRow);
        }
    }
}
