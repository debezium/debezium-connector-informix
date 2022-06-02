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
import java.util.Arrays;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.data.SchemaAndValueField;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.util.Testing;

import laoflch.debezium.connector.informix.util.TestHelper;

public class InformixRestartConnectorIT extends AbstractConnectorTest {

    private InformixConnection connection;

    @Before
    public void before() throws SQLException {
        connection = TestHelper.testConnection();

        connection.execute("create table if not exists hello(a integer, b varchar(200))");
        connection.execute("truncate table hello");

        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.DB_HISTORY_PATH);
        Testing.Print.enable();
    }

    @After
    public void after() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    public void restartNormal() throws Exception {
        final Configuration config = TestHelper.defaultConfig()
                .with(SNAPSHOT_MODE, INITIAL_SCHEMA_ONLY)
                .build();

        start(InformixConnector.class, config);
        // Wait InformixStreamingChangeEventSource.execute() is running.
        Thread.sleep(60_000);

        connection.execute("insert into testdb:hello values(0, 'hello-0')");

        SourceRecords sourceRecordsBefore = consumeRecordsByTopic(1);
        List<SourceRecord> recordsBefore = sourceRecordsBefore.recordsForTopic("testdb.informix.hello");
        assertThat(recordsBefore).isNotNull();
        assertThat(recordsBefore).hasSize(1);

        stopConnector();

        // After stop the connector
        connection.execute("insert into testdb:hello values(1, 'hello-1')");

        /*
         * Restart the connector and consume data
         */
        start(InformixConnector.class, config);

        // Wait InformixStreamingChangeEventSource.execute() is running.
        Thread.sleep(60_000);

        /*
         * TODO: This extra insertion will help informix push all records immediately. Don't know why.
         * Add this will make testcase more stable.
         */
        connection.execute("insert into testdb:hello values(2, 'hello-2')");

        SourceRecords sourceRecords = consumeRecordsByTopic(1);
        List<SourceRecord> insertOne = sourceRecords.recordsForTopic("testdb.informix.hello");
        assertThat(insertOne).isNotNull();
        assertThat(insertOne).hasSize(1);

        final List<SchemaAndValueField> expectedDeleteRow = Arrays.asList(
                new SchemaAndValueField("a", Schema.OPTIONAL_INT32_SCHEMA, 1),
                new SchemaAndValueField("b", Schema.OPTIONAL_STRING_SCHEMA, "hello-1"));

        final SourceRecord insertOneRecord = insertOne.get(0);
        final Struct insertOneValue = (Struct) insertOneRecord.value();

        assertRecord((Struct) insertOneValue.get("after"), expectedDeleteRow);

        stopConnector();
    }
}
