/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.util.List;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.config.Configuration;
import io.debezium.connector.informix.InformixConnectorConfig.SnapshotMode;
import io.debezium.connector.informix.util.TestHelper;
import io.debezium.data.SchemaAndValueField;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.junit.ConditionalFail;

/**
 * Integration test for the Debezium Informix connector.
 *
 */
public class InformixLargeTranactionIT extends AbstractAsyncEngineConnectorTest {

    @Rule
    public TestRule conditionalFail = new ConditionalFail();

    private InformixConnection connection;

    @Before
    public void before() throws SQLException {
        connection = TestHelper.testConnection();
        connection.execute(
                "DROP TABLE IF EXISTS tablea",
                "DROP TABLE IF EXISTS tableb",
                "CREATE TABLE tablea (id int not null, cola varchar(30), primary key (id))",
                "CREATE TABLE tableb (id int not null, colb varchar(30), primary key (id))");
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
                            "DROP TABLE tablea",
                            "DROP TABLE tableb")
                    .close();
        }
    }

    private void testLargeTransaction(Configuration config) throws InterruptedException, SQLException {
        final int RECORDS_PER_TABLE = 100_000;
        final int ID_START = 1;

        start(InformixConnector.class, config);
        assertConnectorIsRunning();

        // Wait for streaming to start
        waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        connection.setAutoCommit(false);
        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START + i;
            connection.executeWithoutCommitting("INSERT INTO tablea VALUES(" + id + ", 'a')");
            connection.executeWithoutCommitting("INSERT INTO tableb VALUES(" + id + ", 'b')");
        }
        connection.commit();

        waitForAvailableRecords();

        final SourceRecords records = consumeRecordsByTopic(RECORDS_PER_TABLE);
        final List<SourceRecord> tableA = records.recordsForTopic("testdb.informix.tablea");
        final List<SourceRecord> tableB = records.recordsForTopic("testdb.informix.tableb");
        assertThat(tableA).isNullOrEmpty();
        assertThat(tableB).hasSize(RECORDS_PER_TABLE);
        assertNoRecordsToConsume();
    }

    @Test
    public void testLargeTransactionWithCaffeine() throws Exception {
        final Configuration config = TestHelper.defaultConfig()
                .with(InformixConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(InformixConnectorConfig.TABLE_INCLUDE_LIST, "testdb.informix.tableb")
                .build();

        testLargeTransaction(config);
    }

    @Test
    public void testLargeTransactionWithHazelcast() throws Exception {
        final Configuration config = TestHelper.defaultConfig()
                .with(InformixConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(InformixConnectorConfig.TABLE_INCLUDE_LIST, "testdb.informix.tableb")
                .with(InformixConnectorConfig.JCACHE_PROVIDER_CLASSNAME, "com.hazelcast.cache.HazelcastMemberCachingProvider")
                .with(InformixConnectorConfig.JCACHE_URI, "hazelcast.yaml")
                .build();

        testLargeTransaction(config);
    }

    @Test
    public void testLargeTransactionWithEhCache() throws Exception {
        final Configuration config = TestHelper.defaultConfig()
                .with(InformixConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(InformixConnectorConfig.TABLE_INCLUDE_LIST, "testdb.informix.tableb")
                .with(InformixConnectorConfig.JCACHE_PROVIDER_CLASSNAME, "org.ehcache.jcache.JCacheCachingProvider")
                .with(InformixConnectorConfig.JCACHE_URI, "ehcache.xml")
                .build();

        testLargeTransaction(config);
    }

    private void assertRecord(Struct record, List<SchemaAndValueField> expected) {
        expected.forEach(schemaAndValueField -> schemaAndValueField.assertFor(record));
    }
}
