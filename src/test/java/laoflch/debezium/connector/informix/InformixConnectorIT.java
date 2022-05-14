package laoflch.debezium.connector.informix;

import io.debezium.config.Configuration;
import io.debezium.data.SchemaAndValueField;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.util.Testing;
import laoflch.debezium.connector.informix.util.TestHelper;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static laoflch.debezium.connector.informix.InformixConnectorConfig.SNAPSHOT_MODE;
import static laoflch.debezium.connector.informix.InformixConnectorConfig.SnapshotMode.INITIAL_SCHEMA_ONLY;
import static org.fest.assertions.Assertions.assertThat;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


public class InformixConnectorIT extends AbstractConnectorTest {

    private InformixConnection connection;

    @Before
    public void before() throws SQLException {
        connection = TestHelper.testConnection();
        // connection.execute("drop table if exists hello");

        /*
         * Since all DDL operations are forbidden during Informix CDC, we have to prepare
         * all tables for testing.
         */
        connection.execute("create table if not exists hello(a integer, b varchar(200))");
        connection.execute("truncate table hello");
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.DB_HISTORY_PATH);
        Testing.Print.enable();
    }

    @After
    public void after() throws SQLException {
        if (connection != null) {
            // connection.execute("drop table hello");
            connection.close();
        }
    }

    @Test
    public void insertOneRecord() throws Exception {
        connection.execute("truncate table hello");

        final Configuration config = TestHelper.defaultConfig()
                .with(SNAPSHOT_MODE, INITIAL_SCHEMA_ONLY)
                .build();

        start(InformixConnector.class, config);

        /*
         * Wait InformixStreamingChangeEventSource.execute() is running.
         */
        Thread.sleep(60_000);

        connection.execute("insert into testdb:hello values(0, 'hello-0')");

        SourceRecords sourceRecords = consumeRecordsByTopic(1);
        List<SourceRecord> insertOne = sourceRecords.recordsForTopic("testdb.informix.hello");
        assertThat(insertOne).isNotNull();
        assertThat(insertOne).hasSize(1);

        final List<SchemaAndValueField> expectedDeleteRow = Arrays.asList(
                new SchemaAndValueField("a", Schema.OPTIONAL_INT32_SCHEMA, 0),
                new SchemaAndValueField("b", Schema.OPTIONAL_STRING_SCHEMA, "hello-0"));

        final SourceRecord insertOneRecord = insertOne.get(0);
        final Struct insertOneValue = (Struct) insertOneRecord.value();

        assertRecord((Struct) insertOneValue.get("after"), expectedDeleteRow);

        stopConnector();
    }

    @Test
    public void insertBatchRecords() throws Exception {
        int randStart = (new Random()).nextInt(0, 1000);
        final int RECORDS_PER_TABLE = 5;

        connection.execute("truncate table hello");

        final Configuration config = TestHelper.defaultConfig()
                .with(SNAPSHOT_MODE, INITIAL_SCHEMA_ONLY)
                .build();

        start(InformixConnector.class, config);
        /*
         * Wait InformixStreamingChangeEventSource.execute() is running.
         */
        Thread.sleep(60_000);

        List<Integer> listIntIds = IntStream.range(0, RECORDS_PER_TABLE)
                .boxed().collect(Collectors.toList());
        Collections.shuffle(listIntIds);

        for (int i = randStart; i < randStart + RECORDS_PER_TABLE; i++) {
            String insertSql = String.format("INSERT INTO testdb:hello VALUES (%d, 'hello-%d')",
                    listIntIds.get(i - randStart),
                    listIntIds.get(i - randStart)
            );

            connection.execute(insertSql);
        }

        SourceRecords sourceRecords = consumeRecordsByTopic(RECORDS_PER_TABLE);
        List<SourceRecord> insertOne = sourceRecords.recordsForTopic("testdb.informix.hello");
        assertThat(insertOne).isNotNull();
        assertThat(insertOne).hasSize(RECORDS_PER_TABLE);

        for (int i = randStart; i < randStart + RECORDS_PER_TABLE; i++) {
            Integer currIdx = listIntIds.get(i - randStart);
            String strValue = "hello-" + currIdx;

            final List<SchemaAndValueField> expectedDeleteRow = Arrays.asList(
                    new SchemaAndValueField("a", Schema.OPTIONAL_INT32_SCHEMA, currIdx),
                    new SchemaAndValueField("b", Schema.OPTIONAL_STRING_SCHEMA, strValue));

            final SourceRecord insertOneRecord = insertOne.get(i - randStart);
            final Struct insertOneValue = (Struct) insertOneRecord.value();

            assertRecord((Struct) insertOneValue.get("after"), expectedDeleteRow);
        }

        stopConnector();
    }

    private void assertRecord(Struct record, List<SchemaAndValueField> expected) {
        expected.forEach(schemaAndValueField -> schemaAndValueField.assertFor(record));
    }
}
