package laoflch.debezium.connector.informix;

import io.debezium.config.Configuration;
import io.debezium.data.SchemaAndValueField;
import io.debezium.data.SourceRecordAssert;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.util.Testing;
import laoflch.debezium.connector.informix.util.TestHelper;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static laoflch.debezium.connector.informix.InformixConnectorConfig.SNAPSHOT_MODE;
import static laoflch.debezium.connector.informix.InformixConnectorConfig.SnapshotMode.INITIAL_SCHEMA_ONLY;
import static org.fest.assertions.Assertions.assertThat;

public class InformixCdcTypesIT extends AbstractConnectorTest {

    private InformixConnection connection;

    @Before
    public void before() throws SQLException {
        connection = TestHelper.testConnection();

        connection.execute("create table if not exists test_bigint(a bigint)");
        connection.execute("truncate table test_bigint");
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.DB_HISTORY_PATH);
        Testing.Print.enable();
    }

    @After
    public void after() throws SQLException {
        if (connection != null) {
            // connection.execute("drop table if exists test_bigint");
            connection.close();
        }
    }

    @Test
    public void testTypeBigint() throws Exception {
        Long testLongValue = new Random().nextLong();

        connection.execute("truncate table test_bigint");

        final Configuration config = TestHelper.defaultConfig()
                .with(SNAPSHOT_MODE, INITIAL_SCHEMA_ONLY)
                .build();

        start(InformixConnector.class, config);

        /*
         * Wait InformixStreamingChangeEventSource.execute() is running.
         */
        Thread.sleep(60_000);

        connection.execute(String.format("insert into test_bigint values(%d)", testLongValue));

        SourceRecords sourceRecords = consumeRecordsByTopic(1);
        List<SourceRecord> insertOne = sourceRecords.recordsForTopic("testdb.informix.test_bigint");
        assertThat(insertOne).isNotNull();
        assertThat(insertOne).hasSize(1);

        final List<SchemaAndValueField> expectedDeleteRow = Arrays.asList(
                new SchemaAndValueField("a", Schema.OPTIONAL_INT64_SCHEMA, testLongValue)
        );

        // final Struct expectStruct = new Struct();

        final SourceRecord insertOneRecord = insertOne.get(0);
        final Struct insertOneValue = (Struct) insertOneRecord.value();
        logger.info("{}", insertOne);

        assertRecord((Struct) insertOneValue.get("after"), expectedDeleteRow);
        // SourceRecordAssert.assertThat(insertOneRecord).valueAfterFieldIsEqualTo();

        stopConnector();
    }

    private void assertRecord(Struct record, List<SchemaAndValueField> expected) {
        expected.forEach(schemaAndValueField -> schemaAndValueField.assertFor(record));
    }
}
