package laoflch.debezium.connector.informix;

import io.debezium.config.Configuration;
import io.debezium.data.SchemaAndValueField;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.time.Date;
import io.debezium.util.Strings;
import io.debezium.util.Testing;
import laoflch.debezium.connector.informix.util.TestHelper;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static io.debezium.relational.RelationalDatabaseConnectorConfig.DECIMAL_HANDLING_MODE;
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
        connection.execute("create table if not exists test_bigserial(a bigserial)");
        connection.execute("truncate table test_bigserial");
        connection.execute("create table if not exists test_char(a char)");
        connection.execute("truncate table test_char");
        connection.execute("create table if not exists test_date(a date)");
        connection.execute("truncate table test_date");
        connection.execute("create table if not exists test_decimal(a decimal)");
        connection.execute("truncate table test_decimal");
        connection.execute("create table if not exists test_decimal_20(a decimal(20))");
        connection.execute("truncate table test_decimal_20");
        connection.execute("create table if not exists test_decimal_20_5(a decimal(20, 5))");
        connection.execute("truncate table test_decimal_20_5");

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

        connection.execute("truncate table test_bigint");
        connection.execute("truncate table test_bigserial");
        connection.execute("truncate table test_char");
        connection.execute("truncate table test_date");
        connection.execute("truncate table test_decimal");
        connection.execute("truncate table test_decimal_20_5");

        final Configuration config = TestHelper.defaultConfig()
                .with(SNAPSHOT_MODE, INITIAL_SCHEMA_ONLY)
                .with(DECIMAL_HANDLING_MODE, JdbcValueConverters.DecimalMode.STRING)
                .build();

        start(InformixConnector.class, config);

        /*
         * Wait InformixStreamingChangeEventSource.execute() is running.
         */
        Thread.sleep(60_000);

        /*
         * bigint
         */
        Long testLongValue = new Random().nextLong();
        insertOneAndValidate("test_bigint", Schema.OPTIONAL_INT64_SCHEMA, testLongValue.toString(), testLongValue);

        /*
         * bigserial
         */
        Long testBigSerialValue = new Random().nextLong();
        insertOneAndValidate("test_bigserial", Schema.INT64_SCHEMA, testBigSerialValue.toString(), testBigSerialValue);

        /*
         * char
         */
        // insertOneAndValidate("test_char", Schema.OPTIONAL_STRING_SCHEMA, "'a'", 'a');

        /*
         * date
         *
         * As described from official manual:
         *    "The DATE data type stores the calendar date. DATE data types require four bytes. A
         *     calendar date is stored internally as an integer value equal to the number of days
         *     since December 31, 1899."
         * - https://www.ibm.com/docs/en/informix-servers/12.10?topic=types-date-data-type
         *
         * TODO: But, as we test locally, it seems the base date is "1970-01-01", not the "1899-12-31".
         */
        List<String> arrTestDate = Arrays.asList(new String[] { "2022-01-01" });
        for (String strTestDate : arrTestDate) {
            Integer d = Math.toIntExact(diffInDays(strTestDate, "1970-01-01"));
            insertOneAndValidate("test_date", io.debezium.time.Date.builder().optional().build(), "'" + strTestDate + "'", d);
        }

        /*
         * decimal
         */
        Map<String, String> decimal_data_expect = new LinkedHashMap<String, String>() {{
            put("12.1", "12.1");
            put("22.12345678901234567890", "22.12345678901235");        // Rounded number
            put("12345678901234567890.12345", "12345678901234570000");
        }};
        for (Map.Entry<String, String> entry: decimal_data_expect.entrySet()) {
            insertOneAndValidate("test_decimal", Schema.OPTIONAL_STRING_SCHEMA, entry.getKey(), entry.getValue());
        }

        /*
         * decimal(20)
         */
        Map<String, String> decimal_20_data_expect = new LinkedHashMap<String, String>() {{
            put("88.07", "88.07");
            put("33.12345", "33.12345");        // Rounded number
            put("123456789012345.12345", "123456789012345.12345");
        }};
        for (Map.Entry<String, String> entry: decimal_20_data_expect.entrySet()) {
            insertOneAndValidate("test_decimal_20", Schema.OPTIONAL_STRING_SCHEMA, entry.getKey(), entry.getValue());
        }

        /*
         * decimal(20, 5)
         */
        Map<String, String> decimal_20_5_data_expect = new LinkedHashMap<String, String>() {{
            put("12.1", "12.10000");
            put("22.12345", "22.12345");        // Rounded number
            put("123456789012345.12345", "123456789012345.12345");
        }};
        for (Map.Entry<String, String> entry: decimal_20_5_data_expect.entrySet()) {
            insertOneAndValidate("test_decimal_20_5", Schema.OPTIONAL_STRING_SCHEMA, entry.getKey(), entry.getValue());
        }

        stopConnector();
    }

    private void insertOneAndValidate(String tableName, Schema valueSchema, String insertValue, Object expectValue) throws SQLException, InterruptedException {
        String topicName = String.format("testdb.informix.%s", tableName);
        connection.execute(String.format("insert into %s values(%s)", tableName, insertValue));

        SourceRecords sourceRecords = consumeRecordsByTopic(1);
        List<SourceRecord> insertOne = sourceRecords.recordsForTopic(topicName);
        assertThat(insertOne).isNotNull();
        assertThat(insertOne).hasSize(1);

        final List<SchemaAndValueField> expectedDeleteRow = Arrays.asList(
                new SchemaAndValueField("a", valueSchema, expectValue)
        );

        final SourceRecord insertedOneRecord = insertOne.get(0);
        final Struct insertedOneValue = (Struct) insertedOneRecord.value();

        assertRecord((Struct) insertedOneValue.get("after"), expectedDeleteRow);
        // SourceRecordAssert.assertThat(insertOneRecord).valueAfterFieldIsEqualTo();
    }

    private void assertRecord(Struct record, List<SchemaAndValueField> expected) {
        expected.forEach(schemaAndValueField -> schemaAndValueField.assertFor(record));
    }

    private long diffInDays(String date1, String date2) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        try {
            java.util.Date d1 = sdf.parse(date1);
            java.util.Date d2 = sdf.parse(date2);
            long diffInMs = Math.abs(d1.getTime() - d2.getTime());
            long diff = TimeUnit.DAYS.convert(diffInMs, TimeUnit.MILLISECONDS);
            System.out.println(diff);
            return diff;
        } catch (ParseException e) {
            return -1;
        }
    }
}
