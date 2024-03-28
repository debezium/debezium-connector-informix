/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.informix.InformixConnectorConfig.SnapshotMode;
import io.debezium.connector.informix.util.TestHelper;
import io.debezium.data.SourceRecordAssert;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.junit.Flaky;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.RelationalDatabaseConnectorConfig.DecimalHandlingMode;
import io.debezium.time.Date;
import io.debezium.util.Testing;

public class InformixCdcTypesIT extends AbstractConnectorTest {

    private InformixConnection connection;

    @Before
    public void before() throws SQLException {
        connection = TestHelper.testConnection();
        connection.execute(
                "drop table if exists test_bigint",
                "drop table if exists test_bigserial",
                "drop table if exists test_char",
                "drop table if exists test_date",
                "drop table if exists test_decimal",
                "drop table if exists test_decimal_20",
                "drop table if exists test_decimal_20_5",
                "create table test_bigint(a bigint)",
                "create table test_bigserial(a bigserial)",
                "create table test_char(a char)",
                "create table test_date(a date)",
                "create table test_decimal(a decimal)",
                "create table test_decimal_20(a decimal(20))",
                "create table test_decimal_20_5(a decimal(20, 5))");

        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
        Testing.Print.enable();
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
                    .execute("drop table test_bigint",
                            "drop table test_bigserial",
                            "drop table test_char",
                            "drop table test_date",
                            "drop table test_decimal",
                            "drop table test_decimal_20",
                            "drop table test_decimal_20_5")
                    .close();
        }
    }

    @Test
    @Flaky("DBZ-7531")
    public void testTypes() throws Exception {

        final Configuration config = TestHelper.defaultConfig()
                .with(InformixConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(RelationalDatabaseConnectorConfig.DECIMAL_HANDLING_MODE, DecimalHandlingMode.STRING)
                .build();

        start(InformixConnector.class, config);

        assertConnectorIsRunning();

        waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

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
         * "The DATE data type stores the calendar date. DATE data types require four bytes. A
         * calendar date is stored internally as an integer value equal to the number of days
         * since December 31, 1899."
         * - https://www.ibm.com/docs/en/informix-servers/12.10?topic=types-date-data-type
         *
         * TODO: But, as we test locally, it seems the base date is "1970-01-01", not the "1899-12-31".
         */
        String[] arrTestDate = new String[]{ "2022-01-01" };
        for (String strTestDate : arrTestDate) {
            Integer d = Math.toIntExact(diffInDays("1970-01-01", strTestDate));
            insertOneAndValidate("test_date", Date.builder().optional().build(), "'" + strTestDate + "'", d);
        }

        /*
         * decimal
         */
        Map<String, String> decimal_data_expect = new LinkedHashMap<>() {
            {
                put("12.1", "12.1");
                put("22.12345678901234567890", "22.12345678901235"); // Rounded number
                put("12345678901234567890.12345", "12345678901234570000");
            }
        };
        for (Map.Entry<String, String> entry : decimal_data_expect.entrySet()) {
            insertOneAndValidate("test_decimal", Schema.OPTIONAL_STRING_SCHEMA, entry.getKey(), entry.getValue());
        }

        /*
         * decimal(20)
         */
        Map<String, String> decimal_20_data_expect = new LinkedHashMap<>() {
            {
                put("88.07", "88.07");
                put("33.12345", "33.12345"); // Rounded number
                put("123456789012345.12345", "123456789012345.12345");
            }
        };
        for (Map.Entry<String, String> entry : decimal_20_data_expect.entrySet()) {
            insertOneAndValidate("test_decimal_20", Schema.OPTIONAL_STRING_SCHEMA, entry.getKey(), entry.getValue());
        }

        /*
         * decimal(20, 5)
         */
        Map<String, String> decimal_20_5_data_expect = new LinkedHashMap<>() {
            {
                put("12.1", "12.10000");
                put("22.12345", "22.12345"); // Rounded number
                put("123456789012345.12345", "123456789012345.12345");
            }
        };
        for (Map.Entry<String, String> entry : decimal_20_5_data_expect.entrySet()) {
            insertOneAndValidate("test_decimal_20_5", Schema.OPTIONAL_STRING_SCHEMA, entry.getKey(), entry.getValue());
        }
    }

    private void insertOneAndValidate(String tableName, Schema valueSchema, String insertValue, Object expectValue) throws Exception {
        String topicName = String.format("testdb.informix.%s", tableName);
        connection.execute(String.format("insert into %s values(%s)", tableName, insertValue));

        waitForAvailableRecords(10, TimeUnit.SECONDS);

        List<SourceRecord> records = consumeRecordsByTopic(1).recordsForTopic(topicName);
        assertThat(records).isNotNull().hasSize(1);

        Schema aSchema = SchemaBuilder.struct()
                .optional().name(String.format("%s.Value", topicName)).field("a", valueSchema)
                .build();
        Struct aStruct = new Struct(aSchema).put("a", expectValue);

        SourceRecordAssert.assertThat(records.get(0)).valueAfterFieldIsEqualTo(aStruct);
    }

    private long diffInDays(String one, String other) {
        return LocalDate.parse(one).until(LocalDate.parse(other), ChronoUnit.DAYS);
    }

}
