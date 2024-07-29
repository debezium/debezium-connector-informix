/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.sql.Clob;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.config.Configuration.Builder;
import io.debezium.connector.informix.util.TestHelper;
import io.debezium.data.SchemaAndValueField;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.jdbc.JdbcValueConverters.DecimalMode;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.time.Date;
import io.debezium.time.MicroTime;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.Time;
import io.debezium.time.Timestamp;
import io.debezium.util.Testing;

/**
 * Integration test to verify different Oracle datatypes.
 *
 * @author Jiri Pechanec, Lars M Johansson
 */
public abstract class AbstractInformixDatatypesTest extends AbstractAsyncEngineConnectorTest {

    private static final String DDL_STRING = "create table type_string (" +
            "  id serial not null primary key, " +
            "  val_varchar varchar(255), " +
            "  val_nvarchar nvarchar(255), " +
            "  val_lvarchar lvarchar(1000), " +
            "  val_char char(3), " +
            "  val_nchar nchar(3)" +
            ");";

    private static final String DDL_FP = "create table type_fp (" +
            "  id serial not null primary key, " +
            "  val_sf smallfloat, " +
            "  val_f float, " +
            "  val_f_10 float (10), " +
            "  val_r real, " +
            "  val_dp double precision, " +
            "  val_numeric numeric(10, 6), " +
            "  val_decimal decimal(10, 6), " +
            "  val_decimal_vs decimal(10), " +
            "  val_decimal_vs2 decimal" +
            ");";

    private static final String DDL_INT = "create table type_int (" +
            "  id serial not null primary key, " +
            "  val_int int, " +
            "  val_int8 int8, " +
            "  val_integer integer, " +
            "  val_smallint smallint, " +
            "  val_bigint bigint, " +
            "  val_decimal decimal(10,0), " +
            "  val_numeric numeric(10,0)" +
            ");";

    private static final String DDL_TIME = "create table type_time (" +
            "  id serial not null primary key, " +
            "  val_date date, " +
            "  val_time datetime hour to second, " +
            "  val_datetime datetime year to second, " +
            "  val_timestamp datetime year to fraction, " +
            "  val_timestamp_us datetime year to fraction(5) " +
            // " val_int_ytm interval year to month, " +
            // " val_int_dts interval day(3) to second " +
            ");";

    private static final String DDL_CLOB = "create table type_clob (" +
            "  id serial not null primary key, " +
            "  val_clob_inline clob, " +
            "  val_clob_short clob, " +
            "  val_clob_long clob" +
            ");";

    private static final List<SchemaAndValueField> EXPECTED_STRING = Arrays.asList(
            new SchemaAndValueField("val_varchar", Schema.OPTIONAL_STRING_SCHEMA, "vc"),
            new SchemaAndValueField("val_nvarchar", Schema.OPTIONAL_STRING_SCHEMA, "nvc"),
            new SchemaAndValueField("val_lvarchar", Schema.OPTIONAL_STRING_SCHEMA, "lvc"),
            new SchemaAndValueField("val_char", Schema.OPTIONAL_STRING_SCHEMA, "c  "),
            new SchemaAndValueField("val_nchar", Schema.OPTIONAL_STRING_SCHEMA, "nc "));

    private static final List<SchemaAndValueField> EXPECTED_FP = Arrays.asList(
            new SchemaAndValueField("val_sf", Schema.OPTIONAL_FLOAT32_SCHEMA, 1.1f),
            new SchemaAndValueField("val_f", Schema.OPTIONAL_FLOAT64_SCHEMA, 2.22d),
            new SchemaAndValueField("val_f_10", Schema.OPTIONAL_FLOAT64_SCHEMA, 3.333d),
            new SchemaAndValueField("val_r", Schema.OPTIONAL_FLOAT32_SCHEMA, 4.4444f),
            new SchemaAndValueField("val_dp", Schema.OPTIONAL_FLOAT64_SCHEMA, 5.55555d),
            new SchemaAndValueField("val_numeric", SpecialValueDecimal.builder(DecimalMode.PRECISE, 10, 6).optional().build(), new BigDecimal("1234.567891")),
            new SchemaAndValueField("val_decimal", SpecialValueDecimal.builder(DecimalMode.PRECISE, 10, 6).optional().build(), new BigDecimal("1234.567891")),
            new SchemaAndValueField("val_decimal_vs", VariableScaleDecimal.builder().optional().build(),
                    VariableScaleDecimal.fromLogical(VariableScaleDecimal.builder().optional().build(), new BigDecimal("77.323"))),
            new SchemaAndValueField("val_decimal_vs2", VariableScaleDecimal.builder().optional().build(),
                    VariableScaleDecimal.fromLogical(VariableScaleDecimal.builder().optional().build(), new BigDecimal("77.323"))));

    private static final List<SchemaAndValueField> EXPECTED_FP_AS_STRING = Arrays.asList(
            new SchemaAndValueField("val_sf", Schema.OPTIONAL_FLOAT32_SCHEMA, 1.1f),
            new SchemaAndValueField("val_f", Schema.OPTIONAL_FLOAT64_SCHEMA, 2.22),
            new SchemaAndValueField("val_f_10", Schema.OPTIONAL_FLOAT64_SCHEMA, 3.333),
            new SchemaAndValueField("val_r", Schema.OPTIONAL_FLOAT32_SCHEMA, 4.4444f),
            new SchemaAndValueField("val_dp", Schema.OPTIONAL_FLOAT64_SCHEMA, 5.55555),
            new SchemaAndValueField("val_numeric", Schema.OPTIONAL_STRING_SCHEMA, "1234.567891"),
            new SchemaAndValueField("val_decimal", Schema.OPTIONAL_STRING_SCHEMA, "1234.567891"),
            new SchemaAndValueField("val_decimal_vs", Schema.OPTIONAL_STRING_SCHEMA, "77.323"),
            new SchemaAndValueField("val_decimal_vs2", Schema.OPTIONAL_STRING_SCHEMA, "77.323"));

    private static final List<SchemaAndValueField> EXPECTED_FP_AS_DOUBLE = Arrays.asList(
            new SchemaAndValueField("val_sf", Schema.OPTIONAL_FLOAT32_SCHEMA, 1.1f),
            new SchemaAndValueField("val_f", Schema.OPTIONAL_FLOAT64_SCHEMA, 2.22),
            new SchemaAndValueField("val_f_10", Schema.OPTIONAL_FLOAT64_SCHEMA, 3.333),
            new SchemaAndValueField("val_r", Schema.OPTIONAL_FLOAT32_SCHEMA, 4.4444f),
            new SchemaAndValueField("val_dp", Schema.OPTIONAL_FLOAT64_SCHEMA, 5.55555),
            new SchemaAndValueField("val_numeric", Schema.OPTIONAL_FLOAT64_SCHEMA, 1234.567891),
            new SchemaAndValueField("val_decimal", Schema.OPTIONAL_FLOAT64_SCHEMA, 1234.567891),
            new SchemaAndValueField("val_decimal_vs", Schema.OPTIONAL_FLOAT64_SCHEMA, 77.323),
            new SchemaAndValueField("val_decimal_vs2", Schema.OPTIONAL_FLOAT64_SCHEMA, 77.323));

    private static final List<SchemaAndValueField> EXPECTED_INT = Arrays.asList(
            new SchemaAndValueField("val_int", Schema.OPTIONAL_INT32_SCHEMA, 1),
            new SchemaAndValueField("val_int8", Schema.OPTIONAL_INT64_SCHEMA, 22L),
            new SchemaAndValueField("val_integer", Schema.OPTIONAL_INT32_SCHEMA, 333),
            new SchemaAndValueField("val_smallint", Schema.OPTIONAL_INT16_SCHEMA, (short) 4444),
            new SchemaAndValueField("val_bigint", Schema.OPTIONAL_INT64_SCHEMA, 55555L),
            new SchemaAndValueField("val_decimal", SpecialValueDecimal.builder(DecimalMode.PRECISE, 10, 0).optional().build(), BigDecimal.valueOf(99999_99999L)),
            new SchemaAndValueField("val_numeric", SpecialValueDecimal.builder(DecimalMode.PRECISE, 10, 0).optional().build(), BigDecimal.valueOf(99999_99999L)));

    private static final List<SchemaAndValueField> EXPECTED_TIME = Arrays.asList(
            new SchemaAndValueField("val_date", Date.builder().optional().build(),
                    (int) LocalDate.of(2024, 3, 27).toEpochDay()),
            new SchemaAndValueField("val_time", Time.builder().optional().build(),
                    LocalTime.of(12, 34, 56).toSecondOfDay() * 1_000),
            new SchemaAndValueField("val_datetime", Timestamp.builder().optional().build(),
                    LocalDateTime.of(2024, 3, 27, 12, 34, 56).toEpochSecond(ZoneOffset.UTC) * 1_000),
            new SchemaAndValueField("val_timestamp", Timestamp.builder().optional().build(),
                    LocalDateTime.of(2024, 3, 27, 12, 34, 56).toEpochSecond(ZoneOffset.UTC) * 1_000 + 123),
            new SchemaAndValueField("val_timestamp_us", MicroTimestamp.builder().optional().build(),
                    LocalDateTime.of(2024, 3, 27, 12, 34, 56).toEpochSecond(ZoneOffset.UTC) * 1_000_000 + 123450));
    // new SchemaAndValueField("VAL_INT_YTM", MicroDuration.builder().optional().build(), -110451600_000_000L),
    // new SchemaAndValueField("VAL_INT_DTS", MicroDuration.builder().optional().build(), -93784_560_000L));

    private static final List<SchemaAndValueField> EXPECTED_TIME_AS_ADAPTIVE = Arrays.asList(
            new SchemaAndValueField("val_date", Date.builder().optional().build(),
                    (int) LocalDate.of(2024, 3, 27).toEpochDay()),
            new SchemaAndValueField("val_time", MicroTime.builder().optional().build(),
                    LocalTime.of(12, 34, 56).toSecondOfDay() * 1_000_000L),
            new SchemaAndValueField("val_datetime", Timestamp.builder().optional().build(),
                    LocalDateTime.of(2024, 3, 27, 12, 34, 56).toEpochSecond(ZoneOffset.UTC) * 1_000),
            new SchemaAndValueField("val_timestamp", Timestamp.builder().optional().build(),
                    LocalDateTime.of(2024, 3, 27, 12, 34, 56).toEpochSecond(ZoneOffset.UTC) * 1_000 + 123),
            new SchemaAndValueField("val_timestamp_us", MicroTimestamp.builder().optional().build(),
                    LocalDateTime.of(2024, 3, 27, 12, 34, 56).toEpochSecond(ZoneOffset.UTC) * 1_000_000 + 123450));
    // new SchemaAndValueField("VAL_INT_YTM", MicroDuration.builder().optional().build(), -110451600_000_000L),
    // new SchemaAndValueField("VAL_INT_DTS", MicroDuration.builder().optional().build(), -93784_560_000L));

    private static final List<SchemaAndValueField> EXPECTED_TIME_AS_CONNECT = Arrays.asList(
            new SchemaAndValueField("val_date", org.apache.kafka.connect.data.Date.builder().optional().build(),
                    java.util.Date.from(LocalDate.of(2024, 3, 27).atStartOfDay().atOffset(ZoneOffset.UTC).toInstant())),
            new SchemaAndValueField("val_time", org.apache.kafka.connect.data.Time.builder().optional().build(),
                    java.util.Date.from(LocalTime.of(12, 34, 56).atDate(LocalDate.EPOCH).atOffset(ZoneOffset.UTC).toInstant())),
            new SchemaAndValueField("val_datetime", org.apache.kafka.connect.data.Timestamp.builder().optional().build(),
                    java.util.Date.from(LocalDateTime.of(2024, 3, 27, 12, 34, 56).atOffset(ZoneOffset.UTC).toInstant())),
            new SchemaAndValueField("val_timestamp", org.apache.kafka.connect.data.Timestamp.builder().optional().build(),
                    java.util.Date.from(LocalDateTime.of(2024, 3, 27, 12, 34, 56, 123 * 1_000_000).atOffset(ZoneOffset.UTC).toInstant())),
            new SchemaAndValueField("val_timestamp_us", org.apache.kafka.connect.data.Timestamp.builder().optional().build(),
                    java.util.Date.from(LocalDateTime.of(2024, 3, 27, 12, 34, 56, 12345 * 10_000).atOffset(ZoneOffset.UTC).toInstant())));
    // new SchemaAndValueField("VAL_INT_YTM", MicroDuration.builder().optional().build(), -110451600_000_000L),
    // new SchemaAndValueField("VAL_INT_DTS", MicroDuration.builder().optional().build(), -93784_560_000L));

    private static final String CLOB_TXT = "TestClob123";
    private static final String CLOB_JSON = Files.readResourceAsString("data/test_lob_data.json");

    private static final List<SchemaAndValueField> EXPECTED_CLOB = Arrays.asList(
            new SchemaAndValueField("val_clob_inline", Schema.OPTIONAL_STRING_SCHEMA, CLOB_TXT),
            new SchemaAndValueField("val_clob_short", Schema.OPTIONAL_STRING_SCHEMA, part(CLOB_JSON, 0, 512)),
            new SchemaAndValueField("val_clob_long", Schema.OPTIONAL_STRING_SCHEMA, part(CLOB_JSON, 0, 5000)));

    public static final String CLOB_TXT_UPDATE = "TestClob123Update";

    private static final List<SchemaAndValueField> EXPECTED_CLOB_UPDATE = Arrays.asList(
            new SchemaAndValueField("val_clob_inline", Schema.OPTIONAL_STRING_SCHEMA, CLOB_TXT_UPDATE),
            new SchemaAndValueField("val_clob_short", Schema.OPTIONAL_STRING_SCHEMA, part(CLOB_JSON, 1, 512)),
            new SchemaAndValueField("val_clob_long", Schema.OPTIONAL_STRING_SCHEMA, part(CLOB_JSON, 1, 5000)));

    private static final String[] ALL_TABLES = {
            "informix.type_string",
            "informix.type_fp",
            "informix.type_int",
            "informix.type_time",
            "informix.type_clob"
    };

    private static final String[] ALL_DDLS = {
            DDL_STRING,
            DDL_FP,
            DDL_INT,
            DDL_TIME,
            DDL_CLOB,
    };

    private static InformixConnection connection;

    @BeforeClass
    public static void beforeClass() throws SQLException {
        connection = TestHelper.testConnection();
        dropTables();
    }

    @AfterClass
    public static void afterClass() throws SQLException {
        if (connection != null) {
            connection.rollback();
            dropTables();
            connection.close();
        }
    }

    protected static void createTables() throws SQLException {
        connection.execute(ALL_DDLS);
    }

    public static void dropTables() throws SQLException {
        for (String table : ALL_TABLES) {
            TestHelper.dropTable(connection, table);
        }
    }

    protected List<String> getAllTables() {
        return Arrays.asList(ALL_TABLES);
    }

    protected abstract boolean insertRecordsDuringTest();

    protected abstract Builder connectorConfig();

    protected abstract void init(TemporalPrecisionMode temporalPrecisionMode) throws Exception;

    @Test
    public void stringTypes() throws Exception {
        int expectedRecordCount = 0;

        if (insertRecordsDuringTest()) {
            waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
            insertStringTypes();
        }
        waitForAvailableRecords();

        Testing.debug("Inserted");
        expectedRecordCount++;

        final SourceRecords records = consumeRecordsByTopic(expectedRecordCount);

        List<SourceRecord> testTableRecords = records.recordsForTopic("testdb.informix.type_string");
        assertThat(testTableRecords).hasSize(expectedRecordCount);
        SourceRecord record = testTableRecords.get(0);

        VerifyRecord.isValid(record);

        // insert
        if (insertRecordsDuringTest()) {
            VerifyRecord.isValidInsert(record, true);
        }
        else {
            VerifyRecord.isValidRead(record);
        }

        Struct after = (Struct) ((Struct) record.value()).get("after");
        assertRecord(after, EXPECTED_STRING);
    }

    @Test
    public void fpTypes() throws Exception {
        int expectedRecordCount = 0;

        if (insertRecordsDuringTest()) {
            waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
            insertFpTypes();
        }
        waitForAvailableRecords();

        Testing.debug("Inserted");
        expectedRecordCount++;

        final SourceRecords records = consumeRecordsByTopic(expectedRecordCount);

        List<SourceRecord> testTableRecords = records.recordsForTopic("testdb.informix.type_fp");
        assertThat(testTableRecords).hasSize(expectedRecordCount);
        SourceRecord record = testTableRecords.get(0);

        VerifyRecord.isValid(record);

        // insert
        if (insertRecordsDuringTest()) {
            VerifyRecord.isValidInsert(record, true);
        }
        else {
            VerifyRecord.isValidRead(record);
        }

        Struct after = (Struct) ((Struct) record.value()).get("after");
        assertRecord(after, EXPECTED_FP);
    }

    @Test
    @FixFor("DBZ-1552")
    public void fpTypesAsString() throws Exception {
        stopConnector();
        initializeConnectorTestFramework();
        final Configuration config = connectorConfig()
                .with(InformixConnectorConfig.DECIMAL_HANDLING_MODE, DecimalMode.STRING)
                .build();

        start(InformixConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        int expectedRecordCount = 0;

        if (insertRecordsDuringTest()) {
            waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
            insertFpTypes();
        }
        waitForAvailableRecords();

        Testing.debug("Inserted");
        expectedRecordCount++;

        final SourceRecords records = consumeRecordsByTopic(expectedRecordCount);

        List<SourceRecord> testTableRecords = records.recordsForTopic("testdb.informix.type_fp");
        assertThat(testTableRecords).hasSize(expectedRecordCount);
        SourceRecord record = testTableRecords.get(0);

        VerifyRecord.isValid(record);

        // insert
        if (insertRecordsDuringTest()) {
            VerifyRecord.isValidInsert(record, true);
        }
        else {
            VerifyRecord.isValidRead(record);
        }

        Struct after = (Struct) ((Struct) record.value()).get("after");
        assertRecord(after, EXPECTED_FP_AS_STRING);
    }

    @Test
    @FixFor("DBZ-1552")
    public void fpTypesAsDouble() throws Exception {
        stopConnector();
        initializeConnectorTestFramework();
        final Configuration config = connectorConfig()
                .with(InformixConnectorConfig.DECIMAL_HANDLING_MODE, DecimalMode.DOUBLE)
                .build();

        start(InformixConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        int expectedRecordCount = 0;

        if (insertRecordsDuringTest()) {
            waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
            insertFpTypes();
        }
        waitForAvailableRecords();

        Testing.debug("Inserted");
        expectedRecordCount++;

        final SourceRecords records = consumeRecordsByTopic(expectedRecordCount);

        List<SourceRecord> testTableRecords = records.recordsForTopic("testdb.informix.type_fp");
        assertThat(testTableRecords).hasSize(expectedRecordCount);
        SourceRecord record = testTableRecords.get(0);

        VerifyRecord.isValid(record);

        // insert
        if (insertRecordsDuringTest()) {
            VerifyRecord.isValidInsert(record, true);
        }
        else {
            VerifyRecord.isValidRead(record);
        }

        Struct after = (Struct) ((Struct) record.value()).get("after");
        assertRecord(after, EXPECTED_FP_AS_DOUBLE);
    }

    @Test
    public void intTypes() throws Exception {
        int expectedRecordCount = 0;

        if (insertRecordsDuringTest()) {
            waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
            insertIntTypes();
        }
        waitForAvailableRecords();

        Testing.debug("Inserted");
        expectedRecordCount++;

        final SourceRecords records = consumeRecordsByTopic(expectedRecordCount);

        List<SourceRecord> testTableRecords = records.recordsForTopic("testdb.informix.type_int");
        assertThat(testTableRecords).hasSize(expectedRecordCount);
        SourceRecord record = testTableRecords.get(0);

        VerifyRecord.isValid(record);

        // insert
        if (insertRecordsDuringTest()) {
            VerifyRecord.isValidInsert(record, true);
        }
        else {
            VerifyRecord.isValidRead(record);
        }

        Struct after = (Struct) ((Struct) record.value()).get("after");
        assertRecord(after, EXPECTED_INT);
    }

    @Test
    public void timeTypes() throws Exception {
        int expectedRecordCount = 0;

        if (insertRecordsDuringTest()) {
            waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
            insertTimeTypes();
        }
        waitForAvailableRecords();

        Testing.debug("Inserted");
        expectedRecordCount++;

        final SourceRecords records = consumeRecordsByTopic(expectedRecordCount);

        List<SourceRecord> testTableRecords = records.recordsForTopic("testdb.informix.type_time");
        assertThat(testTableRecords).hasSize(expectedRecordCount);
        SourceRecord record = testTableRecords.get(0);

        VerifyRecord.isValid(record);

        // insert
        if (insertRecordsDuringTest()) {
            VerifyRecord.isValidInsert(record, true);
        }
        else {
            VerifyRecord.isValidRead(record);
        }

        Struct after = (Struct) ((Struct) record.value()).get("after");
        assertRecord(after, EXPECTED_TIME);
    }

    @Test
    @FixFor("DBZ-3268")
    public void timeTypesAsAdaptiveMicroseconds() throws Exception {
        stopConnector();
        init(TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS);

        int expectedRecordCount = 0;

        if (insertRecordsDuringTest()) {
            waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
            insertTimeTypes();
        }
        waitForAvailableRecords();

        Testing.debug("Inserted");
        expectedRecordCount++;

        final SourceRecords records = consumeRecordsByTopic(expectedRecordCount);

        List<SourceRecord> testTableRecords = records.recordsForTopic("testdb.informix.type_time");
        assertThat(testTableRecords).hasSize(expectedRecordCount);
        SourceRecord record = testTableRecords.get(0);

        VerifyRecord.isValid(record);

        // insert
        if (insertRecordsDuringTest()) {
            VerifyRecord.isValidInsert(record, true);
        }
        else {
            VerifyRecord.isValidRead(record);
        }

        Struct after = (Struct) ((Struct) record.value()).get("after");
        assertRecord(after, EXPECTED_TIME_AS_ADAPTIVE);
    }

    @Test
    @FixFor("DBZ-3268")
    public void timeTypesAsConnect() throws Exception {
        stopConnector();
        init(TemporalPrecisionMode.CONNECT);

        int expectedRecordCount = 0;

        if (insertRecordsDuringTest()) {
            waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
            insertTimeTypes();
        }
        waitForAvailableRecords();

        Testing.debug("Inserted");
        expectedRecordCount++;

        final SourceRecords records = consumeRecordsByTopic(expectedRecordCount);

        List<SourceRecord> testTableRecords = records.recordsForTopic("testdb.informix.type_time");
        assertThat(testTableRecords).hasSize(expectedRecordCount);
        SourceRecord record = testTableRecords.get(0);

        VerifyRecord.isValid(record);

        // insert
        if (insertRecordsDuringTest()) {
            VerifyRecord.isValidInsert(record, true);
        }
        else {
            VerifyRecord.isValidRead(record);
        }

        Struct after = (Struct) ((Struct) record.value()).get("after");
        assertRecord(after, EXPECTED_TIME_AS_CONNECT);
    }

    @Test
    public void clobTypes() throws Exception {
        int expectedRecordCount = 0;

        if (insertRecordsDuringTest()) {
            waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
            insertClobTypes();
        }
        waitForAvailableRecords();

        Testing.debug("Inserted");
        expectedRecordCount++;

        SourceRecords records = consumeRecordsByTopic(expectedRecordCount);

        List<SourceRecord> testTableRecords = records.recordsForTopic("testdb.informix.type_clob");
        assertThat(testTableRecords).hasSize(expectedRecordCount);
        SourceRecord record = testTableRecords.get(0);

        VerifyRecord.isValid(record);

        // insert
        if (insertRecordsDuringTest()) {
            VerifyRecord.isValidInsert(record, true);
        }
        else {
            VerifyRecord.isValidRead(record);
        }

        Struct after = (Struct) ((Struct) record.value()).get("after");
        assertRecord(after, EXPECTED_CLOB);

        if (insertRecordsDuringTest()) {
            // Update clob types
            updateClobTypes();

            records = consumeRecordsByTopic(1);
            testTableRecords = records.recordsForTopic("testdb.informix.type_clob");
            assertThat(testTableRecords).hasSize(1);
            record = testTableRecords.get(0);

            VerifyRecord.isValid(record);
            VerifyRecord.isValidUpdate(record, true);

            after = (Struct) ((Struct) record.value()).get("after");
            assertRecord(after, EXPECTED_CLOB_UPDATE);
        }
    }

    protected static void insertStringTypes() throws SQLException {
        connection.execute("INSERT INTO type_string VALUES (0, 'vc', 'nvc', 'lvc', 'c', 'nc');");
    }

    protected static void insertFpTypes() throws SQLException {
        connection.execute("INSERT INTO type_fp VALUES (0, 1.1, 2.22, 3.333, 4.4444, 5.55555, 1234.567891, 1234.567891, 77.323, 77.323)");
    }

    protected static void insertIntTypes() throws SQLException {
        connection.execute(
                "INSERT INTO type_int VALUES (0, 1, 22, 333, 4444, 55555, 9999999999, 9999999999)");
    }

    protected static void insertTimeTypes() throws SQLException {
        connection.execute("INSERT INTO type_time VALUES ("
                + "0"
                + ", '2024-03-27'"
                + ", DATETIME(12:34:56) HOUR TO SECOND"
                + ", DATETIME(2024-03-27 12:34:56) YEAR TO SECOND"
                + ", DATETIME(2024-03-27 12:34:56.123) YEAR TO FRACTION"
                + ", DATETIME(2024-03-27 12:34:56.12345) YEAR TO FRACTION(5)"
                // + ", '-3-6'"
                // + ", '-123 12:34:56'"
                + ")");
    }

    protected static void insertClobTypes() throws SQLException {
        Clob clob1 = connection.connection().createClob();
        clob1.setString(0, part(CLOB_JSON, 0, 512));

        Clob clob2 = connection.connection().createClob();
        clob2.setString(0, part(CLOB_JSON, 0, 5000));

        connection.prepareUpdate("INSERT INTO type_clob VALUES (0, ?, ?, ?)", ps -> {
            ps.setString(1, CLOB_TXT);
            ps.setClob(2, clob1);
            ps.setClob(3, clob2);
        });
        connection.commit();
    }

    protected static void updateClobTypes() throws Exception {
        Clob clob1 = connection.connection().createClob();
        clob1.setString(0, part(CLOB_JSON, 1, 512));

        Clob clob2 = connection.connection().createClob();
        clob2.setString(0, part(CLOB_JSON, 1, 5000));

        connection.prepareUpdate("UPDATE type_clob SET VAL_CLOB_INLINE=?, VAL_CLOB_SHORT=?, VAL_CLOB_LONG=?" +
                " WHERE ID = 1", ps -> {
                    ps.setString(1, CLOB_TXT_UPDATE);
                    ps.setClob(2, clob1);
                    ps.setClob(3, clob2);
                });
        connection.commit();
    }

    private static String part(String text, int start, int length) {
        return text == null ? "" : text.substring(start, Math.min(length, text.length()));
    }

    private void assertRecord(Struct record, List<SchemaAndValueField> expected) {
        expected.forEach(schemaAndValueField -> schemaAndValueField.assertFor(record));
    }

}
