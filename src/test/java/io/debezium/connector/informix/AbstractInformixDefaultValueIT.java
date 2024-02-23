/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.config.Configuration;
import io.debezium.connector.informix.util.TestHelper;
import io.debezium.data.Envelope;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.junit.ConditionalFail;
import io.debezium.junit.Flaky;

/**
 * Abstract default value integration test.
 *
 * This test is extended by two variants, online and offline schema evolution. All tests should
 * be included in this class and therefore should pass both variants to make sure that the
 * schema evolution process works regardless of mode used by the user.
 *
 * @author Lars M Johansson, Chris Cranford
 */
public abstract class AbstractInformixDefaultValueIT extends AbstractConnectorTest {

    @Rule
    public TestRule conditionalFail = new ConditionalFail();

    private InformixConnection connection;
    private Configuration config;

    @Before
    public void before() throws SQLException {
        connection = TestHelper.testConnection();

        connection.execute("DROP TABLE IF EXISTS dv_test");

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
                    .execute("DROP TABLE IF EXISTS dv_test")
                    .close();
        }
    }

    @Test
    @FixFor("DBZ-4990")
    @Flaky("DBZ-7542")
    public void shouldHandleBooleanDefaultTypes() throws Exception {
        List<ColumnDefinition> columnDefinitions = List.of(
                new ColumnDefinition("val_boolean", "BOOLEAN",
                        "'t'", "'f'",
                        true, false,
                        AssertionType.FIELD_DEFAULT_EQUAL));

        shouldHandleDefaultValuesCommon(columnDefinitions);
    }

    @Test
    @FixFor("DBZ-4990")
    @Flaky("DBZ-7542")
    public void shouldHandleNumericDefaultTypes() throws Exception {
        // TODO: remove once https://github.com/Apicurio/apicurio-registry/issues/2990 is fixed
        if (VerifyRecord.isApucurioAvailable()) {
            skipAvroValidation();
        }

        List<ColumnDefinition> columnDefinitions = List.of(
                new ColumnDefinition("val_bigint", "BIGINT",
                        "1", "2",
                        1L, 2L,
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_integer", "INTEGER",
                        "1", "2",
                        1, 2,
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_smallint", "SMALLINT",
                        "1", "2",
                        (short) 1, (short) 2,
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_decimal", "DECIMAL(5,0)",
                        "314", "628",
                        BigDecimal.valueOf(314), BigDecimal.valueOf(628),
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_numeric", "NUMERIC(5,0)",
                        "314", "628",
                        BigDecimal.valueOf(314), BigDecimal.valueOf(628),
                        AssertionType.FIELD_DEFAULT_EQUAL));

        shouldHandleDefaultValuesCommon(columnDefinitions);
    }

    @Test
    @FixFor("DBZ-4990")
    @Flaky("DBZ-7542")
    public void shouldHandleFloatPointDefaultTypes() throws Exception {
        // TODO: remove once https://github.com/Apicurio/apicurio-registry/issues/2980 is fixed
        if (VerifyRecord.isApucurioAvailable()) {
            skipAvroValidation();
        }

        List<ColumnDefinition> columnDefinitions = Arrays.asList(
                new ColumnDefinition("val_double", "DOUBLE PRECISION",
                        "3.14", "6.28",
                        3.14d, 6.28d,
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_float", "FLOAT",
                        "3.14", "6.28",
                        3.14d, 6.28d,
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_real", "REAL",
                        "3.14", "6.28",
                        3.14f, 6.28f,
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_decimal", "DECIMAL(5,2)",
                        "3.14", "6.28",
                        BigDecimal.valueOf(3.14), BigDecimal.valueOf(6.28),
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_numeric", "NUMERIC(5,2)",
                        "3.14", "6.28",
                        BigDecimal.valueOf(3.14), BigDecimal.valueOf(6.28),
                        AssertionType.FIELD_DEFAULT_EQUAL));

        shouldHandleDefaultValuesCommon(columnDefinitions);
    }

    @Test
    @FixFor("DBZ-4990")
    @Flaky("DBZ-7542")
    public void shouldHandleCharacterDefaultTypes() throws Exception {
        List<ColumnDefinition> columnDefinitions = Arrays.asList(
                new ColumnDefinition("val_char", "char(5)",
                        "'YES'", "'NO'",
                        "YES  ", "NO   ",
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_varchar", "VARCHAR(100)",
                        "'hello'", "'world'",
                        "hello", "world",
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_lvarchar", "LVARCHAR(1000)",
                        "'hello'", "'world'",
                        "hello", "world",
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_nchar", "NCHAR(5)",
                        "'ON'", "'OFF'",
                        "ON   ", "OFF  ",
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_nvarchar", "NVARCHAR(100)",
                        "'cedric'", "'entertainer'",
                        "cedric", "entertainer",
                        AssertionType.FIELD_DEFAULT_EQUAL));

        shouldHandleDefaultValuesCommon(columnDefinitions);
    }

    @Test
    @FixFor("DBZ-4990")
    @Flaky("DBZ-7542")
    public void shouldHandleDateTimeDefaultTypes() throws Exception {
        List<ColumnDefinition> columnDefinitions = Arrays.asList(
                new ColumnDefinition("val_date", "DATE",
                        "'2024-01-01'", "'2024-01-02'",
                        19723, 19724,
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_time", "DATETIME HOUR TO SECOND",
                        "DATETIME(01:02:03) HOUR TO SECOND", "DATETIME(02:03:04) HOUR TO SECOND",
                        3723000000L, 7384000000L,
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_datetime", "DATETIME YEAR TO SECOND",
                        "DATETIME(2024-01-01 01:02:03) YEAR TO SECOND", "DATETIME(2024-01-02 01:02:03) YEAR TO SECOND",
                        1704070923000L, 1704157323000L,
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_timestamp", "DATETIME YEAR TO FRACTION",
                        "DATETIME(2024-01-01 01:02:03.003) YEAR TO FRACTION", "DATETIME(2024-01-02 01:02:03.003) YEAR TO FRACTION",
                        1704070923003L, 1704157323003L,
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_timestamp_us", "DATETIME YEAR TO FRACTION(5)",
                        "DATETIME(2024-01-01 01:02:03.00005) YEAR TO FRACTION(5)", "DATETIME(2024-01-02 01:02:03.00005) YEAR TO FRACTION(5)",
                        1704070923000050L, 1704157323000050L,
                        AssertionType.FIELD_DEFAULT_EQUAL));

        shouldHandleDefaultValuesCommon(columnDefinitions);
    }

    protected abstract void performSchemaChange(Configuration config, InformixConnection connection, String alterStatement) throws Exception;

    /**
     * Handles executing the full common set of default value tests for the supplied column definitions.
     *
     * @param columnDefinitions list of column definitions, should not be {@code null}
     * @throws Exception if an exception occurred
     */
    private void shouldHandleDefaultValuesCommon(List<ColumnDefinition> columnDefinitions) throws Exception {
        testDefaultValuesCreateTableAndSnapshot(columnDefinitions);
        testDefaultValuesAlterTableModifyExisting(columnDefinitions);
        testDefaultValuesAlterTableAdd(columnDefinitions);
        testDefaultValuesByRestartAndLoadingHistoryTopic();
    }

    /**
     * Restarts the connector and verifies when the database history topic is loaded that we can parse
     * all the loaded history statements without failures.
     */
    private void testDefaultValuesByRestartAndLoadingHistoryTopic() throws Exception {
        stopConnector();

        start(InformixConnector.class, config);
        assertConnectorIsRunning();

        waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
    }

    /**
     * Creates the table and pre-inserts a record captured during the snapshot phase.  The snapshot
     * record will be validated against the supplied column definitions.
     *
     * The goal of this method is to test that when a table is snapshot which uses default values
     * that both the in-memory schema representation and the snapshot pipeline change event have
     * the right default value resolution.
     *
     * @param columnDefinitions list of column definitions, should not be {@code null}
     * @throws Exception if an exception occurred
     */
    private void testDefaultValuesCreateTableAndSnapshot(List<ColumnDefinition> columnDefinitions) throws Exception {
        // Build SQL
        final StringBuilder createSql = new StringBuilder();
        createSql.append("CREATE TABLE dv_test (id INTEGER NOT NULL PRIMARY KEY");
        for (ColumnDefinition column : columnDefinitions) {
            createSql.append(", ")
                    .append(column.name)
                    .append(" ").append(column.definition)
                    .append(" ").append("DEFAULT ").append(column.addDefaultValue);
            createSql.append(", ")
                    .append(column.name).append("_null")
                    .append(" ").append(column.definition)
                    .append(" ").append("DEFAULT NULL");
            if (column.temporalType) {
                final String currentDefaultValue = column.getCurrentRegister();
                createSql.append(", ")
                        .append(column.name).append("_current")
                        .append(" ").append(column.definition)
                        .append(" ").append("DEFAULT ").append(currentDefaultValue);
                createSql.append(", ")
                        .append(column.name).append("_current_nonnull")
                        .append(" ").append(column.definition)
                        .append(" ").append("DEFAULT ").append(currentDefaultValue).append(" NOT NULL");
            }
        }
        createSql.append(')');

        // Create table and add cdc support
        connection.execute(createSql.toString());

        // Insert snapshot record
        connection.execute("INSERT INTO dv_test (id) VALUES (0)");

        // store config so it can be used by other methods
        config = TestHelper.defaultConfig()
                .with(InformixConnectorConfig.TABLE_INCLUDE_LIST, TestHelper.TEST_DATABASE + ".informix.dv_test")
                .with(InformixConnectorConfig.CDC_BUFFERSIZE, 0x800)
                .with(InformixConnectorConfig.TIME_PRECISION_MODE, TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS)
                .build();

        // start connector
        start(InformixConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        SourceRecords records = consumeRecordsByTopic(1);

        // Verify we got only 1 record for our test
        List<SourceRecord> tableRecords = records.recordsForTopic(TestHelper.TEST_DATABASE + ".informix.dv_test");
        assertThat(tableRecords).hasSize(1);
        assertNoRecordsToConsume();

        SourceRecord record = tableRecords.get(0);
        VerifyRecord.isValidRead(record, "id", 0);

        for (ColumnDefinition column : columnDefinitions) {
            switch (column.assertionType) {
                case FIELD_DEFAULT_EQUAL:
                    assertSchemaFieldWithSameDefaultAndValue(record, column.name.toLowerCase(), column.expectedAddDefaultValue);
                    assertSchemaFieldWithSameDefaultAndValue(record, column.name.toLowerCase() + "_null", null);
                    break;
                case FIELD_NO_DEFAULT:
                    assertSchemaFieldNoDefaultWithValue(record, column.name.toLowerCase(), column.expectedAddDefaultValue);
                    assertSchemaFieldNoDefaultWithValue(record, column.name.toLowerCase() + "_null", null);
                    break;
                default:
                    throw new RuntimeException("Unexpected assertion type: " + column.assertionType);
            }
            if (column.temporalType) {
                assertSchemaFieldWithDefaultCurrentDateTime(record, column.name.toLowerCase() + "_current", null);
                if (column.definition.equalsIgnoreCase("DATE")) {
                    assertSchemaFieldWithDefaultCurrentDateTime(record, column.name.toLowerCase() + "_current_nonnull", 0);
                }
                else {
                    assertSchemaFieldWithDefaultCurrentDateTime(record, column.name.toLowerCase() + "_current_nonnull", 0L);
                }
            }
        }

        waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        connection.execute("INSERT INTO dv_test (id) values (1)");

        records = consumeRecordsByTopic(1);
        tableRecords = records.recordsForTopic(TestHelper.TEST_DATABASE + ".informix.dv_test");
        assertThat(tableRecords).hasSize(1);
        assertNoRecordsToConsume();

        record = tableRecords.get(0);
        VerifyRecord.isValidInsert(record, "id", 1);
    }

    /**
     * Alters the underlying table changing the default value to its second form.  This method then inserts
     * a new record that is then validated against the supplied column definitions.
     *
     * The goal of this method is to test that when DDL modifies an existing column in an existing table
     * that the right default value resolution occurs and that the in-memory schema representation is
     * correct as well as the change event capture pipeline.
     *
     * @param columnDefinitions list of column definitions, should not be {@code null}
     * @throws Exception if an exception occurred
     */
    private void testDefaultValuesAlterTableModifyExisting(List<ColumnDefinition> columnDefinitions) throws Exception {
        // Build SQL
        final StringBuilder alterSql = new StringBuilder();
        alterSql.append("ALTER TABLE %table% MODIFY (");
        for (ColumnDefinition column : columnDefinitions) {
            alterSql.append(column.name).append(' ').append(column.definition)
                    .append(" DEFAULT ").append(column.modifyDefaultValue).append(", ")
                    .append(column.name).append("_null ").append(column.definition)
                    .append(" DEFAULT NULL, ");
        }
        alterSql.replace(alterSql.length() - 2, alterSql.length(), ")");

        performSchemaChange(config, connection, alterSql.toString());

        connection.execute("INSERT INTO dv_test (id) values (2)");

        SourceRecords records = consumeRecordsByTopic(1);

        // Verify we got only 1 record for our test
        List<SourceRecord> tableRecords = records.recordsForTopic(TestHelper.TEST_DATABASE + ".informix.dv_test");
        assertThat(tableRecords).hasSize(1);
        assertNoRecordsToConsume();

        SourceRecord record = tableRecords.get(0);
        VerifyRecord.isValidInsert(record, "id", 2);

        for (ColumnDefinition column : columnDefinitions) {
            switch (column.assertionType) {
                case FIELD_DEFAULT_EQUAL:
                    assertSchemaFieldWithSameDefaultAndValue(record, column.name.toLowerCase(), column.expectedModifyDefaultValue);
                    assertSchemaFieldWithSameDefaultAndValue(record, column.name.toLowerCase() + "_null", null);
                    break;
                case FIELD_NO_DEFAULT:
                    assertSchemaFieldNoDefaultWithValue(record, column.name.toLowerCase(), column.expectedModifyDefaultValue);
                    assertSchemaFieldNoDefaultWithValue(record, column.name.toLowerCase() + "_null", null);
                    break;
                default:
                    throw new RuntimeException("Unexpected assertion type: " + column.assertionType);
            }
            if (column.temporalType) {
                assertSchemaFieldWithDefaultCurrentDateTime(record, column.name.toLowerCase() + "_current", null);
                if (column.definition.equalsIgnoreCase("DATE")) {
                    assertSchemaFieldWithDefaultCurrentDateTime(record, column.name.toLowerCase() + "_current_nonnull", 0);
                }
                else {
                    assertSchemaFieldWithDefaultCurrentDateTime(record, column.name.toLowerCase() + "_current_nonnull", 0L);
                }
            }
        }
    }

    /**
     * Alters the underlying table changing adding a new column prefixed with {@code A} to each of the column
     * definition with the initial default value definition.
     *
     * The goal of this method is to test that when DDL adds a new column to an existing table that the right
     * default value resolution occurs and that the in-memory schema representation is correct as well as the
     * change event capture pipeline.
     *
     * @param columnDefinitions list of column definitions, should not be {@code null}
     * @throws Exception if an exception occurred
     */
    private void testDefaultValuesAlterTableAdd(List<ColumnDefinition> columnDefinitions) throws Exception {
        // Build SQL
        final StringBuilder alterSql = new StringBuilder();
        alterSql.append("ALTER TABLE %table% ");
        Iterator<ColumnDefinition> iterator = columnDefinitions.iterator();
        while (iterator.hasNext()) {
            final ColumnDefinition column = iterator.next();
            alterSql.append("ADD (")
                    .append("a").append(column.name)
                    .append(" ").append(column.definition)
                    .append(" ").append("DEFAULT ").append(column.addDefaultValue)
                    .append("), ");
            alterSql.append(" ADD (")
                    .append("a").append(column.name).append("_null")
                    .append(" ").append(column.definition)
                    .append(" ").append("DEFAULT NULL ")
                    .append("), ");
            if (column.temporalType) {
                final String currentDefaultValue = column.getCurrentRegister();
                alterSql.append(" ADD (")
                        .append("a").append(column.name).append("_current")
                        .append(" ").append(column.definition)
                        .append(" ").append("DEFAULT ").append(currentDefaultValue)
                        .append("), ");
                alterSql.append(" ADD (")
                        .append("a").append(column.name).append("_current_nonnull")
                        .append(" ").append(column.definition)
                        .append(" ").append("DEFAULT ").append(currentDefaultValue).append(" NOT NULL ")
                        .append("), ");
            }
        }
        alterSql.replace(alterSql.length() - 2, alterSql.length(), "");

        performSchemaChange(config, connection, alterSql.toString());

        connection.execute("INSERT INTO dv_test (id) values (3)");

        // TODO: ALTER TABLE ADD columns sometimes(!) result in 'ghost' inserts for all existing rows(?)
        SourceRecords records = consumeRecordsByTopic(4);
        assertNoRecordsToConsume();

        List<SourceRecord> tableRecords = records.recordsForTopic(TestHelper.TEST_DATABASE + ".informix.dv_test");

        SourceRecord record = tableRecords.get(tableRecords.size() - 1);
        VerifyRecord.isValidInsert(record, "id", 3);

        for (ColumnDefinition column : columnDefinitions) {
            switch (column.assertionType) {
                case FIELD_DEFAULT_EQUAL:
                    assertSchemaFieldWithSameDefaultAndValue(record, column.name.toLowerCase(), column.expectedModifyDefaultValue);
                    assertSchemaFieldWithSameDefaultAndValue(record, column.name.toLowerCase() + "_null", null);
                    assertSchemaFieldWithSameDefaultAndValue(record, "a" + column.name.toLowerCase(), column.expectedAddDefaultValue);
                    assertSchemaFieldWithSameDefaultAndValue(record, "a" + column.name.toLowerCase() + "_null", null);
                    break;
                case FIELD_NO_DEFAULT:
                    assertSchemaFieldNoDefaultWithValue(record, column.name.toLowerCase(), column.expectedModifyDefaultValue);
                    assertSchemaFieldNoDefaultWithValue(record, column.name.toLowerCase() + "_null", null);
                    assertSchemaFieldNoDefaultWithValue(record, "a" + column.name.toLowerCase(), column.expectedAddDefaultValue);
                    assertSchemaFieldNoDefaultWithValue(record, "a" + column.name.toLowerCase() + "_null", null);
                    break;
                default:
                    throw new RuntimeException("Unexpected assertion type: " + column.assertionType);
            }
            if (column.temporalType) {
                assertSchemaFieldWithDefaultCurrentDateTime(record, column.name.toLowerCase() + "_current", null);
                assertSchemaFieldWithDefaultCurrentDateTime(record, "a" + column.name.toLowerCase() + "_current", null);
                if (column.definition.equalsIgnoreCase("DATE")) {
                    assertSchemaFieldWithDefaultCurrentDateTime(record, column.name.toLowerCase() + "_current_nonnull", 0);
                    assertSchemaFieldWithDefaultCurrentDateTime(record, "a" + column.name.toLowerCase() + "_current_nonnull", 0);
                }
                else {
                    assertSchemaFieldWithDefaultCurrentDateTime(record, column.name.toLowerCase() + "_current_nonnull", 0L);
                    assertSchemaFieldWithDefaultCurrentDateTime(record, "a" + column.name.toLowerCase() + "_current_nonnull", 0L);
                }
            }
        }
    }

    /**
     * Asserts that the schema field's default value and after emitted event value are the same.
     *
     * @param record the change event record, never {@code null}
     * @param fieldName the field name, never {@code null}
     * @param expectedValue the expected value in the field's default and "after" struct
     */
    private static void assertSchemaFieldWithSameDefaultAndValue(SourceRecord record, String fieldName, Object expectedValue) {
        assertSchemaFieldValueWithDefault(record, fieldName, expectedValue, r -> {
            assertThat(r).as("Unexpected field value: " + fieldName).isEqualTo(expectedValue);
        });
    }

    /**
     * Asserts that the schema field's default value is not set and that the emitted event value matches.
     *
     * @param record the change event record, never {@code null}
     * @param fieldName the field name, never {@code null}
     * @param fieldValue the expected value in the field's "after" struct
     */
    // asserts that the field schema has no default value and an emitted value
    private static void assertSchemaFieldNoDefaultWithValue(SourceRecord record, String fieldName, Object fieldValue) {
        assertSchemaFieldValueWithDefault(record, fieldName, null, r -> {
            assertThat(r).as("Unexpected field value: " + fieldName).isEqualTo(fieldValue);
        });
    }

    private static void assertSchemaFieldValueWithDefault(SourceRecord record, String fieldName, Object expectedDefault, Consumer<Object> valueCheck) {
        final Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        final Field field = after.schema().field(fieldName);
        assertThat(field).as("Expected non-null field for " + fieldName).isNotNull();
        final Object defaultValue = field.schema().defaultValue();
        if (expectedDefault == null) {
            assertThat(defaultValue).isNull();
            return;
        }
        else {
            assertThat(defaultValue).as("Expected non-null default value for field " + fieldName).isNotNull();
        }
        assertThat(defaultValue.getClass()).isEqualTo(expectedDefault.getClass());
        assertThat(defaultValue).as("Unexpected default value: " + fieldName + " with field value: " + after.get(fieldName)).isEqualTo(expectedDefault);
        valueCheck.accept(after.get(fieldName));
    }

    private static void assertSchemaFieldWithDefaultCurrentDateTime(SourceRecord record, String fieldName, Object expectedValue) {
        assertSchemaFieldValueWithDefault(record, fieldName, expectedValue, v -> {
            if (expectedValue == null) {
                assertThat(v).isNull();
            }
            else if (expectedValue instanceof Long) {
                assertThat((Long) v).as("Unexpected field value: " + fieldName).isGreaterThan(1L);
            }
            else if (expectedValue instanceof Integer) {
                assertThat((Integer) v).as("Unexpected field value: " + fieldName).isGreaterThan(1);
            }
        });
    }

    private static void assertSchemaFieldDefaultAndNonNullValue(SourceRecord record, String fieldName, Object defaultValue) {
        assertSchemaFieldValueWithDefault(record, fieldName, defaultValue, r -> {
            assertThat(r).as("Unexpected field value: " + fieldName).isNotNull();
        });
    }

    /**
     * Defines the different assertion types for a given column definition.
     */
    enum AssertionType {
        // field and default values are identical
        FIELD_DEFAULT_EQUAL,
        // schema has no default value specified
        FIELD_NO_DEFAULT
    }

    /**
     * Defines a column definition and its attributes that are used by tests.
     */
    private static class ColumnDefinition {
        public final String name;
        public final String definition;
        public final String addDefaultValue;
        public final String modifyDefaultValue;
        public final Object expectedAddDefaultValue;
        public final Object expectedModifyDefaultValue;
        public final AssertionType assertionType;
        public final boolean temporalType;

        ColumnDefinition(String name, String definition, String addDefaultValue, String modifyDefaultValue,
                         Object expectedAddDefaultValue, Object expectedModifyDefaultValue, AssertionType assertionType) {
            this.name = name;
            this.definition = definition;
            this.addDefaultValue = addDefaultValue;
            this.modifyDefaultValue = modifyDefaultValue;
            this.expectedAddDefaultValue = expectedAddDefaultValue;
            this.expectedModifyDefaultValue = expectedModifyDefaultValue;
            this.assertionType = assertionType;
            this.temporalType = definition.toUpperCase().startsWith("DATE");
        }

        public String getCurrentRegister() {
            if (definition.equalsIgnoreCase("DATE")) {
                return "TODAY";
            }
            else if (definition.toUpperCase().startsWith("DATETIME")) {
                if (definition.toUpperCase().contains("FRACTION")) {
                    return "SYSDATE" + definition.toUpperCase().substring(8);
                }
                else {
                    return "CURRENT" + definition.toUpperCase().substring(8);
                }
            }
            else {
                throw new RuntimeException("Unexpected temporal type for current time register: " + definition);
            }
        }
    }

}
