/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.ConfigurationNames;
import io.debezium.connector.informix.InformixConnection;
import io.debezium.connector.informix.InformixConnectorConfig;
import io.debezium.embedded.async.AsyncEngineConfig;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.storage.file.history.FileSchemaHistory;
import io.debezium.util.Testing;

public class TestHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestHelper.class);

    public static final String TEST_DATABASE = "testdb";
    public static final String TEST_CONNECTOR = "informix_server";
    public static final Path SCHEMA_HISTORY_PATH = Testing.Files.createTestingPath("file-schema-history.txt").toAbsolutePath();

    /**
     * Key for schema parameter used to store a source column's type name.
     */
    public static final String TYPE_NAME_PARAMETER_KEY = "__debezium.source.column.type";

    /**
     * Key for schema parameter used to store a source column's type length.
     */
    public static final String TYPE_LENGTH_PARAMETER_KEY = "__debezium.source.column.length";

    /**
     * Key for schema parameter used to store a source column's type scale.
     */
    public static final String TYPE_SCALE_PARAMETER_KEY = "__debezium.source.column.scale";

    private static final String STATEMENTS_TABLE_PLACEHOLDER = "#";
    private static final String STATEMENTS_SCHEMA_PLACEHOLDER = "@";
    /**
     * Check if 'testdb' is enable database logging.
     */
    public static final String IS_CDC_ENABLED = "select name, is_logging, is_buff_log, is_ansi from sysmaster:sysdatabases where name='%s'";

    public static final String IFX_LOCK_MODE_WAIT = "IFX_LOCK_MODE_WAIT";

    public static JdbcConfiguration.Builder adminJdbcConfig() {
        return JdbcConfiguration.copy(Configuration.fromSystemProperties(ConfigurationNames.DATABASE_CONFIG_PREFIX))
                .withDefault(JdbcConfiguration.DATABASE, TEST_DATABASE)
                .withDefault(JdbcConfiguration.HOSTNAME, "localhost")
                .withDefault(JdbcConfiguration.PORT, 9088)
                .withDefault(JdbcConfiguration.USER, "informix")
                .withDefault(JdbcConfiguration.PASSWORD, "in4mix");
    }

    public static JdbcConfiguration.Builder defaultJdbcConfig() {
        return JdbcConfiguration.copy(Configuration.fromSystemProperties(ConfigurationNames.DATABASE_CONFIG_PREFIX))
                .withDefault(JdbcConfiguration.DATABASE, TEST_DATABASE)
                .withDefault(JdbcConfiguration.HOSTNAME, "localhost")
                .withDefault(JdbcConfiguration.PORT, 9088)
                .withDefault(JdbcConfiguration.USER, "informix")
                .withDefault(JdbcConfiguration.PASSWORD, "in4mix")
                .with(IFX_LOCK_MODE_WAIT, 10);
    }

    /**
     * Returns a default configuration suitable for most test cases. Can be amended/overridden in individual tests as
     * needed.
     */
    public static Configuration.Builder defaultConfig() {

        return Configuration.copy(defaultJdbcConfig().build().map(key -> ConfigurationNames.DATABASE_CONFIG_PREFIX + key))
                .with(CommonConnectorConfig.EXECUTOR_SHUTDOWN_TIMEOUT_MS, 30_000)
                .with(AsyncEngineConfig.TASK_MANAGEMENT_TIMEOUT_MS, 30_000)
                .with(CommonConnectorConfig.TOPIC_PREFIX, TEST_DATABASE)
                .with(CommonConnectorConfig.DRIVER_CONFIG_PREFIX + IFX_LOCK_MODE_WAIT, 10)
                .with(InformixConnectorConfig.SCHEMA_HISTORY, FileSchemaHistory.class)
                .with(FileSchemaHistory.FILE_PATH, SCHEMA_HISTORY_PATH)
                .with(InformixConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(InformixConnectorConfig.CDC_TIMEOUT, 0)
                .with(InformixConnectorConfig.CDC_BUFFERSIZE, 0x200);
    }

    public static InformixConnection adminConnection() {
        return new InformixConnection(TestHelper.adminJdbcConfig().build());
    }

    public static void dropTable(InformixConnection connection, String table) throws SQLException {
        connection.execute("drop table if exists " + table);
    }

    public static void dropTables(InformixConnection connection, String... tables) throws SQLException {
        for (String table : tables) {
            dropTable(connection, table);
        }
    }

    private static class LazyConnectionHolder {
        static final InformixConnection INSTANCE = new InformixConnection(TestHelper.defaultJdbcConfig().build());
    }

    public static InformixConnection testConnection() {
        return LazyConnectionHolder.INSTANCE;
    }

    /**
     * Check if 'testdb''s logging or buf_logging is enabled.
     */
    public static void assertCdcEnabled(InformixConnection conn) throws SQLException {

        Statement stmt = conn.connection().createStatement();
        ResultSet rs = stmt.executeQuery(String.format(IS_CDC_ENABLED, TEST_DATABASE));

        int is_logging = 0;
        int is_buff_logging = 0;

        while (rs.next()) {
            is_logging += rs.getInt("is_logging");
            is_buff_logging += rs.getInt("is_buff_log");
        }

        assertThat(is_logging + is_buff_logging).isPositive();
    }

}
