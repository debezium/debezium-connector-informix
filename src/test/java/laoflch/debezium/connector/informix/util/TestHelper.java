/*
 * Copyright Debezium-Informix-Connector Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package laoflch.debezium.connector.informix.util;

import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.history.FileDatabaseHistory;
import io.debezium.util.Testing;

import laoflch.debezium.connector.informix.InformixConnection;
import laoflch.debezium.connector.informix.InformixConnectorConfig;

public class TestHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestHelper.class);

    public static final String TEST_DATABASE = "testdb";
    public static final Path DB_HISTORY_PATH = Testing.Files.createTestingPath("file-db-history-connect.txt").toAbsolutePath();

    /**
     * Check if 'testdb' is enable database logging.
     */
    public static final String IS_CDC_ENABLED = "select name, is_logging, is_buff_log, is_ansi from sysmaster:sysdatabases where name='testdb'";

    public static JdbcConfiguration adminJdbcConfig() {
        // TODO: Fix parameter from properties
        return JdbcConfiguration.copy(Configuration.fromSystemProperties("database."))
                .withDefault(JdbcConfiguration.DATABASE, "testdb")
                .withDefault(JdbcConfiguration.HOSTNAME, "localhost")
                .withDefault(JdbcConfiguration.PORT, 9088)
                .withDefault(JdbcConfiguration.USER, "informix")
                .withDefault(JdbcConfiguration.PASSWORD, "in4mix")
                .build();
    }

    public static JdbcConfiguration defaultJdbcConfig() {
        // TODO: Fix parameter from properties
        return JdbcConfiguration.copy(Configuration.fromSystemProperties("database."))
                .withDefault(JdbcConfiguration.DATABASE, TEST_DATABASE)
                .withDefault(JdbcConfiguration.HOSTNAME, "localhost")
                .withDefault(JdbcConfiguration.PORT, 9088)
                .withDefault(JdbcConfiguration.USER, "informix")
                .withDefault(JdbcConfiguration.PASSWORD, "in4mix")
                .build();
    }

    /**
     * Returns a default configuration suitable for most test cases. Can be amended/overridden in individual tests as
     * needed.
     */
    public static Configuration.Builder defaultConfig() {
        JdbcConfiguration jdbcConfiguration = defaultJdbcConfig();
        Configuration.Builder builder = Configuration.create();

        jdbcConfiguration.forEach(
                (field, value) -> builder.with(InformixConnectorConfig.DATABASE_CONFIG_PREFIX + field, value));

        return builder.with(InformixConnectorConfig.SERVER_NAME, "testdb")
                .with(InformixConnectorConfig.DATABASE_HISTORY, FileDatabaseHistory.class)
                .with(FileDatabaseHistory.FILE_PATH, DB_HISTORY_PATH)
                .with(InformixConnectorConfig.INCLUDE_SCHEMA_CHANGES, false);
    }

    public static InformixConnection adminConnection() {
        return new InformixConnection(TestHelper.adminJdbcConfig());
    }

    public static InformixConnection testConnection() {
        return new InformixConnection(TestHelper.defaultJdbcConfig());
    }

    /**
     * Check if 'testdb''s logging or buf_logging is enabled.
     */
    public static void isCdcEnabled(InformixConnection conn) throws SQLException {

        Statement stmt = conn.connection().createStatement();
        ResultSet rs = stmt.executeQuery(IS_CDC_ENABLED);

        int is_logging = -1;
        int is_buff_logging = -1;

        while (rs.next()) {
            is_logging = rs.getInt("is_logging");
            is_buff_logging = rs.getInt("is_buff_log");
        }

        // Assert.assertFalse(is_logging != 1 && is_buff_logging != 1, "Logging or Buf_Logging should enabled for 'testdb'");
    }
}
