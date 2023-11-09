/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import java.sql.SQLException;
import java.time.Instant;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.informix.jdbc.IfxDriver;

import io.debezium.config.Configuration;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import io.debezium.util.Strings;

/**
 * {@link JdbcConnection} extension to be used with IBM Informix
 *
 * @author Laoflch Luo, Xiaolin Zhang, Lars M Johansson
 *
 */
public class InformixConnection extends JdbcConnection {

    private static final Logger LOGGER = LoggerFactory.getLogger(InformixConnection.class);

    private static final String GET_DATABASE_NAME = "select dbinfo('dbname') as dbname from systables where tabid = 1";

    private static final String GET_MAX_LSN = "select max(seqnum) as seqnum from syscdcv1:informix.syscdctabs where dbname = '%s'";

    private static final String GET_CURRENT_TIMESTAMP = "select sysdate as sysdate from sysmaster:sysdual";

    private static final String QUOTED_CHARACTER = ""; // TODO: Unless DELIMIDENT is set, column names cannot be quoted

    private static final String URL_PATTERN = "jdbc:informix-sqli://${"
            + JdbcConfiguration.HOSTNAME + "}:${"
            + JdbcConfiguration.PORT + "}/${"
            + JdbcConfiguration.DATABASE + "}:user=${"
            + JdbcConfiguration.USER + "};password=${"
            + JdbcConfiguration.PASSWORD + "}";

    private static final ConnectionFactory FACTORY = JdbcConnection.patternBasedFactory(
            URL_PATTERN,
            IfxDriver.class.getName(),
            InformixConnection.class.getClassLoader(),
            JdbcConfiguration.PORT.withDefault(InformixConnectorConfig.PORT.defaultValueAsString()));

    /**
     * actual name of the database, which could differ in casing from the database name given in the connector config.
     */
    private final String realDatabaseName;

    /**
     * Creates a new connection using the supplied configuration.
     *
     * @param config {@link Configuration} instance, may not be null.
     */
    public InformixConnection(JdbcConfiguration config) {
        super(config, FACTORY, QUOTED_CHARACTER, QUOTED_CHARACTER);
        realDatabaseName = retrieveRealDatabaseName().trim();
    }

    /**
     * @return the current largest log sequence number
     */
    public Lsn getMaxLsn() {
        /*
         * return queryAndMap(GET_MAX_LSN, singleResultMapper(rs -> {
         * final Lsn lsn = Lsn.valueOf(rs.getLong(1));
         * LOGGER.trace("Current maximum lsn is {}", lsn);
         * return lsn;
         * }, "Maximum LSN query must return exactly one value"));
         */
        return Lsn.valueOf(0x00L);
    }

    public String getRealDatabaseName() {
        return realDatabaseName;
    }

    private String retrieveRealDatabaseName() {
        try {
            return queryAndMap(GET_DATABASE_NAME, singleResultMapper(rs -> rs.getString(1), "Could not retrieve database name"));
        }
        catch (SQLException e) {
            throw new RuntimeException("Couldn't obtain database name", e);
        }
    }

    /**
     * Returns a JDBC connection string for the current configuration.
     *
     * @return a {@code String} where the variables in {@code urlPattern} are replaced with values from the configuration
     */
    public String connectionString() {
        return connectionString(URL_PATTERN);
    }

    @Override
    public Optional<Instant> getCurrentTimestamp() throws SQLException {
        return queryAndMap(GET_CURRENT_TIMESTAMP, rs -> rs.next() ? Optional.of(rs.getTimestamp(1).toInstant()) : Optional.empty());
    }

    @Override
    public String quotedTableIdString(TableId tableId) {
        // TODO: Unless DELIMIDENT is set, table names cannot be quoted
        StringBuilder builder = new StringBuilder();

        String catalogName = tableId.catalog();
        if (!Strings.isNullOrBlank(catalogName)) {
            builder.append(catalogName).append(':');
        }

        String schemaName = tableId.schema();
        if (!Strings.isNullOrBlank(schemaName)) {
            builder.append(schemaName).append('.');
        }

        return builder.append(tableId.table()).toString();
    }

    @Override
    public String quotedColumnIdString(String columnName) {
        // TODO: Unless DELIMIDENT is set, column names cannot be quoted
        return columnName;
    }
}
