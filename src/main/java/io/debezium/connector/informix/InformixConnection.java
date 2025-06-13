/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.time.Instant;
import java.util.Optional;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.informix.jdbc.IfxDriver;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
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

    private static final String GET_MAX_LSN = "select uniqid, used as logpage from sysmaster:syslogs where is_current = 1";
    private static final String GET_MIN_LSN = "select min(uniqid) as uniqid , 0 as logpage from sysmaster:syslogs";

    private static final String GET_CURRENT_TIMESTAMP = "select sysdate as sysdate from sysmaster:sysdual";

    private static final String QUOTED_CHARACTER = ""; // Unless DELIMIDENT is set, database identifiers cannot be quoted

    private static final String URL_PATTERN = "jdbc:informix-sqli://${"
            + JdbcConfiguration.HOSTNAME + "}:${"
            + JdbcConfiguration.PORT + "}/${"
            + JdbcConfiguration.DATABASE + "}:user=${"
            + JdbcConfiguration.USER + "};password=${"
            + JdbcConfiguration.PASSWORD + "}";

    private static final ConnectionFactory FACTORY = JdbcConnection.patternBasedFactory(
            URL_PATTERN,
            IfxDriver.class.getCanonicalName(),
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

    private String retrieveRealDatabaseName() {
        try {
            return queryAndMap(GET_DATABASE_NAME, singleResultMapper(rs -> rs.getString(1), "Could not retrieve database name"));
        }
        catch (SQLException e) {
            throw new DebeziumException("Couldn't obtain database name", e);
        }
    }

    public String getRealDatabaseName() {
        return realDatabaseName;
    }

    /**
     * Calculates the highest available Log Sequence Number.
     * Tecnically, the _exact_ highest available LSN is not available in the JDBC session, but the current page of the active
     * logical log file is. Each logical log file is a 32-bit address space of 4k pages, thus the log position within the file is
     * the current page number bit-shifted 12 positions to the left.
     * This is also the logical log position the change stream client actually starts from if set to start from the 'CURRENT'
     * position (LSN = 0).
     * (The full logical sequence number is a 64-bit integer where the unique id of the logical log file is placed in the 32 most
     * significant bits.)
     *
     * @return the current highest log sequence number
     */
    public Lsn getMaxLsn() throws SQLException {
        return queryAndMap(GET_MAX_LSN, singleResultMapper(rs -> {
            final Lsn lsn = Lsn.of(rs.getLong("uniqid"), rs.getLong("logpage") << 12);
            LOGGER.trace("Current maximum lsn is {}", lsn.toLongString());
            return lsn;
        }, "Maximum LSN query must return exactly one value"));
    }

    /**
     * Calculates the lowest available Log Sequence Number.
     * @see InformixConnection#getMaxLsn()
     * @return the current lowest log sequence number
     */
    private Lsn getMinLsn() throws SQLException {
        return queryAndMap(GET_MIN_LSN, singleResultMapper(rs -> {
            final Lsn lsn = Lsn.of(rs.getLong("uniqid"), rs.getLong("logpage") << 12);
            LOGGER.trace("Current minimum lsn is {}", lsn.toLongString());
            return lsn;
        }, "Minimum LSN query must return exactly one value"));
    }

    public boolean validateLogPosition(Partition partition, OffsetContext offset, CommonConnectorConfig config) {
        final TxLogPosition lastPosition = ((InformixOffsetContext) offset).getChangePosition();
        final Lsn lastBeginLsn = lastPosition.getBeginLsn();
        final Lsn restartLsn = lastBeginLsn.isAvailable() ? lastBeginLsn : lastPosition.getCommitLsn();
        LOGGER.trace("Restart LSN is '{}'", restartLsn);

        try {
            final Lsn minLsn = getMinLsn();
            LOGGER.trace("Lowest available LSN is '{}'", minLsn);

            return restartLsn.isAvailable() && minLsn.isAvailable() && restartLsn.compareTo(minLsn) >= 0;
        }
        catch (SQLException e) {
            throw new DebeziumException("Couldn't obtain lowest available Log Sequence Number", e);
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
    public Optional<Boolean> nullsSortLast() {
        // "NULL values by default are ordered as less than values that are not NULL"
        // https://www.ibm.com/docs/en/informix-servers/14.10?topic=clause-ascending-descending-orders#ids_sqs_1055
        return Optional.of(false);
    }

    @Override
    public String quotedTableIdString(TableId tableId) {
        // Unless DELIMIDENT is set, database identifiers cannot be quoted
        StringBuilder quoted = new StringBuilder();

        if (!Strings.isNullOrBlank(tableId.catalog())) {
            quoted.append(InformixIdentifierQuoter.quoteIfNecessary(tableId.catalog())).append(':');
        }

        if (!Strings.isNullOrBlank(tableId.schema())) {
            quoted.append(InformixIdentifierQuoter.quoteIfNecessary(tableId.schema())).append('.');
        }

        return quoted.append(InformixIdentifierQuoter.quoteIfNecessary(tableId.table())).toString();
    }

    @Override
    public String quotedColumnIdString(String columnName) {
        // Unless DELIMIDENT is set, column names cannot be quoted
        return InformixIdentifierQuoter.quoteIfNecessary(columnName);
    }

    public DataSource datasource() {
        return new DataSource() {
            private PrintWriter logWriter;

            @Override
            public Connection getConnection() throws SQLException {
                return connection();
            }

            @Override
            public Connection getConnection(String username, String password) throws SQLException {
                JdbcConfiguration config = JdbcConfiguration.copy(config()).withUser(username).withPassword(password).build();
                return FACTORY.connect(config);
            }

            @Override
            public PrintWriter getLogWriter() {
                return this.logWriter;
            }

            @Override
            public void setLogWriter(PrintWriter out) {
                this.logWriter = out;
            }

            @Override
            public void setLoginTimeout(int seconds) {
                throw new UnsupportedOperationException("setLoginTimeout");
            }

            @Override
            public int getLoginTimeout() {
                return (int) config().getConnectionTimeout().toSeconds();
            }

            @Override
            public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
                return java.util.logging.Logger.getLogger("io.debezium.connector.informix");
            }

            @Override
            @SuppressWarnings("unchecked")
            public <T> T unwrap(Class<T> iface) throws SQLException {
                if (iface.isInstance(this)) {
                    return (T) this;
                }
                throw new SQLException("DataSource of type [" + getClass().getName() + "] cannot be unwrapped as [" + iface.getName() + "]");
            }

            @Override
            public boolean isWrapperFor(Class<?> iface) throws SQLException {
                return iface.isInstance(this);
            }
        };
    }

}
