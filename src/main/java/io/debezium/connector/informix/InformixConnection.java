/*
 * Copyright Debezium-Informix-Connector Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.informix;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;

public class InformixConnection extends JdbcConnection {

    private static Logger LOGGER = LoggerFactory.getLogger(InformixConnection.class);

    private static String URL_PATTERN = "jdbc:informix-sqli://${" +
            JdbcConfiguration.HOSTNAME + "}:${" +
            JdbcConfiguration.PORT + "}/${" +
            JdbcConfiguration.DATABASE +
            "}:user=${" + JdbcConfiguration.USER +
            "};password=${" + JdbcConfiguration.PASSWORD + "}";

    private static ConnectionFactory FACTORY = JdbcConnection.patternBasedFactory(URL_PATTERN,
            com.informix.jdbc.IfxDriver.class.getName(),
            InformixConnection.class.getClassLoader());

    private String realDatabaseName;
    private InformixCDCEngine cdcEngine;

    public InformixConnection(Configuration config) {
        super(config, FACTORY);

        realDatabaseName = config.getString(JdbcConfiguration.DATABASE);
        cdcEngine = InformixCDCEngine.build(config);
    }

    public InformixCDCEngine getCdcEngine() {
        return cdcEngine;
    }

    public String getRealDatabaseName() {
        return realDatabaseName;
    }
}
