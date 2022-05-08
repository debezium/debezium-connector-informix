package laoflch.debezium.connector.informix;

import io.debezium.config.Configuration;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private Long lsn;
    private Integer transactionID;

    public InformixConnection(Configuration config) {
        super(config, FACTORY);

        realDatabaseName = config.getString(JdbcConfiguration.DATABASE);
        cdcEngine = InformixCDCEngine.build(config);
        lsn = 0L;
        transactionID = 0;
    }

    public InformixCDCEngine getCdcEngine() {
        return cdcEngine;
    }

    public String getRealDatabaseName() {
        return realDatabaseName;
    }
}
