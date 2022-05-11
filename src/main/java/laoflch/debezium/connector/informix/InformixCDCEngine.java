package laoflch.debezium.connector.informix;

import com.informix.jdbcx.IfxDataSource;
import com.informix.stream.api.IfmxStreamRecord;
import com.informix.stream.cdc.IfxCDCEngine;
import com.informix.stream.impl.IfxStreamException;
import io.debezium.config.Configuration;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class InformixCDCEngine {

    private static Logger LOGGER = LoggerFactory.getLogger(InformixCDCEngine.class);

    private static String URL_PATTERN = "jdbc:informix-sqli://%s:%s/syscdcv1:user=%s;password=%s;";

    private String host;
    private String port;
    private String user;
    private String password;

    long lsn;
    int timeOut;
    boolean hasInit;

    private IfxCDCEngine cdcEngine;

    private Map<Integer, TableId> labelId_tableId_map;

    public InformixCDCEngine(Configuration config) {
        cdcEngine = null;
        lsn = 0;
        timeOut = 5;
        hasInit = false;

        host = config.getString(JdbcConfiguration.HOSTNAME);
        port = config.getString(JdbcConfiguration.PORT);
        user = config.getString(JdbcConfiguration.USER);
        password = config.getString(JdbcConfiguration.PASSWORD);
        // new InformixCDCEngineJava(host, port, user, password);

        // TODO: try HPPC or FastUtils's Integer Map?
        labelId_tableId_map = new Hashtable<>();
    }

    public void init(InformixDatabaseSchema schema) {
        try {
            String url = InformixCDCEngine.genURLStr(host, port, user, password);
            this.cdcEngine = this.buildCDCEngine(url, this.lsn, this.timeOut, schema);

            LOGGER.info("Connecting to Informix URL: {}", url);

            this.cdcEngine.init();
            this.hasInit = true;
        } catch (SQLException ex) {
            LOGGER.error("Caught SQLException: {}", ex);
        } catch (IfxStreamException ex) {
            LOGGER.error("Caught IfxStreamException: {}", ex);
        }
    }

    public IfxCDCEngine buildCDCEngine(String url, Long lsn, int timeOut, InformixDatabaseSchema schema) throws SQLException {
        IfxDataSource ds = new IfxDataSource(url);
        IfxCDCEngine.Builder builder = new IfxCDCEngine.Builder(ds);

        // TODO: Make an parameter 'buffer size' for better performance. Default value is 10240
        builder.buffer(819200);

        schema.tableIds().stream().forEach((TableId tid) -> {
            List<String> cols = schema.tableFor(tid).columns().stream()
                    .map(col -> col.name())
                    .collect(Collectors.toList());

            String tname = tid.catalog() + ":" + tid.schema() + "." + tid.table();
            builder.watchTable(tname, cols.toArray(new String[0]));
        });

        if (lsn > 0) {
            builder.sequenceId(lsn);
        }
        builder.timeout(timeOut);

        /*
         * Build Map of Label_id to TableId.
         */
        for (IfxCDCEngine.IfmxWatchedTable tbl: builder.getWatchedTables()) {
            TableId tid = new TableId(tbl.getDatabaseName(), tbl.getNamespace(), tbl.getTableName());
            labelId_tableId_map.put(tbl.getLabel(), tid);
            LOGGER.info("Added WatchedTable : label={} -> tableId={}", tbl.getLabel(), tid);
        }

        return builder.build();
    }

    public Long setStartLsn(Long fromLsn) {
        this.lsn = fromLsn;
        return fromLsn;
    }

    public Map<Integer, TableId> convertLabel2TableId() {
        return this.labelId_tableId_map;
    }

    public void stream(StreamHandler streamHandler) throws InterruptedException, SQLException, IfxStreamException {
        while (streamHandler.accept(cdcEngine.getRecord())) {

        }
    }

    public IfxCDCEngine getCdcEngine() {
        return cdcEngine;
    }

    public static String genURLStr(String host, String port, String user, String password) {
        return String.format(URL_PATTERN, host, port, user, password);
    }

    public static InformixCDCEngine build(Configuration config) {
        return new InformixCDCEngine(config);
    }

    public interface StreamHandler {

        boolean accept(IfmxStreamRecord record) throws SQLException, IfxStreamException, InterruptedException;

    }
}
