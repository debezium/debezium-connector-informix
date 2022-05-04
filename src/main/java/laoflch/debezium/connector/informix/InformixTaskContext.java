package laoflch.debezium.connector.informix;

import io.debezium.connector.common.CdcSourceTaskContext;

public class InformixTaskContext extends CdcSourceTaskContext {
    public InformixTaskContext(InformixConnectorConfig config, InformixDatabaseSchema schema) {
        super(config.getContextName(), config.getLogicalName(), schema::tableIds);
    }
}
