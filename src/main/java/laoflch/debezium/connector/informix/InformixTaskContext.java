/*
 * Copyright Debezium-Informix-Connector Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package laoflch.debezium.connector.informix;

import io.debezium.connector.common.CdcSourceTaskContext;

public class InformixTaskContext extends CdcSourceTaskContext {
    public InformixTaskContext(InformixConnectorConfig config, InformixDatabaseSchema schema) {
        super(config.getContextName(), config.getLogicalName(), schema::tableIds);
    }
}
