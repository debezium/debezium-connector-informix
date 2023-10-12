/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import io.debezium.connector.common.CdcSourceTaskContext;

/**
 * A state (context) associated with an Informix task
 *
 * @author Jiri Pechanec, Lars M Johansson
 *
 */
public class InformixTaskContext extends CdcSourceTaskContext {
    public InformixTaskContext(InformixConnectorConfig config, InformixDatabaseSchema schema) {
        super(config.getContextName(), config.getLogicalName(), config.getCustomMetricTags(), schema::tableIds);
    }
}
