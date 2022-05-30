/*
 * Copyright Debezium-Informix-Connector Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package laoflch.debezium.connector.informix;

import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;

public class InformixTopicSelector {
    public static TopicSelector<TableId> defaultSelector(InformixConnectorConfig connectorConfig) {
        return TopicSelector.defaultSelector(connectorConfig,
                (tableId, prefix, delimiter) -> String.join(delimiter, prefix, tableId.schema(), tableId.table()));
    }
}
