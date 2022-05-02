package laoflch.debezium.connector.informix

import io.debezium.relational.TableId
import io.debezium.schema.TopicSelector


/**
 * The topic naming strategy based on connector configuration and table name
 *
 * @author laoflch Luo
 *
 */
object InformixTopicSelector {
  def defaultSelector(connectorConfig: InformixConnectorConfig): TopicSelector[TableId] =
    TopicSelector.defaultSelector(connectorConfig,
      (tableId: TableId, prefix: String, delimiter: String) =>
        String.join(delimiter, prefix, tableId.schema, tableId.table))
}
