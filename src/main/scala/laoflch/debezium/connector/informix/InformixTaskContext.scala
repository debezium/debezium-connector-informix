package laoflch.debezium.connector.informix

import io.debezium.connector.common.CdcSourceTaskContext
import io.debezium.schema.DataCollectionId

import java.util


/**
 * A state (context) associated with a Informix task
 *
 * @author laoflch Luo
 *
 */
class InformixTaskContext(val config: InformixConnectorConfig,
                          val schema: InformixDatabaseSchema)
  extends CdcSourceTaskContext(config.getContextName,
    config.getLogicalName,
    () => schema.tableIds.asInstanceOf[util.Collection[DataCollectionId]]) {
  // replace java schema::tableIds
}
