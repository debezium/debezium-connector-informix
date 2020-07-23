package laoflch.debezium.connector.informix.integrtest

import java.util

import io.debezium.connector.common.CdcSourceTaskContext
import io.debezium.schema.DataCollectionId


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
    ()=>schema.tableIds.asInstanceOf[util.Collection[DataCollectionId]]) {
  // replace java schema::tableIds
}