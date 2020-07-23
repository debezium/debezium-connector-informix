package laoflch.debezium.connector.informix.integrtest

import io.debezium.connector.db2.{Db2Connection, Db2ConnectorConfig, Db2DatabaseSchema, Db2ValueConverters}
import io.debezium.relational.{HistorizedRelationalDatabaseSchema, Table, TableId, TableSchemaBuilder}
import io.debezium.relational.ddl.DdlParser
import io.debezium.relational.history.TableChanges
import io.debezium.schema.SchemaChangeEvent.SchemaChangeEventType
import io.debezium.schema.{SchemaChangeEvent, TopicSelector}
import io.debezium.util.SchemaNameAdjuster
import org.slf4j.{Logger, LoggerFactory}

object InformixDatabaseSchema {
  private val LOGGER = LoggerFactory.getLogger(classOf[InformixDatabaseSchema])
}

class InformixDatabaseSchema(val connectorConfig: InformixConnectorConfig,
                        val schemaNameAdjuster: SchemaNameAdjuster,
                        val topicSelector: TopicSelector[TableId],
                        val connection: InformixConnection)
  extends HistorizedRelationalDatabaseSchema(
  connectorConfig,
  topicSelector,
  connectorConfig.getTableFilters.dataCollectionFilter,
  connectorConfig.getColumnFilter,
  new TableSchemaBuilder(new InformixValueConverters(connectorConfig.getDecimalMode,
    connectorConfig.getTemporalPrecisionMode),
    schemaNameAdjuster,
    connectorConfig.customConverterRegistry,
    connectorConfig.getSourceInfoStructMaker[SourceInfo].schema,
    connectorConfig.getSanitizeFieldNames),
  false,
  connectorConfig.getKeyMapper) {


  override def applySchemaChange(schemaChange: SchemaChangeEvent): Unit = {

    // just a single table per DDL event for
    val table = schemaChange.getTables.iterator.next
    buildAndRegisterSchema(table)
    tables.overwriteTable(table)
    //var tableChanges = null
    if (schemaChange.getType eq SchemaChangeEventType.CREATE) {
      record(schemaChange,(new TableChanges).create(table))

    }
    else if (schemaChange.getType eq SchemaChangeEventType.ALTER) {
      record(schemaChange,(new TableChanges).alter(table))
    }
  }

  override protected def getDdlParser: DdlParser = null
}
