package laoflch.debezium.connector.informix

import io.debezium.pipeline.EventDispatcher
import io.debezium.pipeline.source.AbstractSnapshotChangeEventSource
import io.debezium.pipeline.source.spi.{ChangeEventSource, SnapshotProgressListener}
import io.debezium.pipeline.spi.OffsetContext
import io.debezium.pipeline.txmetadata.TransactionContext
import io.debezium.relational.{RelationalSnapshotChangeEventSource, Table, TableId}
import io.debezium.schema.SchemaChangeEvent
import io.debezium.schema.SchemaChangeEvent.SchemaChangeEventType
import io.debezium.util.Clock
import laoflch.debezium.connector.informix.InformixConnectorConfig.SnapshotIsolationMode
import org.slf4j.LoggerFactory

import java.sql.{Connection, SQLException, Savepoint}
import java.util
import java.util.{Optional, Set}
import scala.jdk.CollectionConverters


object InformixSnapshotChangeEventSource {

  @throws[SQLException]
  class InformixSnapshotContext(catalogName: String) extends RelationalSnapshotChangeEventSource.RelationalSnapshotContext(catalogName) {
    var isolationLevelBeforeStart = 0
    var preSchemaSnapshotSavepoint: Savepoint = null
  }
}


class InformixSnapshotChangeEventSource(connectorConfig: InformixConnectorConfig, previousOffset: InformixOffsetContext, jdbcConnection: InformixConnection, schema: InformixDatabaseSchema,
                                        dispatcher: EventDispatcher[TableId], clock: Clock, snapshotProgressListener: SnapshotProgressListener)
  extends RelationalSnapshotChangeEventSource(connectorConfig, previousOffset, jdbcConnection, schema,
    dispatcher, clock, snapshotProgressListener) {

  private val LOGGER = LoggerFactory.getLogger(classOf[InformixSnapshotChangeEventSource])

  /*private val connectorConfig = connectorConfig
  private val jdbcConnection = jdbcConnection*/


  override protected def getSnapshottingTask(previousOffset: OffsetContext): AbstractSnapshotChangeEventSource.SnapshottingTask = {
    var snapshotSchema = true
    var snapshotData = true
    // found a previous offset and the earlier snapshot has completed
    if (previousOffset != null && !previousOffset.isSnapshotRunning) {
      LOGGER.info("A previous offset indicating a completed snapshot has been found. Neither schema nor data will be snapshotted.")
      snapshotSchema = false
      snapshotData = false
    }
    else {
      LOGGER.info("No previous offset has been found")
      if (connectorConfig.getSnapshotMode.includeData)
        LOGGER.info("According to the connector configuration both schema and data will be snapshotted")
      else
        LOGGER.info("According to the connector configuration only schema will be snapshotted")
      snapshotData = connectorConfig.getSnapshotMode.includeData
    }
    new AbstractSnapshotChangeEventSource.SnapshottingTask(snapshotSchema, snapshotData)
  }

  @throws[Exception]
  override protected def prepare(context: ChangeEventSource.ChangeEventSourceContext) = new InformixSnapshotChangeEventSource.InformixSnapshotContext(jdbcConnection.getRealDatabaseName)

  @throws[Exception]
  override protected def connectionCreated(snapshotContext: RelationalSnapshotChangeEventSource.RelationalSnapshotContext): Unit = {
    snapshotContext.asInstanceOf[InformixSnapshotChangeEventSource.InformixSnapshotContext].isolationLevelBeforeStart = jdbcConnection.connection.getTransactionIsolation
  }

  @throws[Exception]
  override protected def getAllTableIds(ctx: RelationalSnapshotChangeEventSource.RelationalSnapshotContext): Set[TableId] = {
    jdbcConnection.readTableNames(ctx.catalogName, null, null, Array[String]("TABLE"))
  }

  @throws[SQLException]
  @throws[InterruptedException]
  override protected def lockTablesForSchemaSnapshot(sourceContext: ChangeEventSource.ChangeEventSourceContext, snapshotContext: RelationalSnapshotChangeEventSource.RelationalSnapshotContext): Unit = {
    if (connectorConfig.getSnapshotIsolationMode == SnapshotIsolationMode.READ_UNCOMMITTED) {
      jdbcConnection.connection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED)
      LOGGER.info("Schema locking was disabled in connector configuration")
    }
    else if (connectorConfig.getSnapshotIsolationMode == SnapshotIsolationMode.READ_COMMITTED) {
      jdbcConnection.connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED)
      LOGGER.info("Schema locking was disabled in connector configuration")
    }
    else if ( /*(connectorConfig.getSnapshotIsolationMode == SnapshotIsolationMode.EXCLUSIVE) ||*/
      (connectorConfig.getSnapshotIsolationMode == SnapshotIsolationMode.REPEATABLE_READ)) {
      jdbcConnection.connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ)
      /*snapshotContext.asInstanceOf[InformixSnapshotChangeEventSource.InformixSnapshotContext].preSchemaSnapshotSavepoint =
        jdbcConnection.connection.setSavepoint("informix_schema_snapshot")
      LOGGER.info("Executing schema locking")
      try {
        val statement = jdbcConnection.connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
        try {
         // import scala.collection.JavaConversions._
          for (tableId <- CollectionConverters.SetHasAsScala(snapshotContext.capturedTables).asScala) {
            if (!sourceContext.isRunning) throw new InterruptedException("Interrupted while locking table " + tableId)
            LOGGER.info("Locking table {}", tableId)
            val query = String.format("SELECT * FROM %s.%s WHERE 0=1 WITH CS", tableId.schema, tableId.table)
            statement.executeQuery(query).close()
          }
         }finally
          {
            if (statement != null) statement.close()
          }
        }*/

    }
    else throw new IllegalStateException("Unknown locking mode specified.")
  }

  @throws[SQLException]
  override protected def releaseSchemaSnapshotLocks(snapshotContext: RelationalSnapshotChangeEventSource.RelationalSnapshotContext): Unit = { // Exclusive mode: locks should be kept until the end of transaction.
    // read_uncommitted mode; read_committed mode: no locks have been acquired.
    if (connectorConfig.getSnapshotIsolationMode eq SnapshotIsolationMode.REPEATABLE_READ) {
      //jdbcConnection.connection.rollback(snapshotContext.asInstanceOf[InformixSnapshotChangeEventSource.InformixSnapshotContext].preSchemaSnapshotSavepoint)
      LOGGER.info("Schema locks released.")
    }
  }

  @throws[Exception]
  override protected def determineSnapshotOffset(ctx: RelationalSnapshotChangeEventSource.RelationalSnapshotContext): Unit = {
    val offset = new InformixOffsetContext(connectorConfig, TxLogPosition.NULL, false, false, TransactionContext.load(new util.HashMap[String, Any]()))
    // offset.setCDCEngine(jdbcConnection.getCDCEngine())
    ctx.offset = offset
  }

  @throws[SQLException]
  @throws[InterruptedException]
  override protected def readTableStructure(sourceContext: ChangeEventSource.ChangeEventSourceContext,
                                            snapshotContext: RelationalSnapshotChangeEventSource.RelationalSnapshotContext): Unit = {

    LOGGER.info("snapshotContext.capturedTables={}", snapshotContext.capturedTables)
    val schemas = CollectionConverters.SetHasAsScala(snapshotContext.capturedTables).asScala.map[String]((x: TableId) => x.schema)
    LOGGER.info("readTableStructure, schema={}", schemas)

    // reading info only for the schemas we're interested in as per the set of captured tables;
    // while the passed table name filter alone would skip all non-included tables, reading the schema
    // would take much longer that way

    for (schema <- schemas) {
      if (!sourceContext.isRunning) throw new InterruptedException("Interrupted while reading structure of schema " + schema)
      LOGGER.info("Reading structure of schema '{}'", schema)
      jdbcConnection.readSchema(snapshotContext.tables,
        snapshotContext.catalogName,
        schema,
        connectorConfig.getTableFilters.dataCollectionFilter,
        null, false)
    }

    // add tables to offset use by cdc lsn

/*
    jdbcConnection.getCDCEngine().setTableColsMap(
      CollectionConverters.SetHasAsScala(snapshotContext.tables.tableIds()).asScala.map[(String, CDCTabeEntry)](
        x => (x.catalog() + ":" + x.schema() + ":" + x.table() ->
          CDCTabeEntry(x,
            CollectionConverters
              .ListHasAsScala(snapshotContext.tables.forTable(x.catalog(), x.schema(), x.table()).columns())
              .asScala
              .map[String](x => x.name()).toSeq)
          ))
        .toMap
    )
*/

    /* def extractColsFormTable(cols:java.util.List[Column]): Seq[String] ={
       CollectionConverters.ListHasAsScala(cols).asScala.map[String](x=>x.name()).toSeq
     }*/

    //snapshotContext.offset.asInstanceOf[InformixOffsetContext].getCDCEngine().watchTableAndCols=watchTableAndCols

  }

  @throws[SQLException]
  override protected def getCreateTableEvent(snapshotContext: RelationalSnapshotChangeEventSource.RelationalSnapshotContext,
                                             table: Table) =
    new SchemaChangeEvent(snapshotContext.offset.getPartition,
      snapshotContext.offset.getOffset,
      snapshotContext.catalogName,
      table.id.schema,
      null, table, SchemaChangeEventType.CREATE, true)

  override protected def complete(snapshotContext: AbstractSnapshotChangeEventSource.SnapshotContext): Unit = {
    try jdbcConnection.connection.setTransactionIsolation(
      snapshotContext.asInstanceOf[InformixSnapshotChangeEventSource.InformixSnapshotContext].isolationLevelBeforeStart
    )
    catch {
      case e: SQLException =>
        throw new RuntimeException("Failed to set transaction isolation level.", e)
    }
  }

  /**
   * Generate a valid Informix query string for the specified table
   *
   * @param tableId the table to generate a query for
   * @return a valid query string
   */
  override protected def getSnapshotSelect(snapshotContext: AbstractSnapshotChangeEventSource.SnapshotContext,
                                           tableId: TableId): Optional[String] =
    Optional.of(String.format("SELECT * FROM %s.%s", tableId.schema, tableId.table))

}