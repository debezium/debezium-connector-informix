/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package laoflch.debezium.connector.informix

import java.sql.SQLException
import java.util.{Collections, List, Map}
import java.util

import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.source.SourceRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import io.debezium.config.Configuration
import io.debezium.config.Field
import io.debezium.connector.base.ChangeEventQueue
import io.debezium.connector.common.BaseSourceTask
import io.debezium.pipeline.ChangeEventSourceCoordinator
import io.debezium.pipeline.DataChangeEvent
import io.debezium.pipeline.ErrorHandler
import io.debezium.pipeline.EventDispatcher
import io.debezium.pipeline.spi.{ChangeEventCreator, OffsetContext}
import io.debezium.relational.{HistorizedRelationalDatabaseConnectorConfig, TableId}
import io.debezium.relational.history.DatabaseHistory
import io.debezium.util.Clock
import io.debezium.util.SchemaNameAdjuster
import laoflch.debezium.connector.informix

import scala.jdk.CollectionConverters
import scala.collection.mutable


/**
 * The main task executing streaming from Informix.
 * Responsible for lifecycle management the streaming code.
 *
 * @author  laoflch Luo
 *
 */
object InformixConnectorTask {
  private val LOGGER = LoggerFactory.getLogger(classOf[InformixConnectorTask])
  private val CONTEXT_NAME = "informix-server-connector-task"
  class changeEventCreator() extends ChangeEventCreator{

    override def createDataChangeEvent(x:SourceRecord): DataChangeEvent = {
      new DataChangeEvent(x)
    }
  }
}

class InformixConnectorTask extends BaseSourceTask {


  override def version: String = Module.version

  private var queue: ChangeEventQueue[DataChangeEvent] = null
  private var dataConnection: InformixConnection = null
  private var metadataConnection: InformixConnection = null
  private var schema: InformixDatabaseSchema = null


  override def start( config: Configuration): ChangeEventSourceCoordinator = {


    val connectorConfig = new InformixConnectorConfig(config)
    val topicSelector = InformixTopicSelector.defaultSelector(connectorConfig)
    val schemaNameAdjuster = SchemaNameAdjuster.create(InformixConnectorTask.LOGGER)
    // By default do not load whole result sets into memory

    val jdbcConfig = config.edit
                              .withDefault("database.responseBuffering", "adaptive")
                              .withDefault("database.fetchSize", 10000)
                              .build
                              .filter((x: String) => !((x.startsWith(DatabaseHistory.CONFIGURATION_FIELD_PREFIX_STRING) || x == HistorizedRelationalDatabaseConnectorConfig.DATABASE_HISTORY.name)))
                              .subset("database.", true);
    val dataConnection = new InformixConnection(jdbcConfig)

    val metadataConnection = new InformixConnection(jdbcConfig)
    try{
      dataConnection.setAutoCommit(false)

    }catch{
      case e: SQLException =>
        e.printStackTrace()
        throw new ConnectException(e)
    }
    val schema = new InformixDatabaseSchema(connectorConfig, schemaNameAdjuster, topicSelector, dataConnection)

    schema.initializeStorage()

    val previousOffset = getPreviousOffset(new (InformixOffsetContext.Loader)(connectorConfig))
    if (previousOffset != null) schema.recover(previousOffset)



    val taskContext = new InformixTaskContext(connectorConfig, schema)
    val clock = Clock.system
    // Set up the task record queue ...
    this.queue =
    new ChangeEventQueue
    .Builder[DataChangeEvent]()
      .pollInterval(connectorConfig.getPollInterval)
      .maxBatchSize(connectorConfig.getMaxBatchSize)
      .maxQueueSize(connectorConfig.getMaxQueueSize)
      .loggingContextSupplier(() => taskContext.configureLoggingContext(InformixConnectorTask.CONTEXT_NAME))
      .build

    //createChangeEventQueueBuilder(connectorConfig,taskContext,InformixConnectorTask.CONTEXT_NAME)

    val errorHandler = new ErrorHandler(classOf[InformixConnector], connectorConfig.getLogicalName, this.queue)

    val metadataProvider = new InformixEventMetadataProvider

    val dispatcher = new EventDispatcher[TableId](connectorConfig,
      topicSelector,
      schema,
      this.queue,
      connectorConfig.getTableFilters.dataCollectionFilter,
      new InformixConnectorTask.changeEventCreator(),
      metadataProvider)

    val coordinator = new ChangeEventSourceCoordinator(previousOffset,
      errorHandler,
      classOf[InformixConnector],
      connectorConfig,
      new InformixChangeEventSourceFactory(connectorConfig, dataConnection, metadataConnection, errorHandler, dispatcher, clock, schema),
      dispatcher,
      schema)

    coordinator.start(taskContext, this.queue, metadataProvider)
    coordinator
  }

  /**
   * Loads the connector's persistent offset (if present) via the given loader.
   */
  override protected def getPreviousOffset(loader: OffsetContext.Loader): OffsetContext = {
    val partition = loader.getPartition
    val previousOffset = context.offsetStorageReader.offsets(Collections.singleton(partition)).get(partition)

    //previousOffset.put("",this.dataConnection.getCDCEngine())
    if (previousOffset != null) {
      val offsetContext = loader.load(previousOffset)
      InformixConnectorTask.LOGGER.info("Found previous offset {}", offsetContext)
      offsetContext
    }
    else null
  }

  @throws[InterruptedException]
  override def doPoll: util.List[SourceRecord] = {
    //import scala.collection.JavaConversions._

    CollectionConverters.BufferHasAsJava(CollectionConverters.ListHasAsScala(this.queue.poll).asScala.map[SourceRecord]((x:DataChangeEvent)=>x.getRecord)).asJava

  }

  override def doStop(): Unit = {
    try if (dataConnection != null) dataConnection.close()
    catch {
      case e: SQLException =>
        InformixConnectorTask.LOGGER.error("Exception while closing JDBC connection", e)
    }
    try if (metadataConnection != null) metadataConnection.close()
    catch {
      case e: SQLException =>
        InformixConnectorTask.LOGGER.error("Exception while closing JDBC metadata connection", e)
    }
    if (schema != null) schema.close()
  }

  override protected def getAllConfigurationFields: java.lang.Iterable[Field] = {

    InformixConnectorConfig.ALL_FIELDS.asInstanceOf[java.lang.Iterable[Field]]
  }
}
