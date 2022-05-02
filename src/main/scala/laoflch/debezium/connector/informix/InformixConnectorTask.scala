/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package laoflch.debezium.connector.informix

import io.debezium.config.{Configuration, Field}
import io.debezium.connector.base.ChangeEventQueue
import io.debezium.connector.common.BaseSourceTask
import io.debezium.pipeline.{ChangeEventSourceCoordinator, DataChangeEvent, ErrorHandler, EventDispatcher}
import io.debezium.pipeline.spi.{ChangeEventCreator, OffsetContext}
import io.debezium.relational.history.DatabaseHistory
import io.debezium.relational.{HistorizedRelationalDatabaseConnectorConfig, TableId}
import io.debezium.util.{Clock, SchemaNameAdjuster}
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.source.SourceRecord
import org.slf4j.{Logger, LoggerFactory}

import java.sql.SQLException
import java.util
import java.util.Collections
import scala.jdk.CollectionConverters


/**
 * The main task executing streaming from Informix.
 * Responsible for lifecycle management the streaming code.
 *
 * @author laoflch Luo
 *
 */
object InformixConnectorTask {
  private val LOGGER = LoggerFactory.getLogger(classOf[InformixConnectorTask])
  private val CONTEXT_NAME = "informix-server-connector-task"

  class changeEventCreator() extends ChangeEventCreator {

    override def createDataChangeEvent(x: SourceRecord): DataChangeEvent = {
      new DataChangeEvent(x)
    }
  }
}

class InformixConnectorTask extends BaseSourceTask {

  private val LOGGER: Logger = LoggerFactory.getLogger(classOf[InformixConnectorTask])
  private var queue: ChangeEventQueue[DataChangeEvent] = null
  private var dataConnection: InformixConnection = null
  private var metadataConnection: InformixConnection = null
  private var schema: InformixDatabaseSchema = null

  override def version: String = Module.version

  override def start(config: Configuration): ChangeEventSourceCoordinator = {

    val connectorConfig = new InformixConnectorConfig(config)
    val topicSelector = InformixTopicSelector.defaultSelector(connectorConfig)
    val schemaNameAdjuster = SchemaNameAdjuster.create(InformixConnectorTask.LOGGER)

    val jdbcConfig = config.edit
      .withDefault("database.responseBuffering", "adaptive")
      .withDefault("database.fetchSize", 10000)
      .withDefault("informix.debug.file", "/tmp/debezium_informix_trace.out")
      .build
      .filter((x: String) => !(
        (x.startsWith(DatabaseHistory.CONFIGURATION_FIELD_PREFIX_STRING) ||
          x == HistorizedRelationalDatabaseConnectorConfig.DATABASE_HISTORY.name)))
      .subset("database.", true);

    this.dataConnection = new InformixConnection(jdbcConfig)
    this.metadataConnection = new InformixConnection(jdbcConfig)

    try {
      this.dataConnection.setAutoCommit(false)
    } catch {
      case e: SQLException =>
        e.printStackTrace()
        throw new ConnectException(e)
    }

    this.schema = new InformixDatabaseSchema(connectorConfig, schemaNameAdjuster, topicSelector, this.dataConnection)
    this.schema.initializeStorage()

    val previousOffset = getPreviousOffset(new (InformixOffsetContext.Loader)(connectorConfig))
    if (previousOffset != null)
      this.schema.recover(previousOffset)

    val taskContext = new InformixTaskContext(connectorConfig, this.schema)
    val clock = Clock.system
    // Set up the task record queue ...
    this.queue = new ChangeEventQueue.Builder[DataChangeEvent]()
      .pollInterval(connectorConfig.getPollInterval) // DEFAULT_POLL_INTERVAL_MILLIS = 500;
      .maxBatchSize(connectorConfig.getMaxBatchSize) // DEFAULT_MAX_BATCH_SIZE = 2048;
      .maxQueueSize(connectorConfig.getMaxQueueSize) // DEFAULT_MAX_QUEUE_SIZE = 8192;
      .loggingContextSupplier(() => taskContext.configureLoggingContext(InformixConnectorTask.CONTEXT_NAME))
      .build

    //createChangeEventQueueBuilder(connectorConfig,taskContext,InformixConnectorTask.CONTEXT_NAME)

    val errorHandler = new ErrorHandler(classOf[InformixConnector], connectorConfig.getLogicalName, this.queue)
    val metadataProvider = new InformixEventMetadataProvider

    val dispatcher = new EventDispatcher[TableId](connectorConfig,
      topicSelector,
      this.schema,
      this.queue,
      connectorConfig.getTableFilters.dataCollectionFilter,
      new InformixConnectorTask.changeEventCreator(),
      metadataProvider)

    val coordinator = new ChangeEventSourceCoordinator(previousOffset,
      errorHandler,
      classOf[InformixConnector],
      connectorConfig,
      new InformixChangeEventSourceFactory(connectorConfig, this.dataConnection, this.metadataConnection,
        errorHandler, dispatcher, clock, this.schema),
      dispatcher,
      this.schema)

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
    CollectionConverters.BufferHasAsJava(CollectionConverters.ListHasAsScala(this.queue.poll).asScala.map[SourceRecord]((x: DataChangeEvent) => x.getRecord)).asJava
  }

  override def doStop(): Unit = {
    try if (dataConnection != null) {
      dataConnection.close()
    }
    catch {
      case e: SQLException =>
        InformixConnectorTask.LOGGER.error("Exception while closing JDBC connection", e)
    }
    try if (metadataConnection != null) {
      metadataConnection.close()
    }
    catch {
      case e: SQLException =>
        InformixConnectorTask.LOGGER.error("Exception while closing JDBC metadata connection", e)
    }
    if (schema != null) {
      schema.close()
    }
  }

  override protected def getAllConfigurationFields: java.lang.Iterable[Field] = {
    InformixConnectorConfig.ALL_FIELDS.asInstanceOf[java.lang.Iterable[Field]]
  }
}
