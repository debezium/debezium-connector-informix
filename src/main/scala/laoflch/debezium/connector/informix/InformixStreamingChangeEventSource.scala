package laoflch.debezium.connector.informix

import com.informix.jdbc.IfmxReadableType
import io.debezium.data.Envelope.Operation
import io.debezium.pipeline.{ErrorHandler, EventDispatcher}
import io.debezium.pipeline.source.spi.{ChangeEventSource, StreamingChangeEventSource}
import io.debezium.relational.TableId
import io.debezium.util.Clock
import org.slf4j.{Logger, LoggerFactory}
import com.informix.stream.api.{IfmxStreamRecord, IfmxStreamRecordType}
import com.informix.stream.cdc.records.IfxCDCOperationRecord
import io.debezium.time.Timestamp

import scala.collection.mutable

object InformixStreamingChangeEventSource {


 /* var BEGIN =

  type COMMIT=COMMIT
  type DELETE=DELETE
  type DISCARD=DISCARD
  type ERROR=ERROR
  type INSERT=INSERT
  type METADATA=METADATA
  type ROLLBACK=ROLLBACK
  type TIMEOUT=TIMEOUT
  type TRUNCATE=TRUNCATE
  type BEFORE_UPDATE=BEFORE_UPDATE
  type AFTER_UPDATE=AFTER_UPDATE*/

}

class InformixStreamingChangeEventSource(connectorConfig: InformixConnectorConfig,
                                         offsetContext: InformixOffsetContext,
                                         dataConnection: InformixConnection,
                                         //metadataConnection: InformixConnection,
                                         dispatcher: EventDispatcher[TableId],
                                         errorHandler: ErrorHandler,
                                         clock: Clock,
                                         schema: InformixDatabaseSchema) extends StreamingChangeEventSource {


  private val LOGGER: Logger = LoggerFactory.getLogger(classOf[InformixStreamingChangeEventSource])


  /**
   * Connection used for reading CDC tables.
   */
  //private val dataConnection: InformixConnection = dataConnection

  /**
   * A separate connection for retrieving timestamps; without it, adaptive
   * buffering will not work.
   */
  //private val metadataConnection: InformixConnection = metadataConnection

  //private val dispatcher: EventDispatcher[TableId] = dispatcher
  //private val errorHandler: ErrorHandler = errorHandler
  //private val clock: Clock = clock
  // private val schema: InformixDatabaseSchema = schema
  // private val offsetContext: InformixOffsetContext = offsetContext
  // private val pollInterval: Duration = connectorConfig.getPollInterval
  // private val connectorConfig: InformixConnectorConfig = connectorConfig


  @throws[InterruptedException]
  override def execute(context: ChangeEventSource.ChangeEventSourceContext): Unit = {
    val cdcEngine = dataConnection.getCDCEngine()
    val transactionContext = offsetContext.getInformixTransactionContext
    val schema = this.schema

    /*
    LOGGER.info("Schema          = {}", schema)
    LOGGER.info("Schema.tableIds = {}", schema.tableIds)
    CollectionConverters.SetHasAsScala(schema.tableIds()).asScala.foreach(x => {
      LOGGER.info("Schema Table: catalog={} schema={} table={}", x.catalog(), x.schema(), x.table())
      CollectionConverters.ListHasAsScala(schema.tableFor(x).columns()).asScala.foreach(c => {
        LOGGER.info("Schema Table Columns: name={} jdbcType={} nativeType={}, typeName={}",
          c.name(), c.jdbcType(), c.nativeType(), c.typeName())
      })
    })
    */

    /**
     * Initialize CDC Engine.
     */
    val lastPosition = offsetContext.getChangePosition
    val fromLsn: Long = lastPosition.getCommitLsn
    cdcEngine.setStartLsn(fromLsn)
    cdcEngine.init(schema)
    val label2TableId = cdcEngine.convertLabel2TableId()

    /**
     * Main Handle Loop
     */
    while (context.isRunning) {
      cdcEngine.stream((record) => {

        record.getType match {
          case IfmxStreamRecordType.TIMEOUT => {
            handleTimeOutEvent(record)
          }

          /**
           *
           * Handle BEFORE_UPDATE
           *
           * */
          case IfmxStreamRecordType.BEFORE_UPDATE => {

            val data = record.asInstanceOf[IfxCDCOperationRecord].getData

            offsetContext.setChangePosition(TxLogPosition.cloneAndSet(offsetContext.getChangePosition,
              TxLogPosition.LSN_NULL,   // commit
              record.getSequenceId,     // change
              record.getTransactionId,  // txnid
              TxLogPosition.LSN_NULL))  // begin

            transactionContext.beforeUpdate(record.getTransactionId, data)
          }

          /**
           *
           * Handle AFTER_UPDATE
           *
           * */
          case IfmxStreamRecordType.AFTER_UPDATE => {

            val newData = record.asInstanceOf[IfxCDCOperationRecord].getData
            val oldData = transactionContext.afterUpdate(record.getTransactionId).get

            offsetContext.setChangePosition(TxLogPosition.cloneAndSet(offsetContext.getChangePosition,
              TxLogPosition.LSN_NULL,
              record.getSequenceId,
              record.getTransactionId,
              TxLogPosition.LSN_NULL))

            val tableId = label2TableId(record.getLabel.toInt)

            handleEvent(tableId, offsetContext, InformixChangeRecordEmitter.OP_UPDATE, oldData, newData, clock, null)
          }

          /**
           *
           * Handle BEGIN
           *
           * */
          case IfmxStreamRecordType.BEGIN => {
            LOGGER.info("Received BEGIN Record")

            transactionContext.beginTxn(record.getTransactionId) match {
              case Some(value) => {
                println(value)
              }
              case None => {
                offsetContext.setChangePosition(TxLogPosition.cloneAndSet(offsetContext.getChangePosition,
                  TxLogPosition.LSN_NULL,
                  record.getSequenceId,
                  record.getTransactionId,
                  record.getSequenceId))
                offsetContext.getTransactionContext.beginTransaction(record.getTransactionId.toString)
              }
            }
          }

          /**
           *
           * Handle COMMIT
           *
           * */
          case IfmxStreamRecordType.COMMIT => {
            LOGGER.info("Received COMMIT Record")
            transactionContext.commitTxn(record.getTransactionId) match {
              case Some(value) => {
                //println(value)

                offsetContext.setChangePosition(TxLogPosition.cloneAndSet(offsetContext.getChangePosition,
                  record.getSequenceId,
                  record.getSequenceId,
                  record.getTransactionId,
                  TxLogPosition.LSN_NULL))

                handleCommitEvent(offsetContext, value)
              }
              case None => {
                //val tableId=label2TableId(record.getLabel.toInt)
              }
            }
            offsetContext.getTransactionContext.endTransaction()
          }

          /**
           *
           * Handle ROLLBACK
           *
           * */
          case IfmxStreamRecordType.ROLLBACK => {
            LOGGER.info("Received ROLLBACK Record")

            transactionContext.rollbackTxn(record.getTransactionId) match {
              case Some(value) => {

                offsetContext.setChangePosition(TxLogPosition.cloneAndSet(offsetContext.getChangePosition,
                  TxLogPosition.LSN_NULL,
                  record.getSequenceId,
                  record.getTransactionId,
                  TxLogPosition.LSN_NULL))

                /**
                 * when rollbackTxn do noting handle but log the discarded records
                 */
                LOGGER.info("Rollback Txn:" + record.getTransactionId)
                handleRollbackEvent(offsetContext, value)
              }
              case None => {
                //val tableId=label2TableId(record.getLabel.toInt)
              }
            }
            offsetContext.getTransactionContext.endTransaction()
          }

          /**
           *
           * Handle INSERT
           *
           * */
          case IfmxStreamRecordType.INSERT => {
            if (LOGGER.isTraceEnabled()) {
              LOGGER.trace("Received INSERT Record")
            }

            val data = record.asInstanceOf[IfxCDCOperationRecord].getData

            offsetContext.setChangePosition(TxLogPosition.cloneAndSet(offsetContext.getChangePosition,
              TxLogPosition.LSN_NULL,
              record.getSequenceId,
              record.getTransactionId,
              TxLogPosition.LSN_NULL))

            val tableId = label2TableId(record.getLabel.toInt)
            if (LOGGER.isTraceEnabled()) {
              LOGGER.trace("Lookup label to tableId : {} -> {}", record.getLabel.toInt, tableId)
            }
            handleEvent(tableId, offsetContext, InformixChangeRecordEmitter.OP_INSERT, null, data, clock, null)
          }

          /**
           *
           * Handle DELETE
           *
           * */
          case IfmxStreamRecordType.DELETE => {
            if (LOGGER.isTraceEnabled()) {
              LOGGER.trace("Received DELETE Record")
            }

            val data = record.asInstanceOf[IfxCDCOperationRecord].getData

            offsetContext.setChangePosition(TxLogPosition.cloneAndSet(offsetContext.getChangePosition,
              TxLogPosition.LSN_NULL,
              record.getSequenceId,
              record.getTransactionId,
              TxLogPosition.LSN_NULL))

            val tableId = label2TableId(record.getLabel.toInt)
            handleEvent(tableId, offsetContext, InformixChangeRecordEmitter.OP_DELETE, data, null, clock, null)
          }

          case _ => {}
        }

        def handleEvent(tableId: TableId,
                        offsetContext: InformixOffsetContext,
                        //txn:Long,
                        operation: Int,
                        data: java.util.Map[String, IfmxReadableType],
                        dataNext: java.util.Map[String, IfmxReadableType],
                        clock: Clock,
                        timestamp: Timestamp) {

          offsetContext.event(tableId, clock.currentTime())

          val cre = new InformixChangeRecordEmitter(offsetContext, operation,
            InformixChangeRecordEmitter.convertIfxData2Array(data),
            InformixChangeRecordEmitter.convertIfxData2Array(dataNext), clock)

          //add event in transcation
          offsetContext.getInformixTransactionContext.addEvent2Tx(tableId, cre, offsetContext.getChangePosition.getTxId)
        }

        def handleTimeOutEvent(record: IfmxStreamRecord): Unit = {
          offsetContext.setChangePosition(TxLogPosition.cloneAndSet(offsetContext.getChangePosition,
            TxLogPosition.LSN_NULL,
            record.getSequenceId,
            TxLogPosition.LSN_NULL,
            TxLogPosition.LSN_NULL))
        }

        def handleCommitEvent(offsetContext: InformixOffsetContext, cre: mutable.Buffer[(TableId, InformixChangeRecordEmitter)]): Unit = {
          try {
            cre.foreach(tuple => {
              LOGGER.info("handleCommit:: {}", tuple)
              dispatcher.dispatchDataChangeEvent(tuple._1, tuple._2)
            })

          } catch {
            case e: Exception => LOGGER.info("HandleCommit got exception: {}", e.toString);
            //case _ => println("handleCommitEvent failed!")
          }
        }

        def handleRollbackEvent(offsetContext: InformixOffsetContext, cre: mutable.Buffer[(TableId, InformixChangeRecordEmitter)]): Unit = {
          try {
            cre.foreach(tuple => LOGGER.info("id:" + tuple._1 + ":" + "ChangeRecord:" + tuple._2.toString))
            //dispatcher.dispatchDataChangeEvent(tableId,cre)
          } catch {
            case e: Exception => e.printStackTrace()
            //case _ => println("handleCommitEvent failed!")
          }
        }

        false
      })
    }
  }

  //override def toString: String = "ChangeTablePointer [changeTable=" + changeTable + ", resultSet=" + resultSet + ", completed=" + completed + ", currentChangePosition=" + currentChangePosition + "]"
}

