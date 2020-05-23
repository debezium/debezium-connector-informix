package laoflch.debezium.connector.informix


import java.sql.ResultSet
import java.time.Duration
import java.util.regex.Pattern

import com.informix.jdbc.IfmxReadableType
import io.debezium.data.Envelope.Operation
import io.debezium.pipeline.{ErrorHandler, EventDispatcher}
import io.debezium.pipeline.source.spi.{ChangeEventSource, StreamingChangeEventSource}
import io.debezium.relational.TableId
import io.debezium.util.Clock
import org.slf4j.{Logger, LoggerFactory}
import laoflch.debezium.connector.informix.InformixCDCEngine
import com.informix.stream.api.{IfmxStreamRecord, IfmxStreamRecordType}
import com.informix.stream.cdc.records.IfxCDCOperationRecord
import io.debezium.time.Timestamp

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

  private val COL_COMMIT_LSN: Int = 2
  private val COL_ROW_LSN: Int = 3
  private val COL_OPERATION: Int = 1
  private val COL_DATA: Int = 5

  private val MISSING_CDC_FUNCTION_CHANGES_ERROR: Pattern = Pattern.compile ("Invalid object name 'cdc.fn_cdc_get_all_changes_(.*)'\\.")

  private val LOGGER: Logger = LoggerFactory.getLogger (classOf[InformixStreamingChangeEventSource] )

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
  private val pollInterval: Duration = connectorConfig.getPollInterval
 //private val connectorConfig: InformixConnectorConfig = connectorConfig



  @throws[InterruptedException]
  override def execute (context: ChangeEventSource.ChangeEventSourceContext): Unit = {
    val cdcEngine = offsetContext.getCDCEngine()

    val label2Table = cdcEngine.converLabel2Table()

   // val middleValue = Map[Int,Any]()

    while (context.isRunning) {
      cdcEngine.stream((record) => {

        //val tableId = label2Table(record.getLabel.toInt)

        record.getType match {
          case IfmxStreamRecordType.TIMEOUT => {
            handleTimeOutEvent(record)
          }
          case IfmxStreamRecordType.BEFORE_UPDATE => {
            if (record.hasOperationData) {
              val data = record.asInstanceOf[IfxCDCOperationRecord].getData
            }
          }

          case IfmxStreamRecordType.BEGIN => {
            println("begin:",record)
            offsetContext.setChangePosition(TxLogPosition.cloneAndSet(offsetContext.getChangePosition,
              TxLogPosition.LSN_NULL,
              record.getSequenceId,
              record.getTransactionId,
              record.getSequenceId))
          }
          case IfmxStreamRecordType.COMMIT => {
            println("commit:",record)

            offsetContext.setChangePosition(TxLogPosition.cloneAndSet(offsetContext.getChangePosition,
              record.getSequenceId,
              record.getSequenceId,
              record.getTransactionId,
              TxLogPosition.LSN_NULL))
          }
          case IfmxStreamRecordType.INSERT => {

            println("insert:",record)
           // offsetContext.event(tableId, null)
            val data = record.asInstanceOf[IfxCDCOperationRecord].getData

            offsetContext.setChangePosition(TxLogPosition.cloneAndSet(offsetContext.getChangePosition,
              TxLogPosition.LSN_NULL,
              record.getSequenceId,
              record.getTransactionId,
              TxLogPosition.LSN_NULL))

           // handleEvent(tableId,offsetContext,InformixChangeRecordEmitter.OP_INSERT,data,null,null,null)
           // dispatcher.dispatchDataChangeEvent(tableId, new InformixChangeRecordEmitter(offsetContext, InformixChangeRecordEmitter.OP_INSERT, data, null, clock))
            val tableId=label2Table(record.getLabel.toInt)
            handleEvent(tableId,offsetContext,InformixChangeRecordEmitter.OP_INSERT,null,data,clock,null)
          }
          case _ =>{}
        }

        def handleEvent(
                        tableId:TableId,
                        offsetContext: InformixOffsetContext,
                        operation: Int,
                        data:java.util.Map[String, IfmxReadableType],
                        dataNext:java.util.Map[String, IfmxReadableType],

                        clock: Clock,
                        timestamp: Timestamp) {
        //offsetContext.setChangePosition(tableWithSmallestLsn.getChangePosition, eventCount)

          offsetContext.event(tableId, clock.currentTime())

          dispatcher.dispatchDataChangeEvent(tableId, new InformixChangeRecordEmitter(offsetContext, operation,
            InformixChangeRecordEmitter.convertIfxData2Array(data),
            InformixChangeRecordEmitter.convertIfxData2Array(dataNext), clock))
      }

        def handleTimeOutEvent(record: IfmxStreamRecord): Unit ={

          offsetContext.setChangePosition(TxLogPosition.cloneAndSet(offsetContext.getChangePosition,
            TxLogPosition.LSN_NULL,
            record.getSequenceId,
            TxLogPosition.LSN_NULL,
            TxLogPosition.LSN_NULL))

        }


        def handleEventInTx(): Unit ={

        }

        println(offsetContext.getChangePosition)

        false
      })
    }

  }
  //override def toString: String = "ChangeTablePointer [changeTable=" + changeTable + ", resultSet=" + resultSet + ", completed=" + completed + ", currentChangePosition=" + currentChangePosition + "]"


}

