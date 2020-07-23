package laoflch.debezium.connector.informix.integrtest

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
import com.informix.stream.api.{IfmxStreamRecord, IfmxStreamRecordType}
import com.informix.stream.cdc.records.IfxCDCOperationRecord
import io.debezium.pipeline.spi.ChangeRecordEmitter
import io.debezium.time.Timestamp

import scala.collection.mutable
import scala.jdk.CollectionConverters

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

    val transactionContext = offsetContext.getInformixTransactionContext

    val label2TableId = cdcEngine.converLabel2TableId()

   // val middleValue = Map[Int,Any]()

    while (context.isRunning) {
      cdcEngine.stream((record) => {

        //val tableId = label2Table(record.getLabel.toInt)

        record.getType match {
          case IfmxStreamRecordType.TIMEOUT => {
            handleTimeOutEvent(record)
          }



          /**
           *
           *
           *
           *
           * */


          case IfmxStreamRecordType.BEFORE_UPDATE => {

            println("before_update:",record)
           // if (record.hasOperationData) {
              val data = record.asInstanceOf[IfxCDCOperationRecord].getData
            //}

            offsetContext.setChangePosition(TxLogPosition.cloneAndSet(offsetContext.getChangePosition,
              TxLogPosition.LSN_NULL,//commit
              record.getSequenceId,//change
              record.getTransactionId,//txnid
              TxLogPosition.LSN_NULL)) //begin

            // handleEvent(tableId,offsetContext,InformixChangeRecordEmitter.OP_INSERT,data,null,null,null)
            // dispatcher.dispatchDataChangeEvent(tableId, new InformixChangeRecordEmitter(offsetContext, InformixChangeRecordEmitter.OP_INSERT, data, null, clock))
            //val tableId=label2Table(record.getLabel.toInt)
            //handleEvent(tableId,offsetContext,InformixChangeRecordEmitter.OP_UPDATE_BEFORE,null,data,clock,null)

            transactionContext.beforeUpdate(record.getTransactionId,data)
          }


          /**
           *
           *
           *
           *
           * */

          case IfmxStreamRecordType.AFTER_UPDATE => {
            // if (record.hasOperationData) {a

            println("after_update:",record)
            val newData = record.asInstanceOf[IfxCDCOperationRecord].getData

            val oldData = transactionContext.afterUpdate(record.getTransactionId).get
            //}

            offsetContext.setChangePosition(TxLogPosition.cloneAndSet(offsetContext.getChangePosition,
              TxLogPosition.LSN_NULL,
              record.getSequenceId,
              record.getTransactionId,
              TxLogPosition.LSN_NULL))

            // handleEvent(tableId,offsetContext,InformixChangeRecordEmitter.OP_INSERT,data,null,null,null)
            // dispatcher.dispatchDataChangeEvent(tableId, new InformixChangeRecordEmitter(offsetContext, InformixChangeRecordEmitter.OP_INSERT, data, null, clock))
            val tableId=label2TableId(record.getLabel.toInt)
            handleEvent(tableId,offsetContext,InformixChangeRecordEmitter.OP_UPDATE,oldData,newData ,clock,null)
          }


          /**
           *
           *
           *
           *
           * */

          case IfmxStreamRecordType.BEGIN => {
            println("begin:",record)
            // begin txn
           // val option=transactionContext.beginTxn(record.getTransactionId)
            transactionContext.beginTxn(record.getTransactionId) match {
              case Some(value)=> {
                println(value)
              }
              case None=>{


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
           *
           *
           *
           * */



          case IfmxStreamRecordType.COMMIT => {
            println("commit:", record)

            // commit txn
            transactionContext.commitTxn(record.getTransactionId) match {
              case None=>{


              //val tableId=label2TableId(record.getLabel.toInt)

            }
              case Some(value)=> {
                println(value)

                offsetContext.setChangePosition (TxLogPosition.cloneAndSet (offsetContext.getChangePosition,
                  record.getSequenceId,
                  record.getSequenceId,
                  record.getTransactionId,
                  TxLogPosition.LSN_NULL) )


                handleCommitEvent(offsetContext,value)


              }
            }
            offsetContext.getTransactionContext.endTransaction()
          }

          /**
           *
           *
           *
           *
           * */

          case IfmxStreamRecordType.ROLLBACK => {

            println("rollback:",record)
            // offsetContext.event(tableId, null)
            //val data = record.asInstanceOf[IfxCDCOperationRecord].getData


            transactionContext.rollbackTxn(record.getTransactionId) match {
              case None => {


                //val tableId=label2TableId(record.getLabel.toInt)

              }
              case Some(value) => {

                offsetContext.setChangePosition(TxLogPosition.cloneAndSet(offsetContext.getChangePosition,
                  TxLogPosition.LSN_NULL,
                  record.getSequenceId,
                  record.getTransactionId,
                  TxLogPosition.LSN_NULL))

                // handleEvent(tableId,offsetContext,InformixChangeRecordEmitter.OP_INSERT,data,null,null,null)
                // dispatcher.dispatchDataChangeEvent(tableId, new InformixChangeRecordEmitter(offsetContext, InformixChangeRecordEmitter.OP_INSERT, data, null, clock))
                // val tableId=label2TableId(record.getLabel.toInt)
                //handleEvent(tableId,offsetContext,InformixChangeRecordEmitter.OP_DELETE,data,null,clock,null)
                handleRollbackEvent(offsetContext,value)
              }
            }
            offsetContext.getTransactionContext.endTransaction()

          }

          /**
           *
           *
           *
           *
           * */

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
            val tableId=label2TableId(record.getLabel.toInt)
            handleEvent(tableId,offsetContext,InformixChangeRecordEmitter.OP_INSERT,null,data,clock,null)
          }

          /**
           *
           *
           *
           *
           * */


          case IfmxStreamRecordType.DELETE => {

            println("delete:",record)
            // offsetContext.event(tableId, null)
            val data = record.asInstanceOf[IfxCDCOperationRecord].getData

            offsetContext.setChangePosition(TxLogPosition.cloneAndSet(offsetContext.getChangePosition,
              TxLogPosition.LSN_NULL,
              record.getSequenceId,
              record.getTransactionId,
              TxLogPosition.LSN_NULL))

            // handleEvent(tableId,offsetContext,InformixChangeRecordEmitter.OP_INSERT,data,null,null,null)
            // dispatcher.dispatchDataChangeEvent(tableId, new InformixChangeRecordEmitter(offsetContext, InformixChangeRecordEmitter.OP_INSERT, data, null, clock))
            val tableId=label2TableId(record.getLabel.toInt)
            handleEvent(tableId,offsetContext,InformixChangeRecordEmitter.OP_DELETE,data,null,clock,null)
          }



          case _ =>{}
        }

        def handleEvent(
                        tableId:TableId,
                        offsetContext: InformixOffsetContext,
                        //txn:Long,
                        operation: Int,
                        data:java.util.Map[String, IfmxReadableType],
                        dataNext:java.util.Map[String, IfmxReadableType],

                        clock: Clock,
                        timestamp: Timestamp) {
        //offsetContext.setChangePosition(tableWithSmallestLsn.getChangePosition, eventCount)

          offsetContext.event(tableId, clock.currentTime())

         /* dispatcher.dispatchDataChangeEvent(tableId, new InformixChangeRecordEmitter(offsetContext, operation,
            InformixChangeRecordEmitter.convertIfxData2Array(data),
            InformixChangeRecordEmitter.convertIfxData2Array(dataNext), clock))*/
          val cre=new InformixChangeRecordEmitter(offsetContext, operation,
            InformixChangeRecordEmitter.convertIfxData2Array(data),
            InformixChangeRecordEmitter.convertIfxData2Array(dataNext), clock)

          //add event in transcation
          offsetContext.getInformixTransactionContext.addEvent2Tx(tableId,cre,offsetContext.getChangePosition.getTxId)
      }

        def handleTimeOutEvent(record: IfmxStreamRecord): Unit ={

          offsetContext.setChangePosition(TxLogPosition.cloneAndSet(offsetContext.getChangePosition,
            TxLogPosition.LSN_NULL,
            record.getSequenceId,
            TxLogPosition.LSN_NULL,
            TxLogPosition.LSN_NULL))

        }

        def handleCommitEvent(offsetContext: InformixOffsetContext, cre: mutable.Buffer[(TableId,InformixChangeRecordEmitter)]): Unit ={
          try{

            //offsetContext.getTransactionContext

            //val transactionCRE=offsetContext.getInformixTransactionContext.getTransactionCRE().get(offsetContext.getChangePosition.getTxId).get

            cre.foreach(tuple=> dispatcher.dispatchDataChangeEvent(tuple._1,tuple._2) )


            //dispatcher.dispatchDataChangeEvent(tableId,cre)
          }catch{
            case e: Exception => e.printStackTrace()
            //case _ => println("handleCommitEvent failed!")
          }


        }

        def handleRollbackEvent(offsetContext: InformixOffsetContext, cre: mutable.Buffer[(TableId,InformixChangeRecordEmitter)]): Unit ={
          try{

            //offsetContext.getTransactionContext

            //val transactionCRE=offsetContext.getInformixTransactionContext.getTransactionCRE().get(offsetContext.getChangePosition.getTxId).get

            //cre.foreach(tuple=> dispatcher.dispatchDataChangeEvent(tuple._1,tuple._2) )


            //dispatcher.dispatchDataChangeEvent(tableId,cre)
          }catch{
            case e: Exception => e.printStackTrace()
            //case _ => println("handleCommitEvent failed!")
          }


        }




        println(offsetContext.getChangePosition)

        false
      })
    }

  }
  //override def toString: String = "ChangeTablePointer [changeTable=" + changeTable + ", resultSet=" + resultSet + ", completed=" + completed + ", currentChangePosition=" + currentChangePosition + "]"


}

