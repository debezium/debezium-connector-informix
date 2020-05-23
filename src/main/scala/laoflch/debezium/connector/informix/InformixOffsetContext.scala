package laoflch.debezium.connector.informix

import java.time.Instant
import java.util
import java.util.{Collections, Map}

import com.informix.jdbc.IfmxReadableType
import com.informix.stream.api.IfmxStreamRecordType
import com.informix.stream.cdc.records.{IfxCDCMetaDataRecord, IfxCDCRecord}
import io.debezium.connector.SnapshotRecord

import io.debezium.pipeline.spi.OffsetContext
import io.debezium.pipeline.txmetadata.TransactionContext
import io.debezium.relational.TableId
import io.debezium.schema.DataCollectionId
import io.debezium.util.Collect
import org.apache.kafka.connect.data.{Schema, Struct}

import scala.jdk.CollectionConverters

class InformixOffsetContext(connectorConfig: InformixConnectorConfig,
                            position: TxLogPosition,
                            snapshot: Boolean,
                            snapshotCompleted: Boolean,
                            //var eventSerialNo: Long,
                            transactionContext: TransactionContext) extends OffsetContext {


  private val sourceInfo: SourceInfo = new SourceInfo(connectorConfig)
  private val sourceInfoSchema: Schema = sourceInfo.schema()


  private val partition: util.Map[String, String] = Collections.singletonMap(InformixOffsetContext.SERVER_PARTITION_KEY, connectorConfig.getLogicalName)
  private var cdcEngine: InformixCDCEngine =null

  setChangePosition(position)


  /*private val snapshotCompleted: Boolean = snapshotCompleted
  private val eventSerialNo = eventSerialNo
  private val transactionContext = transactionContext*/

  //private val transactionContext: TransactionContext = null

  /**
   * The index of the current event within the current transaction.
   */
 // private val eventSerialNo: Long = 0L

/*
  def this(connectorConfig: Db2ConnectorConfig, position: TxLogPosition, snapshot: Boolean, snapshotCompleted: Boolean, eventSerialNo: Long, transactionContext: TransactionContext) {
    this()
    partition = Collections.singletonMap(SERVER_PARTITION_KEY, connectorConfig.getLogicalName)
    sourceInfo = new SourceInfo(connectorConfig)
    sourceInfo.setCommitLsn(position.getCommitLsn)
    sourceInfo.setChangeLsn(position.getInTxLsn)
    sourceInfoSchema = sourceInfo.schema
    this.snapshotCompleted = snapshotCompleted
    if (this.snapshotCompleted) postSnapshotCompletion()
    else sourceInfo.setSnapshot(if (snapshot) SnapshotRecord.TRUE
    else SnapshotRecord.FALSE)
    this.eventSerialNo = eventSerialNo
    this.transactionContext = transactionContext
  }
*/

 def this(connectorConfig: InformixConnectorConfig, position: TxLogPosition, snapshot: Boolean, snapshotCompleted: Boolean) {
     this(connectorConfig, position, snapshot, snapshotCompleted, new TransactionContext)
 }

  def setCDCEngine(cdcEngine: InformixCDCEngine)={this.cdcEngine=cdcEngine}

  def getCDCEngine():InformixCDCEngine=this.cdcEngine


  override def isSnapshotRunning: Boolean = sourceInfo.isSnapshot && !snapshotCompleted

  def isSnapshotCompleted: Boolean = snapshotCompleted

  override def preSnapshotStart(): Unit = {
    sourceInfo.setSnapshot(SnapshotRecord.TRUE)

    if(!this.cdcEngine.hasInit) this.cdcEngine.init()

    this.cdcEngine.stream((record)=>{

      print(record)

      record.getType match {
        case IfmxStreamRecordType.METADATA=>{
          if(record.hasOperationData){
            this.sourceInfo.setColumns(
              CollectionConverters.ListHasAsScala(record.asInstanceOf[IfxCDCMetaDataRecord].getColumns).asScala.toList)
          }

        }
        case _=> {
          if(record.getSequenceId >= 0l && this.sourceInfo.getChangeLsn < 0l){
          //this.position=record.getSequenceId
          this.sourceInfo.setChangeLsn(record.getSequenceId)

          return false
        }
        }
      }
     /* if(record.getSequenceId >= 0l && this.sourceInfo.getChangeLsn < 0l){
        //this.position=record.getSequenceId
        this.sourceInfo.setChangeLsn(record.getSequenceId)

        return false
      }else{
        if(record.getType == IfmxStreamRecordType.METADATA)
      }*/

      true

    })
    //snapshotCompleted = false
  }

  override def preSnapshotCompletion(): Unit = {
    //snapshotCompleted = true
  }

  override def postSnapshotCompletion(): Unit = {
    sourceInfo.setSnapshot(SnapshotRecord.FALSE)
  }

  override def getSourceInfoSchema: Schema = sourceInfoSchema

  override def getSourceInfo: Struct = sourceInfo.struct

  override def getOffset: util.Map[String, _] = {
    if (sourceInfo.isSnapshot)
      Collect.hashMapOf(SourceInfo.SNAPSHOT_KEY, true, InformixOffsetContext.SNAPSHOT_COMPLETED_KEY, snapshotCompleted, SourceInfo.COMMIT_LSN_KEY, sourceInfo.getCommitLsn.toString)
    else transactionContext.store(
      Collect.hashMapOf(SourceInfo.COMMIT_LSN_KEY, sourceInfo.getCommitLsn.toString, SourceInfo.CHANGE_LSN_KEY, sourceInfo.getChangeLsn.toString/*, InformixOffsetContext.EVENT_SERIAL_NO_KEY, eventSerialNo.toString*/))
  }

  override def getPartition: util.Map[String, _] = partition


  override def toString: String = "InformixOffsetContext [" + "sourceInfoSchema=" + sourceInfoSchema + ", sourceInfo=" + sourceInfo + ", partition=" + partition + ", snapshotCompleted=" + snapshotCompleted + ", eventSerialNo=]"

  override def markLastSnapshotRecord(): Unit = {
    sourceInfo.setSnapshot(SnapshotRecord.LAST)
  }

  override def event(tableId: DataCollectionId, timestamp: Instant): Unit = {
    sourceInfo.setSourceTime(timestamp)
    sourceInfo.setTableId(tableId.asInstanceOf[TableId])
  }

  override def getTransactionContext: TransactionContext = null

  def getChangePosition: TxLogPosition = TxLogPosition.valueOf(sourceInfo.getCommitLsn, sourceInfo.getChangeLsn,sourceInfo.getTxId,sourceInfo.getBeginLsn)

  def setChangePosition(position: TxLogPosition): Unit = {
    /*if (getChangePosition == position) eventSerialNo += eventCount
    else eventSerialNo = eventCount*/
    sourceInfo.setCommitLsn(position.getCommitLsn)
    sourceInfo.setChangeLsn(position.getChangeLsn)

    sourceInfo.setTxId(position.getTxId)
    sourceInfo.setBeginLsn(position.getBeginLsn)
  }

}


object InformixOffsetContext {

  private val SERVER_PARTITION_KEY: String = "server"
  private val SNAPSHOT_COMPLETED_KEY: String = "snapshot_completed"
  private val EVENT_SERIAL_NO_KEY: String = "event_serial_no"
  //private val CDC_ENGINE_KEY:String = "cdc_engine"



  class Loader(val connectorConfig: InformixConnectorConfig) extends OffsetContext.Loader {
    override def getPartition: util.Map[String, _] = Collections.singletonMap(SERVER_PARTITION_KEY, connectorConfig.getLogicalName)

    override def load(offset: util.Map[String, _]): OffsetContext = {
      //val changeLsn = Lsn.valueOf(offset.get(SourceInfo.CHANGE_LSN_KEY).asInstanceOf[String])
      //val commitLsn = Lsn.valueOf(offset.get(SourceInfo.COMMIT_LSN_KEY).asInstanceOf[String])offset
      val lsn = offset.get(SourceInfo.CHANGE_LSN_KEY).asInstanceOf[TxLogPosition]
      val snapshot = java.lang.Boolean.TRUE.equals(offset.get(SourceInfo.SNAPSHOT_KEY))
      val snapshotCompleted = java.lang.Boolean.TRUE.equals(offset.get(SNAPSHOT_COMPLETED_KEY))
      // only introduced in 0.10.Beta1, so it might be not present when upgrading from earlier versions
     /* var eventSerialNo = offset.get(EVENT_SERIAL_NO_KEY).asInstanceOf[Long]
      if (eventSerialNo == null) eventSerialNo = java.lang.Long.valueOf(0)
*/
      new InformixOffsetContext(connectorConfig, lsn, snapshot, snapshotCompleted, /*eventSerialNo, */TransactionContext.load(offset))
    }
  }
}

