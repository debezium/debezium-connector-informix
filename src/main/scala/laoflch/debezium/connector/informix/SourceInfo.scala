package laoflch.debezium.connector.informix

import java.time.Instant


import com.informix.jdbc.IfxColumnInfo
import io.debezium.connector.AbstractSourceInfo
import io.debezium.connector.common.BaseSourceInfo
import io.debezium.relational.TableId

object SourceInfo  {
  val CHANGE_LSN_KEY = "change_lsn"
  val COMMIT_LSN_KEY = "commit_lsn"
  val BEGIN_LSN_KEY = "begin_ls"
  val TX_ID = "tx_id"
  val DEBEZIUM_VERSION_KEY = AbstractSourceInfo.DEBEZIUM_VERSION_KEY
  val DEBEZIUM_CONNECTOR_KEY = AbstractSourceInfo.DEBEZIUM_CONNECTOR_KEY
  val SERVER_NAME_KEY = AbstractSourceInfo.SERVER_NAME_KEY
  val TIMESTAMP_KEY = AbstractSourceInfo.TIMESTAMP_KEY
  val SNAPSHOT_KEY = AbstractSourceInfo.SNAPSHOT_KEY
  val DATABASE_NAME_KEY = AbstractSourceInfo.DATABASE_NAME_KEY
  val SCHEMA_NAME_KEY = AbstractSourceInfo.SCHEMA_NAME_KEY
  val TABLE_NAME_KEY = AbstractSourceInfo.TABLE_NAME_KEY
  val COLLECTION_NAME_KEY = AbstractSourceInfo.COLLECTION_NAME_KEY

}

class SourceInfo(connectorConfig:InformixConnectorConfig) extends BaseSourceInfo(connectorConfig) {


  private var changeLsn = -1l
  private var commitLsn = -1l
  private var beginLsn = -1l
  private var txId = -1l
  private var sourceTime: Instant = null
  private var tableId: TableId = null

  private var databaseName = connectorConfig.getDatabaseName

  private var streamMetadata: List[IfxColumnInfo] = null

  /**
   * @param changeLsn - LSN of the change in the database log
   */
  def setChangeLsn(changeLsn: Long): Unit = this.changeLsn=changeLsn

  def getChangeLsn: Long = changeLsn

  /**
   * @param beginLsn - LSN of the { @code COMMIT} of the transaction whose part the change is
   */
  def setBeginLsn(beginLsn: Long): Unit = this.beginLsn=beginLsn

  def getBeginLsn: Long = beginLsn

  /**
   * @param txId - LSN of the { @code COMMIT} of the transaction whose part the change is
   */

  def setTxId(txId: Long): Unit = this.txId=txId

  def getTxId: Long = this.txId



  /**
   * @param commitLsn - LSN of the { @code COMMIT} of the transaction whose part the change is
   */
  def setCommitLsn(commitLsn: Long): Unit = this.commitLsn=commitLsn
  def getCommitLsn: Long = commitLsn
  /**
   * @param instant a time at which the transaction commit was executed
   */
  def setSourceTime(instant: Instant): Unit = this.sourceTime = instant

  def getTableId: TableId = tableId

  /**
   * @param tableId - source table of the event
   */
  def setTableId(tableId: TableId): Unit = this.tableId = tableId

  def setColumns(cols: List[IfxColumnInfo]): Unit = {this.streamMetadata = cols}

  override def toString: String = "SourceInfo [" + "serverName=" + serverName + ", changeLsn=" + changeLsn + ", commitLsn=" + commitLsn + ", snapshot=" + snapshotRecord + ", sourceTime=" + sourceTime + "]"

  override protected def timestamp: Instant = sourceTime

  override protected def database: String = databaseName
}
