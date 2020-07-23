package laoflch.debezium.connector.informix.integrtest

import io.debezium.relational.TableId


/**
 * A logical representation of change table containing changes for a given source table.
 * There is usually one change table for each source table. When the schema of the source table
 * is changed then two change tables could be present.
 *
 * @author laoflch Luo
 *
 */
object ChangeTable {
  private val CDC_SCHEMA = "syscdcsv1"
}

class ChangeTable(/**
                   * The table from which the changes are captured
                   */
                  val sourceTableId: TableId,

                  /**
                   * The logical name of the change capture process
                   */
                  val captureInstance: String,

                  /**
                   * Numeric identifier of change table in DB2 schema
                   */
                  val changeTableObjectId: Int,

                  /**
                   * A LSN from which the data in the change table are relevant
                   */
                  val startLsn: Lsn,

                  /**
                   * A LSN to which the data in the change table are relevant
                   */
                  var stopLsn: Lsn) // this.changeTableId = sourceTableId != null ? new TableId(sourceTableId.catalog(), CDC_SCHEMA, captureInstance + "_CT") : null;
{
  val changeTableId = if (sourceTableId != null) new TableId(sourceTableId.catalog, ChangeTable.CDC_SCHEMA, captureInstance)
  else null
  /**
   * The table that contains the changes for the source table
   */
  //final private var changeTableId: TableId = null

  def this(captureInstance: String, changeTableObjectId: Int, startLsn: Lsn, stopLsn: Lsn) {
    this(null, captureInstance, changeTableObjectId, startLsn, stopLsn)
  }

  def getCaptureInstance: String = captureInstance

  def getStartLsn: Lsn = startLsn

  def getStopLsn: Lsn = stopLsn

  def setStopLsn(stopLsn: Lsn): Unit = {
    this.stopLsn = stopLsn
  }

  def getSourceTableId: TableId = sourceTableId

  def getChangeTableId: TableId = changeTableId

  def getChangeTableObjectId: Int = changeTableObjectId

  override def toString: String = "Capture instance \"" + captureInstance + "\" [sourceTableId=" + sourceTableId + ", changeTableId=" + changeTableId + ", startLsn=" + startLsn + ", changeTableObjectId=" + changeTableObjectId + ", stopLsn=" + stopLsn + "]"
}

