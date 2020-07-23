package laoflch.debezium.connector.informix

object TxLogPosition {
  val NULL = new TxLogPosition(-1l, -1l,-1l,-1l)
  val LSN_NULL:Long = -1l

  def valueOf(commitLsn: Long, changeLsn: Long): TxLogPosition = if (commitLsn == null && changeLsn == null) NULL
  else new TxLogPosition(commitLsn,changeLsn,0x00l,0x00l)

  def valueOf(commitLsn: Long): TxLogPosition = valueOf(commitLsn, 0x00l)

  def valueOf(commitLsn: Long, changeLsn: Long,txId: Long,beginLsn: Long): TxLogPosition = new TxLogPosition(commitLsn, changeLsn,txId,beginLsn)

  def valueOf(commitLsn: Long, changeLsn: Long,beginLsn: Long):TxLogPosition = valueOf(commitLsn, changeLsn,0x00l,beginLsn)

  def cloneAndSet(position: TxLogPosition,commitLsn: Long, changeLsn: Long,txId: Long,beginLsn: Long): TxLogPosition = {
    valueOf(
      if(commitLsn>LSN_NULL)commitLsn else position.getCommitLsn,
      if(changeLsn>LSN_NULL)changeLsn else position.getChangeLsn,
      if(txId>LSN_NULL)txId else position.getTxId,
      if(beginLsn>LSN_NULL)beginLsn else position.getBeginLsn)
  }
}

class TxLogPosition private(val commitLsn: Long, val changeLsn: Long,val txId: Long,val beginLsn: Long) extends Nullable with Comparable[TxLogPosition] {
  def getCommitLsn: Long = commitLsn

  def getChangeLsn: Long = changeLsn

  def getTxId: Long = txId

  def getBeginLsn: Long = beginLsn

  override def toString: String = if (this eq TxLogPosition.NULL) "NULL"
  else commitLsn + ":" + changeLsn + ":" + txId + ":" + beginLsn

  override def hashCode: Int = {
    val prime = 31
    var result = 1
    result = prime * result + (if (commitLsn == null) 0
    else commitLsn.hashCode)
    result = prime * result + (if (changeLsn == null) 0
    else changeLsn.hashCode)
    result = prime * result + (if (beginLsn == null) 0
    else beginLsn.hashCode)
    result = prime * result + (if (txId == null) 0
    else txId.hashCode)
    result
  }

  override def equals(obj: Any): Boolean = {
    if (this eq obj.asInstanceOf[AnyRef]) return true
    if (obj == null) return false
    if (getClass ne obj.getClass) return false
    val other = obj.asInstanceOf[TxLogPosition]
    if (commitLsn == null) if (other.commitLsn != null) return false
    else if (!(commitLsn == other.commitLsn)) return false
    if (changeLsn == null) if (other.changeLsn != null) return false
    else if (!(changeLsn == other.changeLsn)) return false
    if (beginLsn == null) if (other.beginLsn != null) return false
    else if (!(beginLsn == other.beginLsn)) return false
    if (txId == null) if (other.txId != null) return false
    else if (!(txId == other.txId)) return false
    true
  }

  override def compareTo(o: TxLogPosition): Int = {
    val comparison = commitLsn.compareTo(o.getCommitLsn)
    if (comparison == 0) changeLsn.compareTo(o.changeLsn)
    else comparison
  }

  override def isAvailable: Boolean = changeLsn != null && commitLsn != null && beginLsn != null && txId !=null
}

