package laoflch.debezium.connector.informix

import java.util

import com.informix.jdbc.IfmxReadableType
import io.debezium.relational.TableId

import scala.collection.mutable

class InformixTransactionCache {
  //private val beginAndUnCommit: mutable.Map[(Long,Long),mutable.Buffer[(TableId,InformixChangeRecordEmitter)]] = new mutable.HashMap[(Long,Long),mutable.Buffer[(TableId,InformixChangeRecordEmitter)]]
  private val transactionCRE: mutable.Map[Long, mutable.Buffer[(TableId, InformixChangeRecordEmitter)]] = new mutable.HashMap[Long, mutable.Buffer[(TableId, InformixChangeRecordEmitter)]]
  private val beforeAndAfter: mutable.Map[Long, util.Map[String, IfmxReadableType]] = new mutable.HashMap[Long, util.Map[String, IfmxReadableType]]

  /**
   * handle begin and commit for one txn
   */
  def beginTxn(txn: Long): Option[mutable.Buffer[(TableId, InformixChangeRecordEmitter)]] = {
    if (!transactionCRE.contains(txn)) {

      val cre = new mutable.ArrayBuffer[(TableId, InformixChangeRecordEmitter)](initialSize = 4096)

      // beginAndUnCommit.put((beginSeq,txn),cre)

      return transactionCRE.put(txn, cre)
    }

    None
  }

  def commitTxn(txn: Long): Option[mutable.Buffer[(TableId, InformixChangeRecordEmitter)]] = {
    if (transactionCRE.contains(txn)) {

      // beginAndUnCommit.remove(txn)

      return transactionCRE.remove(txn)
    }

    None
  }

  def rollbackTxn(txn: Long): Option[mutable.Buffer[(TableId, InformixChangeRecordEmitter)]] = {
    if (transactionCRE.contains(txn)) {

      //beginAndUnCommit.remove(commitSeq,txn)

      return transactionCRE.remove(txn)
    }

    None
  }

  def addEvent2Tx(tableId: TableId, event: InformixChangeRecordEmitter, txn: Long): mutable.Buffer[(TableId, InformixChangeRecordEmitter)] = {
    if (event != null) {
      val buffer = transactionCRE.get(txn)
      if (buffer != None) {
        return buffer.get.addOne((tableId, event))
      }
    }

    null
  }

  /**
   * handle before and after for one update operation
   */
  def beforeUpdate(txn: Long, data: util.Map[String, IfmxReadableType]): Option[util.Map[String, IfmxReadableType]] = {
    if (!beforeAndAfter.contains(txn)) {
      beforeAndAfter.put(txn, data)
    }

    None
  }

  def afterUpdate(txn: Long): Option[util.Map[String, IfmxReadableType]] = {
    if (beforeAndAfter.contains(txn)) {
      return beforeAndAfter.remove(txn)
    }

    None
  }

  def getTransactionCRE(): mutable.Map[Long, mutable.Buffer[(TableId, InformixChangeRecordEmitter)]] = transactionCRE

}
