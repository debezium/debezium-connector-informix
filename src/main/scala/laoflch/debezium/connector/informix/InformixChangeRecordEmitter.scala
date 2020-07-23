/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package laoflch.debezium.connector.informix.integrtest

import com.informix.jdbc.IfmxReadableType
import io.debezium.data.Envelope
import io.debezium.data.Envelope.Operation
import io.debezium.pipeline.spi.OffsetContext
import io.debezium.relational.RelationalChangeRecordEmitter
import io.debezium.util.Clock

import scala.jdk.CollectionConverters


/**
 * Emits change data based on a single (or two in case of updates) CDC data row(s).
 *
 * @author Jiri Pechanec
 */
object InformixChangeRecordEmitter {
  val OP_DELETE = 0x01
  val OP_INSERT = 0x02
  val OP_UPDATE = 0x03
  //val OP_UPDATE_AFTER = 0x04
  val OP_TRUNCATE = 0x05

  /**
   * convert columns data from map[String,IfmxReadableType] to array[AnyRef].
   * debezium can't convert the IfmxReadableType object to kafka direct,so use map[AnyRef](x=>x.toObject) to extract the jave type value
   * from IfmxReadableType and pass to debezium for kafka
   * @param data the data from informix cdc map[String,IfmxReadableType].
   * @author laoflch luo
   */
  def convertIfxData2Array(data: java.util.Map[String, IfmxReadableType]): Array[AnyRef] = {
    if (data == null) {return Array.emptyObjectArray}

    CollectionConverters.CollectionHasAsScala(data.values()).asScala.map[AnyRef](x=>x.toObject).toArray
    //extract value from IfmxReadableType and pass it to ValueConverter for kafka value

  }
}
class InformixChangeRecordEmitter(val offset: OffsetContext, val operation: Int, val data: Array[AnyRef], val dataNext: Array[AnyRef], val clock: Clock) extends RelationalChangeRecordEmitter(offset, clock) {
  override protected def getOperation: Envelope.Operation = {
    if (operation == InformixChangeRecordEmitter.OP_DELETE) return Operation.DELETE
    else if (operation == InformixChangeRecordEmitter.OP_INSERT) return Operation.CREATE
    else if (operation == InformixChangeRecordEmitter.OP_UPDATE) return Operation.UPDATE
    throw new IllegalArgumentException("Received event of unexpected command type: " + operation)
  }

  override protected def getOldColumnValues: Array[AnyRef] = getOperation match {
    case Operation.CREATE =>null
    case Operation.READ =>null
    case _ =>
      data
  }

  override protected def getNewColumnValues: Array[AnyRef] = getOperation match {
    case Operation.CREATE =>dataNext
    case Operation.READ =>
      data
    case Operation.UPDATE =>
      dataNext
    case _ =>
      null
  }


}

