package laoflch.debezium.connector.informix.integrtest

import java.time.Instant
import java.util
import java.util.Map

import org.apache.kafka.connect.data.Struct
import io.debezium.data.Envelope
import io.debezium.pipeline.source.spi.EventMetadataProvider
import io.debezium.pipeline.spi.OffsetContext
import io.debezium.schema.DataCollectionId
import io.debezium.util.Collect


class InformixEventMetadataProvider extends EventMetadataProvider {

  override def getEventTimestamp(source: DataCollectionId, offset: OffsetContext, key: Any, value: Struct): Instant = {
    if (value == null) return null
    val sourceInfo = value.getStruct(Envelope.FieldName.SOURCE)
    if (source == null) return null
    val timestamp = sourceInfo.getInt64(SourceInfo.TIMESTAMP_KEY)
    if (timestamp == null) null
    else Instant.ofEpochMilli(timestamp)
  }

  override def getEventSourcePosition(source: DataCollectionId, offset: OffsetContext, key: Any, value: Struct): util.Map[String, String] = {
    if (value == null) return null
    val sourceInfo = value.getStruct(Envelope.FieldName.SOURCE)
    if (source == null) return null
    Collect.hashMapOf(/*SourceInfo.COMMIT_LSN_KEY, sourceInfo.getString(SourceInfo.COMMIT_LSN_KEY),*/ SourceInfo.CHANGE_LSN_KEY, sourceInfo.getString(SourceInfo.CHANGE_LSN_KEY))
  }

  override def getTransactionId(source: DataCollectionId, offset: OffsetContext, key: Any, value: Struct): String = {
    if (value == null) return null
    val sourceInfo = value.getStruct(Envelope.FieldName.SOURCE)
    if (source == null) return null
    sourceInfo.getString(SourceInfo.TX_ID)
  }
}

