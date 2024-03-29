/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import java.time.Instant;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.function.BlockingConsumer;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionMonitor;
import io.debezium.schema.SchemaNameAdjuster;

public class InformixTransactionMonitor extends TransactionMonitor {
    public InformixTransactionMonitor(CommonConnectorConfig connectorConfig, EventMetadataProvider eventMetadataProvider,
                                      SchemaNameAdjuster schemaNameAdjuster, BlockingConsumer<SourceRecord> sender,
                                      String topicName) {
        super(connectorConfig, eventMetadataProvider, schemaNameAdjuster, sender, topicName);
    }

    @Override
    protected Struct prepareTxKey(OffsetContext offsetContext) {
        return adjustTxId(new Struct(transactionKeySchema), offsetContext);
    }

    @Override
    protected Struct prepareTxBeginValue(OffsetContext offsetContext, Instant timestamp) {
        return adjustTxId(super.prepareTxBeginValue(offsetContext, timestamp), offsetContext);
    }

    @Override
    protected Struct prepareTxEndValue(OffsetContext offsetContext, Instant timestamp) {
        return adjustTxId(super.prepareTxEndValue(offsetContext, timestamp), offsetContext);
    }

    @Override
    protected Struct prepareTxStruct(OffsetContext offsetContext, long dataCollectionEventOrder, Struct value) {
        return adjustTxId(super.prepareTxStruct(offsetContext, dataCollectionEventOrder, value), offsetContext);
    }

    private Struct adjustTxId(Struct txStruct, OffsetContext offsetContext) {
        final String lsn = ((InformixOffsetContext) offsetContext).getChangePosition().getCommitLsn().toString();
        final String txId = offsetContext.getTransactionContext().getTransactionId();
        txStruct.put(DEBEZIUM_TRANSACTION_ID_KEY, String.format("%s:%s", txId, lsn));
        return txStruct;
    }
}
