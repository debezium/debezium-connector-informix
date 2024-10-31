/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import java.time.Instant;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.SnapshotType;
import io.debezium.pipeline.CommonOffsetContext;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.TableId;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Collect;

public class InformixOffsetContext extends CommonOffsetContext<SourceInfo> {

    private final Schema sourceInfoSchema;
    private final TransactionContext transactionContext;
    private final IncrementalSnapshotContext<TableId> incrementalSnapshotContext;

    public InformixOffsetContext(InformixConnectorConfig connectorConfig, TxLogPosition position, SnapshotType snapshot, boolean snapshotCompleted,
                                 TransactionContext transactionContext, IncrementalSnapshotContext<TableId> incrementalSnapshotContext) {
        super(new SourceInfo(connectorConfig), snapshotCompleted);

        sourceInfo.setCommitLsn(position.getCommitLsn());
        sourceInfo.setChangeLsn(position.getChangeLsn());
        sourceInfo.setBeginLsn(position.getBeginLsn());
        sourceInfoSchema = sourceInfo.schema();

        if (this.snapshotCompleted) {
            postSnapshotCompletion();
        }
        else {
            setSnapshot(snapshot);
            sourceInfo.setSnapshot(snapshot != null ? SnapshotRecord.TRUE : SnapshotRecord.FALSE);
        }

        this.transactionContext = transactionContext;

        this.incrementalSnapshotContext = incrementalSnapshotContext;
    }

    public InformixOffsetContext(InformixConnectorConfig connectorConfig, TxLogPosition position, SnapshotType snapshot, boolean snapshotCompleted) {
        this(connectorConfig, position, snapshot, snapshotCompleted, new TransactionContext(), new SignalBasedIncrementalSnapshotContext<>(false));
    }

    @Override
    public Map<String, ?> getOffset() {
        if (getSnapshot().isPresent()) {
            return Collect.hashMapOf(
                    AbstractSourceInfo.SNAPSHOT_KEY, getSnapshot().get().toString(),
                    SNAPSHOT_COMPLETED_KEY, snapshotCompleted,
                    SourceInfo.COMMIT_LSN_KEY, sourceInfo.getCommitLsn().toString());
        }
        else {
            return incrementalSnapshotContext.store(transactionContext.store(Collect.hashMapOf(
                    SourceInfo.COMMIT_LSN_KEY, sourceInfo.getCommitLsn().toString(),
                    SourceInfo.CHANGE_LSN_KEY, sourceInfo.getChangeLsn() == null ? null : sourceInfo.getChangeLsn().toString(),
                    SourceInfo.BEGIN_LSN_KEY, sourceInfo.getBeginLsn() == null ? null : sourceInfo.getBeginLsn().toString())));
        }
    }

    @Override
    public Schema getSourceInfoSchema() {
        return sourceInfoSchema;
    }

    public TxLogPosition getChangePosition() {
        return TxLogPosition.valueOf(sourceInfo.getCommitLsn(), sourceInfo.getChangeLsn(), sourceInfo.getTxId(), sourceInfo.getBeginLsn());
    }

    public void setChangePosition(TxLogPosition position) {
        sourceInfo.setCommitLsn(position.getCommitLsn());
        sourceInfo.setChangeLsn(position.getChangeLsn());

        sourceInfo.setTxId(position.getTxId());
        sourceInfo.setBeginLsn(position.getBeginLsn());
    }

    public boolean isSnapshotCompleted() {
        return snapshotCompleted;
    }

    @Override
    public void event(DataCollectionId tableId, Instant timestamp) {
        sourceInfo.setTimestamp(timestamp);
        sourceInfo.setTableId((TableId) tableId);
    }

    @Override
    public TransactionContext getTransactionContext() {
        return transactionContext;
    }

    @Override
    public IncrementalSnapshotContext<?> getIncrementalSnapshotContext() {
        return incrementalSnapshotContext;
    }

    @Override
    public String toString() {
        return "InformixOffsetContext [" +
                "sourceInfoSchema=" + sourceInfoSchema +
                ", sourceInfo=" + sourceInfo +
                ", snapshotCompleted=" + snapshotCompleted + "]";
    }

    public static class Loader implements OffsetContext.Loader<InformixOffsetContext> {

        private final InformixConnectorConfig connectorConfig;

        public Loader(InformixConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
        }

        @Override
        public InformixOffsetContext load(Map<String, ?> offset) {
            final Lsn commitLsn = Lsn.of((String) offset.get(SourceInfo.COMMIT_LSN_KEY));
            final Lsn changeLsn = Lsn.of((String) offset.get(SourceInfo.CHANGE_LSN_KEY));
            final Lsn beginLsn = Lsn.of((String) offset.get(SourceInfo.BEGIN_LSN_KEY));

            final SnapshotType snapshot = loadSnapshot(offset).orElse(null);
            boolean snapshotCompleted = loadSnapshotCompleted(offset);

            return new InformixOffsetContext(connectorConfig, TxLogPosition.valueOf(commitLsn, changeLsn, beginLsn), snapshot, snapshotCompleted,
                    TransactionContext.load(offset), SignalBasedIncrementalSnapshotContext.load(offset, false));
        }
    }
}
