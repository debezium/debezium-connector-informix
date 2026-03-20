/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import static java.lang.Thread.currentThread;

import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.informix.jdbc.stream.api.StreamOperationRecord;
import com.informix.jdbc.stream.api.StreamRecord;
import com.informix.jdbc.stream.cdc.records.CDCBeginTransactionRecord;
import com.informix.jdbc.stream.cdc.records.CDCCommitTransactionRecord;
import com.informix.jdbc.stream.cdc.records.CDCMetaDataRecord;
import com.informix.jdbc.stream.cdc.records.CDCTruncateRecord;
import com.informix.jdbc.stream.impl.StreamException;
import com.informix.jdbc.types.ReadableType;

import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.util.Clock;

public class InformixStreamingChangeEventSource implements StreamingChangeEventSource<InformixPartition, InformixOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(InformixStreamingChangeEventSource.class);

    private static final String RECEIVED_GENERIC_RECORD = "Received {} ElapsedT [{}ms]";
    private static final String RECEIVED_UNKNOWN_RECORD_TYPE = "Received unknown record-type {} ElapsedT [{}ms]";

    private final InformixConnectorConfig connectorConfig;
    private final InformixConnection dataConnection;
    private final InformixConnection metadataConnection;
    private final EventDispatcher<InformixPartition, TableId> dispatcher;
    private final ErrorHandler errorHandler;
    private final Clock clock;
    private final InformixDatabaseSchema schema;
    private InformixOffsetContext effectiveOffsetContext;

    public InformixStreamingChangeEventSource(InformixConnectorConfig connectorConfig,
                                              InformixConnection dataConnection, InformixConnection metadataConnection,
                                              EventDispatcher<InformixPartition, TableId> dispatcher, ErrorHandler errorHandler,
                                              Clock clock, InformixDatabaseSchema schema) {
        this.connectorConfig = connectorConfig;
        this.dataConnection = dataConnection;
        this.metadataConnection = metadataConnection;
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.clock = clock;
        this.schema = schema;
    }

    @Override
    public void init(InformixOffsetContext offsetContext) {
        this.effectiveOffsetContext = offsetContext == null
                ? new InformixOffsetContext(
                        connectorConfig,
                        TxLogPosition.current(),
                        null,
                        false)
                : offsetContext;
    }

    /**
     * Executes this source. Implementations should regularly check via the given context if they should stop. If that's
     * the case, they should abort their processing and perform any clean-up needed, such as rolling back pending
     * transactions, releasing locks etc.
     *
     * @param context contextual information for this source's execution
     * @throws InterruptedException in case the snapshot was aborted before completion
     */
    @Override
    public void execute(ChangeEventSourceContext context, InformixPartition partition, InformixOffsetContext offsetContext)
            throws InterruptedException {

        // Need to refresh schema before CDCEngine is started, to capture columns added in off-line schema evolution
        schema.tableIds().stream().map(TableId::schema).distinct().forEach(schemaName -> {
            try {
                metadataConnection.readSchema(
                        schema.tables(),
                        null,
                        schemaName,
                        schema.getTableFilter(),
                        null,
                        true);
            }
            catch (SQLException e) {
                LOGGER.error("Caught SQLException", e);
                errorHandler.setProducerThrowable(e);
            }
        });

        TxLogPosition lastPosition = offsetContext.getChangePosition();
        Lsn lastCommitLsn = lastPosition.getCommitLsn();
        Lsn lastChangeLsn = lastPosition.getChangeLsn();
        Lsn lastBeginLsn = lastPosition.getBeginLsn();
        Lsn beginLsn = lastBeginLsn.isAvailable() ? lastBeginLsn : lastCommitLsn;

        try (InformixCdcTransactionEngine transactionEngine = getTransactionEngine(context, schema, beginLsn)) {
            transactionEngine.init();

            /*
             * Recover Stage. In this stage, we replay event from 'beginLsn' to 'commitLsn', and rebuild the transactionCache.
             */
            if (beginLsn.compareTo(lastCommitLsn) < 0) {
                LOGGER.info("Begin recover: from lastBeginLsn='{}' to lastCommitLsn='{}'", lastBeginLsn, lastCommitLsn);
                boolean recovering = true;
                while (context.isRunning() && recovering) {

                    if (context.isPaused()) {
                        LOGGER.info("Streaming will now pause");
                        context.streamingPaused();
                        context.waitSnapshotCompletion();
                        LOGGER.info("Streaming resumed");
                    }

                    dispatcher.dispatchHeartbeatEvent(partition, offsetContext);

                    StreamRecord streamRecord = transactionEngine.getRecord();
                    if (streamRecord == null) {
                        LOGGER.debug(RECEIVED_GENERIC_RECORD, streamRecord, 0);
                        continue;
                    }

                    switch (streamRecord.getType()) {
                        case TRANSACTION_GROUP -> {
                            InformixStreamTransactionRecord transactionRecord = (InformixStreamTransactionRecord) streamRecord;

                            Lsn commitLsn = Lsn.of(transactionRecord.getEndRecord().getSequenceId());
                            if (commitLsn.compareTo(lastCommitLsn) < 0) {
                                LOGGER.info("Skipping transaction with id: '{}' since commitLsn='{}' < lastCommitLsn='{}'",
                                        transactionRecord.getTransactionId(), commitLsn, lastCommitLsn);
                                break;
                            }
                            if (commitLsn.equals(lastCommitLsn) && lastChangeLsn.equals(lastCommitLsn)) {
                                LOGGER.info("Skipping transaction with id: '{}' since commitLsn='{}' == lastCommitLsn='{}' and lastChangeLsn='{}' == lastCommitLsn",
                                        transactionRecord.getTransactionId(), commitLsn, lastCommitLsn, lastChangeLsn);
                                break;
                            }
                            if (commitLsn.compareTo(lastCommitLsn) > 0) {
                                Lsn currentLsn = Lsn.of(transactionRecord.getBeginRecord().getSequenceId());
                                LOGGER.info("Recover finished: from lastBeginLsn='{}' to lastCommitLsn='{}', current Lsn='{}'",
                                        lastBeginLsn, lastCommitLsn, currentLsn);
                                recovering = false;
                            }
                            handleTransaction(transactionEngine, partition, offsetContext, transactionRecord, recovering);
                        }
                        case METADATA -> handleMetadata(partition, offsetContext, transactionEngine, (CDCMetaDataRecord) streamRecord);
                        case TIMEOUT -> LOGGER.trace(RECEIVED_GENERIC_RECORD, streamRecord, 0);
                        case ERROR -> LOGGER.error(RECEIVED_GENERIC_RECORD, streamRecord, 0);
                        default -> LOGGER.warn(RECEIVED_UNKNOWN_RECORD_TYPE, streamRecord, 0);
                    }
                }
            }

            /*
             * Main Handler Loop
             */
            while (context.isRunning()) {

                if (context.isPaused()) {
                    LOGGER.info("Streaming will now pause");
                    context.streamingPaused();
                    context.waitSnapshotCompletion();
                    LOGGER.info("Streaming resumed");
                }

                dispatcher.dispatchHeartbeatEvent(partition, offsetContext);

                StreamRecord streamRecord = transactionEngine.getRecord();
                if (streamRecord == null) {
                    LOGGER.debug(RECEIVED_GENERIC_RECORD, streamRecord, 0);
                    continue;
                }

                switch (streamRecord.getType()) {
                    case TRANSACTION_GROUP -> handleTransaction(transactionEngine, partition, offsetContext, (InformixStreamTransactionRecord) streamRecord, false);
                    case METADATA -> handleMetadata(partition, offsetContext, transactionEngine, (CDCMetaDataRecord) streamRecord);
                    case TIMEOUT -> LOGGER.trace(RECEIVED_GENERIC_RECORD, streamRecord, 0);
                    case ERROR -> LOGGER.error(RECEIVED_GENERIC_RECORD, streamRecord, 0);
                    default -> LOGGER.warn(RECEIVED_UNKNOWN_RECORD_TYPE, streamRecord, 0);
                }
            }
        }
        catch (InterruptedException e) {
            LOGGER.error("Caught InterruptedException", e);
            errorHandler.setProducerThrowable(e);
            currentThread().interrupt();
        }
        catch (Exception e) {
            LOGGER.error("Caught Exception", e);
            errorHandler.setProducerThrowable(e);
        }
    }

    @Override
    public void commitOffset(Map<String, ?> partition, Map<String, ?> offset) {
        // NOOP
    }

    @Override
    public InformixOffsetContext getOffsetContext() {
        return effectiveOffsetContext;
    }

    private InformixCdcTransactionEngine getTransactionEngine(ChangeEventSourceContext context, InformixDatabaseSchema schema, Lsn startLsn)
            throws SQLException {
        InformixCdcTransactionEngine.Builder builder = InformixCdcTransactionEngine
                .builder(dataConnection.datasource())
                .buffer(connectorConfig.getCdcBuffersize())
                .timeout(connectorConfig.getCdcTimeout())
                .stopLoggingOnClose(connectorConfig.stopLoggingOnClose())
                .returnEmptyTransactions(connectorConfig.returnEmptytransactions())
                .context(context);

        schema.tableIds().forEach((TableId tid) -> {
            String[] colNames = schema.tableFor(tid).retrieveColumnNames().stream()
                    .filter(colName -> schema.getColumnFilter().matches(tid.catalog(), tid.schema(), tid.table(), colName))
                    .map(dataConnection::quoteIdentifier).toArray(String[]::new);
            builder.watchTable(dataConnection.quotedTableIdString(tid), colNames);
        });

        if (startLsn.isAvailable()) {
            builder.sequenceId(startLsn.sequence());
        }
        if (LOGGER.isInfoEnabled()) {
            long sequence = builder.getSequenceId();
            LOGGER.info("Set CDCEngine's LSN to '{}' aka {}", sequence, Lsn.of(sequence).toLongString());
        }

        return builder.build();
    }

    private void handleTransaction(InformixCdcTransactionEngine engine, InformixPartition partition, InformixOffsetContext offsetContext,
                                   InformixStreamTransactionRecord transactionRecord, boolean recover)
            throws InterruptedException, StreamException {
        long tStart = System.nanoTime();

        long lastChangeSeq = offsetContext.getChangePosition().getChangeLsn().sequence();

        int transactionId = transactionRecord.getTransactionId();

        CDCBeginTransactionRecord beginRecord = transactionRecord.getBeginRecord();
        StreamRecord endRecord = transactionRecord.getEndRecord();

        long start = System.nanoTime();

        long beginTs = beginRecord.getTime();
        long beginSeq = beginRecord.getSequenceId();
        long endSeq = endRecord.getSequenceId();
        long restartSeq = engine.getLowestBeginSequence().orElse(endSeq);

        if (!recover) {
            updateChangePosition(offsetContext, endSeq, beginSeq, transactionId, Math.min(restartSeq, beginSeq));
        }

        long end = System.nanoTime();

        LOGGER.debug("Received {} Time [{}] UserId [{}] ElapsedT [{}ms]",
                beginRecord, beginTs, beginRecord.getUserId(), (end - start) / 1000000d);

        switch (endRecord.getType()) {
            case COMMIT -> {
                CDCCommitTransactionRecord commitRecord = (CDCCommitTransactionRecord) endRecord;
                long commitTs = commitRecord.getTime();

                Map<String, ReadableType> before = null;
                Map<String, TableId> label2TableId = engine.getTableIdByLabelId();

                dispatcher.dispatchTransactionStartedEvent(
                        partition,
                        String.valueOf(transactionId),
                        offsetContext,
                        Instant.ofEpochSecond(beginTs));

                List<StreamRecord> streamRecords = transactionRecord.getRecords();
                long finalChangeSeq = streamRecords.isEmpty() ? -1L : streamRecords.get(streamRecords.size() - 1).getSequenceId(); // No .getLast() in Java 17

                for (StreamRecord streamRecord : streamRecords) {
                    start = System.nanoTime();

                    long changeSeq = streamRecord.getSequenceId();

                    if (recover && changeSeq <= lastChangeSeq) {
                        LOGGER.info("Skipping already processed record {}", changeSeq);
                        continue;
                    }

                    Optional<TableId> tableId = Optional.ofNullable(streamRecord.getLabel()).map(label2TableId::get);

                    Map<String, ReadableType> after;

                    updateChangePosition(offsetContext, null, changeSeq, transactionId, null);
                    /*
                     * When transaction metadata is not provided, no transaction commited event is sent and change position
                     * is not updated with endSeq and restartSeq. After a restart, the transaction will be recovered but all
                     * operations are skipped as they have already been processed. Instead, for the last record of the
                     * transaction change position is updated with changeSeq = endSeq and beginSeq = restartSeq
                     */
                    if (!connectorConfig.shouldProvideTransactionMetadata() && changeSeq == finalChangeSeq) {
                        updateChangePosition(offsetContext, null, endSeq, transactionId, restartSeq);
                    }

                    switch (streamRecord.getType()) {
                        case INSERT -> {

                            after = ((StreamOperationRecord) streamRecord).getData();

                            handleOperation(partition, offsetContext, tableId.orElseThrow(), Operation.CREATE, null, after);

                            end = System.nanoTime();

                            LOGGER.debug("Received {} ElapsedT [{}ms] Data After [{}]", streamRecord, (end - start) / 1000000d, after);
                        }
                        case BEFORE_UPDATE -> {

                            before = ((StreamOperationRecord) streamRecord).getData();

                            end = System.nanoTime();

                            LOGGER.debug("Received {} ElapsedT [{}ms] Data Before [{}]", streamRecord, (end - start) / 1000000d, before);
                        }
                        case AFTER_UPDATE -> {

                            after = ((StreamOperationRecord) streamRecord).getData();

                            handleOperation(partition, offsetContext, tableId.orElseThrow(), Operation.UPDATE, before, after);

                            end = System.nanoTime();

                            LOGGER.debug("Received {} ElapsedT [{}ms] Data Before [{}] Data After [{}]", streamRecord, (end - start) / 1000000d, before, after);
                        }
                        case DELETE -> {

                            before = ((StreamOperationRecord) streamRecord).getData();

                            handleOperation(partition, offsetContext, tableId.orElseThrow(), Operation.DELETE, before, null);

                            end = System.nanoTime();

                            LOGGER.debug("Received {} ElapsedT [{}ms] Data Before [{}]", streamRecord, (end - start) / 1000000d, before);
                        }
                        case TRUNCATE -> {
                            /*
                             * According to IBM documentation the 'User data' field of the CDC_REC_TRUNCATE record header contains the
                             * table identifier, otherwise placed in the IfxCDCRecord 'label' field. For unknown reasons, this is
                             * instead placed in the 'userId' field?
                             */
                            CDCTruncateRecord truncateRecord = (CDCTruncateRecord) streamRecord;
                            tableId = Optional.of(truncateRecord.getUserId()).map(Number::toString).map(label2TableId::get);

                            handleOperation(partition, offsetContext, tableId.orElseThrow(), Operation.TRUNCATE, null, null);

                            LOGGER.debug(RECEIVED_GENERIC_RECORD, streamRecord, (end - start) / 1000000d);
                        }
                        case METADATA, TIMEOUT, ERROR -> {
                            end = System.nanoTime();

                            LOGGER.debug(RECEIVED_GENERIC_RECORD, streamRecord, (end - start) / 1000000d);
                        }
                        default -> {
                            end = System.nanoTime();

                            LOGGER.debug(RECEIVED_UNKNOWN_RECORD_TYPE, streamRecord, (end - start) / 1000000d);
                        }
                    }
                }

                start = System.nanoTime();

                updateChangePosition(offsetContext, endSeq, endSeq, transactionId, restartSeq);
                dispatcher.dispatchTransactionCommittedEvent(partition, offsetContext, Instant.ofEpochSecond(commitTs));

                end = System.nanoTime();

                LOGGER.debug("Received {} Time [{}] UserId [{}] ElapsedT [{}ms]", endRecord, commitTs, beginRecord.getUserId(), (end - start) / 1000000d);

                LOGGER.debug("Handle Transaction Events [{}], ElapsedT [{}ms]", streamRecords.size(), (end - tStart) / 1000000d);
            }
            case ROLLBACK -> {

                if (!recover) {
                    updateChangePosition(offsetContext, endSeq, endSeq, transactionId, restartSeq);
                    offsetContext.getTransactionContext().endTransaction();
                }

                end = System.nanoTime();

                LOGGER.debug(RECEIVED_GENERIC_RECORD, endRecord, (end - start) / 1000000d);
            }
        }
    }

    private void handleMetadata(InformixPartition partition, InformixOffsetContext offsetContext, InformixCdcTransactionEngine engine, CDCMetaDataRecord metaDataRecord)
            throws InterruptedException {
        long start = System.nanoTime();
        TableId tableId = engine.getTableIdByLabelId().get(metaDataRecord.getLabel());

        offsetContext.event(tableId, Instant.now());

        dispatcher.dispatchSchemaChangeEvent(partition, offsetContext, null, receiver -> {
            final SchemaChangeEvent event = SchemaChangeEvent.ofAlter(
                    partition,
                    offsetContext,
                    tableId.catalog(),
                    tableId.schema(),
                    "n/a",
                    schema.tableFor(tableId));
            if (!schema.skipSchemaChangeEvent(event)) {
                receiver.schemaChangeEvent(event);
            }
        });
        long end = System.nanoTime();
        LOGGER.debug(RECEIVED_GENERIC_RECORD, metaDataRecord, (end - start) / 1000000d);
    }

    private void updateChangePosition(InformixOffsetContext offsetContext, Long commitSeq, Long changeSeq, Integer transactionId, Long beginSeq) {
        offsetContext.setChangePosition(
                TxLogPosition.cloneAndSet(
                        offsetContext.getChangePosition(),
                        Lsn.of(commitSeq),
                        Lsn.of(changeSeq),
                        transactionId,
                        Lsn.of(beginSeq)));
    }

    private void handleOperation(InformixPartition partition, InformixOffsetContext offsetContext, TableId tableId,
                                 Operation operation, Map<String, ReadableType> before, Map<String, ReadableType> after)
            throws InterruptedException {
        offsetContext.event(tableId, clock.currentTime());

        dispatcher.dispatchDataChangeEvent(partition, tableId,
                new InformixChangeRecordEmitter(partition, offsetContext, clock, connectorConfig, schema, tableId, operation, before, after));
    }
}