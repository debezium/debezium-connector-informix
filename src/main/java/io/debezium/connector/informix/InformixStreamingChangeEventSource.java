/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import static java.lang.Thread.currentThread;

import java.sql.SQLException;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.informix.jdbc.IfmxReadableType;
import com.informix.stream.api.IfmxStreamOperationRecord;
import com.informix.stream.api.IfmxStreamRecord;
import com.informix.stream.api.IfmxStreamRecordType;
import com.informix.stream.cdc.IfxCDCEngine;
import com.informix.stream.cdc.records.IfxCDCBeginTransactionRecord;
import com.informix.stream.cdc.records.IfxCDCCommitTransactionRecord;
import com.informix.stream.cdc.records.IfxCDCMetaDataRecord;
import com.informix.stream.cdc.records.IfxCDCTruncateRecord;
import com.informix.stream.impl.IfxStreamException;

import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.Table;
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
                        false,
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
        try {
            for (TableId tableId : schema.tableIds()) {
                final Table table = metadataConnection.getTableSchemaFromTableId(tableId);
                schema.refresh(table);
            }
        }
        catch (SQLException e) {
            LOGGER.error("Caught SQLException", e);
            errorHandler.setProducerThrowable(e);
        }

        TxLogPosition lastPosition = offsetContext.getChangePosition();
        Lsn lastCommitLsn = lastPosition.getCommitLsn();
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

                    IfmxStreamRecord streamRecord = transactionEngine.getRecord();

                    switch (streamRecord.getType()) {
                        case TRANSACTION_GROUP:
                            InformixStreamTransactionRecord transactionRecord = (InformixStreamTransactionRecord) streamRecord;

                            Lsn commitLsn = Lsn.of(transactionRecord.getEndRecord().getSequenceId());
                            if (commitLsn.compareTo(lastCommitLsn) < 0) {
                                LOGGER.info("Skipping transaction with id: '{}' since commitLsn='{}' < lastCommitLsn='{}'",
                                        transactionRecord.getTransactionId(), commitLsn, lastCommitLsn);
                                break;
                            }
                            if (commitLsn.compareTo(lastCommitLsn) > 0) {
                                LOGGER.info("Recover finished: from lastBeginLsn='{}' to lastCommitLsn='{}', current Lsn='{}'",
                                        lastBeginLsn, lastCommitLsn, commitLsn);
                                recovering = false;
                            }
                            handleTransaction(transactionEngine, partition, offsetContext, transactionRecord, recovering);
                            break;
                        case METADATA:
                            handleMetadata(partition, offsetContext, transactionEngine, (IfxCDCMetaDataRecord) streamRecord);
                            break;
                        case TIMEOUT:
                            LOGGER.trace(RECEIVED_GENERIC_RECORD, streamRecord, 0);
                            break;
                        case ERROR:
                            LOGGER.error(RECEIVED_GENERIC_RECORD, streamRecord, 0);
                            break;
                        default:
                            LOGGER.warn(RECEIVED_UNKNOWN_RECORD_TYPE, streamRecord, 0);
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

                IfmxStreamRecord streamRecord = transactionEngine.getRecord();

                switch (streamRecord.getType()) {
                    case TRANSACTION_GROUP:
                        handleTransaction(transactionEngine, partition, offsetContext, (InformixStreamTransactionRecord) streamRecord, false);
                        break;
                    case METADATA:
                        handleMetadata(partition, offsetContext, transactionEngine, (IfxCDCMetaDataRecord) streamRecord);
                        break;
                    case TIMEOUT:
                        LOGGER.trace(RECEIVED_GENERIC_RECORD, streamRecord, 0);
                        break;
                    case ERROR:
                        LOGGER.error(RECEIVED_GENERIC_RECORD, streamRecord, 0);
                        break;
                    default:
                        LOGGER.warn(RECEIVED_UNKNOWN_RECORD_TYPE, streamRecord, 0);
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

    public InformixCdcTransactionEngine getTransactionEngine(ChangeEventSourceContext context,
                                                             InformixDatabaseSchema schema,
                                                             Lsn startLsn)
            throws SQLException {
        return new InformixCdcTransactionEngine(context, getCDCEngine(schema, startLsn));
    }

    private IfxCDCEngine getCDCEngine(InformixDatabaseSchema schema, Lsn startLsn) throws SQLException {
        IfxCDCEngine.Builder builder = IfxCDCEngine
                .builder(dataConnection.datasource())
                .buffer(connectorConfig.getCdcBuffersize())
                .timeout(connectorConfig.getCdcTimeout());

        schema.tableIds().forEach((TableId tid) -> {
            String[] colNames = schema.tableFor(tid).retrieveColumnNames().toArray(String[]::new);
            builder.watchTable(tid.identifier(), colNames);
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

    private void handleTransaction(InformixCdcTransactionEngine engine, InformixPartition partition,
                                   InformixOffsetContext offsetContext, InformixStreamTransactionRecord transactionRecord,
                                   boolean recover)
            throws InterruptedException, IfxStreamException {
        long tStart = System.nanoTime();

        int transactionId = transactionRecord.getTransactionId();

        IfxCDCBeginTransactionRecord beginRecord = transactionRecord.getBeginRecord();
        IfmxStreamRecord endRecord = transactionRecord.getEndRecord();

        long start = System.nanoTime();

        long beginTs = beginRecord.getTime();
        long beginSeq = beginRecord.getSequenceId();
        long lowestBeginSeq = engine.getLowestBeginSequence().orElse(beginSeq);
        long endSeq = endRecord.getSequenceId();

        if (!recover) {
            updateChangePosition(offsetContext, endSeq, beginSeq, transactionId, lowestBeginSeq < beginSeq ? lowestBeginSeq : beginSeq);
            dispatcher.dispatchTransactionStartedEvent(
                    partition,
                    String.valueOf(transactionId),
                    offsetContext,
                    Instant.ofEpochSecond(beginTs));
        }

        long end = System.nanoTime();

        LOGGER.debug("Received {} Time [{}] UserId [{}] ElapsedT [{}ms]",
                beginRecord, beginTs, beginRecord.getUserId(), (end - start) / 1000000d);

        if (IfmxStreamRecordType.COMMIT.equals(endRecord.getType())) {
            IfxCDCCommitTransactionRecord commitRecord = (IfxCDCCommitTransactionRecord) endRecord;
            long commitSeq = commitRecord.getSequenceId();
            long commitTs = commitRecord.getTime();

            Map<String, IfmxReadableType> before = null;
            Map<String, TableId> label2TableId = engine.getTableIdByLabelId();

            for (IfmxStreamRecord streamRecord : transactionRecord.getRecords()) {
                start = System.nanoTime();

                long changeSeq = streamRecord.getSequenceId();

                if (recover && Lsn.of(changeSeq).compareTo(offsetContext.getChangePosition().getChangeLsn()) <= 0) {
                    LOGGER.info("Skipping already processed record {}", changeSeq);
                    continue;
                }

                Optional<TableId> tableId = Optional.ofNullable(streamRecord.getLabel()).map(label2TableId::get);

                Map<String, IfmxReadableType> after;

                updateChangePosition(offsetContext, null, changeSeq, transactionId, null);

                switch (streamRecord.getType()) {
                    case INSERT:

                        after = ((IfmxStreamOperationRecord) streamRecord).getData();

                        handleOperation(partition, offsetContext, Operation.CREATE, null, after, tableId.orElseThrow());

                        end = System.nanoTime();

                        LOGGER.debug("Received {} ElapsedT [{}ms] Data After [{}]",
                                streamRecord, (end - start) / 1000000d, after);
                        break;
                    case BEFORE_UPDATE:

                        before = ((IfmxStreamOperationRecord) streamRecord).getData();

                        end = System.nanoTime();

                        LOGGER.debug("Received {} ElapsedT [{}ms] Data Before [{}]",
                                streamRecord, (end - start) / 1000000d, before);
                        break;
                    case AFTER_UPDATE:

                        after = ((IfmxStreamOperationRecord) streamRecord).getData();

                        handleOperation(partition, offsetContext, Operation.UPDATE, before, after, tableId.orElseThrow());

                        end = System.nanoTime();

                        LOGGER.debug("Received {} ElapsedT [{}ms] Data Before [{}] Data After [{}]",
                                streamRecord, (end - start) / 1000000d, before, after);
                        break;
                    case DELETE:

                        before = ((IfmxStreamOperationRecord) streamRecord).getData();

                        handleOperation(partition, offsetContext, Operation.DELETE, before, null, tableId.orElseThrow());

                        end = System.nanoTime();

                        LOGGER.debug("Received {} ElapsedT [{}ms] Data Before [{}]",
                                streamRecord, (end - start) / 1000000d, before);
                        break;
                    case TRUNCATE:
                        /*
                         * According to IBM documentation the 'User data' field of the CDC_REC_TRUNCATE record header contains the
                         * table identifier, otherwise placed in the IfxCDCRecord 'label' field. For unknown reasons, this is
                         * instead placed in the 'userId' field?
                         */
                        IfxCDCTruncateRecord truncateRecord = (IfxCDCTruncateRecord) streamRecord;
                        tableId = Optional.of(truncateRecord.getUserId()).map(Number::toString).map(label2TableId::get);

                        handleOperation(partition, offsetContext, Operation.TRUNCATE, null, null, tableId.orElseThrow());

                        LOGGER.debug(RECEIVED_GENERIC_RECORD, streamRecord, (end - start) / 1000000d);
                        break;
                    case METADATA:
                    case TIMEOUT:
                    case ERROR:
                        end = System.nanoTime();

                        LOGGER.debug(RECEIVED_GENERIC_RECORD, streamRecord, (end - start) / 1000000d);
                        break;
                    default:
                        end = System.nanoTime();

                        LOGGER.debug(RECEIVED_UNKNOWN_RECORD_TYPE, streamRecord, (end - start) / 1000000d);
                }
            }

            start = System.nanoTime();

            updateChangePosition(offsetContext, commitSeq, commitSeq, transactionId, lowestBeginSeq);
            dispatcher.dispatchTransactionCommittedEvent(partition, offsetContext, Instant.ofEpochSecond(commitTs));

            end = System.nanoTime();

            LOGGER.debug("Received {} Time [{}] UserId [{}] ElapsedT [{}ms]",
                    endRecord, commitTs, beginRecord.getUserId(), (end - start) / 1000000d);

            LOGGER.debug("Handle Transaction Events [{}], ElapsedT [{}ms]",
                    transactionRecord.getRecords().size(), (end - tStart) / 1000000d);
        }
        if (IfmxStreamRecordType.ROLLBACK.equals(endRecord.getType())) {

            if (!recover) {
                updateChangePosition(offsetContext, endSeq, endSeq, transactionId, null);
                offsetContext.getTransactionContext().endTransaction();
            }

            end = System.nanoTime();

            LOGGER.debug(RECEIVED_GENERIC_RECORD, endRecord, (end - start) / 1000000d);
        }
    }

    private void handleMetadata(InformixPartition partition, InformixOffsetContext offsetContext, InformixCdcTransactionEngine engine,
                                IfxCDCMetaDataRecord metaDataRecord)
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

    private void updateChangePosition(InformixOffsetContext offsetContext,
                                      Long commitSeq, Long changeSeq, Integer transactionId, Long beginSeq) {
        offsetContext.setChangePosition(
                TxLogPosition.cloneAndSet(
                        offsetContext.getChangePosition(),
                        Lsn.of(commitSeq),
                        Lsn.of(changeSeq),
                        transactionId,
                        Lsn.of(beginSeq)));
    }

    private void handleOperation(InformixPartition partition, InformixOffsetContext offsetContext, Operation operation,
                                 Map<String, IfmxReadableType> before, Map<String, IfmxReadableType> after, TableId tableId)
            throws InterruptedException {
        offsetContext.event(tableId, clock.currentTime());

        dispatcher.dispatchDataChangeEvent(partition, tableId,
                new InformixChangeRecordEmitter(partition, offsetContext, operation,
                        InformixChangeRecordEmitter.convertIfxData2Array(before, schema.schemaFor(tableId)),
                        InformixChangeRecordEmitter.convertIfxData2Array(after, schema.schemaFor(tableId)),
                        clock, connectorConfig));
    }

}
