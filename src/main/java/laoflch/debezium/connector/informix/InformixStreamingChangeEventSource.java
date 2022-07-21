/*
 * Copyright Debezium-Informix-Connector Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package laoflch.debezium.connector.informix;

import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.informix.jdbc.IfmxReadableType;
import com.informix.stream.api.IfmxStreamRecord;
import com.informix.stream.cdc.records.IfxCDCBeginTransactionRecord;
import com.informix.stream.cdc.records.IfxCDCCommitTransactionRecord;
import com.informix.stream.cdc.records.IfxCDCMetaDataRecord;
import com.informix.stream.cdc.records.IfxCDCOperationRecord;
import com.informix.stream.cdc.records.IfxCDCRollbackTransactionRecord;
import com.informix.stream.cdc.records.IfxCDCTimeoutRecord;
import com.informix.stream.cdc.records.IfxCDCTruncateRecord;
import com.informix.stream.impl.IfxStreamException;

import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.util.Clock;

public class InformixStreamingChangeEventSource implements StreamingChangeEventSource<InformixOffsetContext> {

    private static Logger LOGGER = LoggerFactory.getLogger(InformixStreamingChangeEventSource.class);

    private final InformixConnectorConfig config;
    private final InformixConnection dataConnection;
    private final EventDispatcher<TableId> dispatcher;
    private final ErrorHandler errorHandler;
    private final Clock clock;
    private final InformixDatabaseSchema schema;

    public InformixStreamingChangeEventSource(InformixConnectorConfig connectorConfig,
                                              InformixConnection dataConnection,
                                              EventDispatcher<TableId> dispatcher,
                                              ErrorHandler errorHandler,
                                              Clock clock,
                                              InformixDatabaseSchema schema) {
        this.config = connectorConfig;
        this.dataConnection = dataConnection;
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.clock = clock;
        this.schema = schema;
    }

    /**
     * Executes this source. Implementations should regularly check via the given context if they should stop. If that's
     * the case, they should abort their processing and perform any clean-up needed, such as rolling back pending
     * transactions, releasing locks etc.
     *
     * @param context contextual information for this source's execution
     * @return an indicator to the position at which the snapshot was taken
     * @throws InterruptedException in case the snapshot was aborted before completion
     */
    @Override
    public void execute(ChangeEventSourceContext context, InformixOffsetContext offsetContext) throws InterruptedException {
        InformixCDCEngine cdcEngine = dataConnection.getCdcEngine();
        InformixTransactionCache transCache = offsetContext.getInformixTransactionCache();

        /*
         * Initialize CDC Engine before main loop;
         */
        TxLogPosition lastPosition = offsetContext.getChangePosition();
        Long fromLsn = lastPosition.getCommitLsn();
        cdcEngine.setStartLsn(fromLsn);
        cdcEngine.init(schema);

        /*
         * Recover Stage. In this stage, we replay event from 'commitLsn' to 'changeLsn', and rebuild the transactionCache.
         */
        while (context.isRunning()) {
            if (lastPosition.getChangeLsn() <= lastPosition.getCommitLsn()) {
                LOGGER.info("Recover skipped, since changeLsn='{}' >= commitLsn='{}'",
                        lastPosition.getChangeLsn(), lastPosition.getCommitLsn());
                break;
            }

            try {
                IfmxStreamRecord record = cdcEngine.getCdcEngine().getRecord();
                if (record.getSequenceId() >= lastPosition.getChangeLsn()) {
                    LOGGER.info("Recover finished: from {} to {}, now Current seqId={}",
                            lastPosition.getCommitLsn(), lastPosition.getChangeLsn(), record.getSequenceId());
                    break;
                }
                switch (record.getType()) {
                    case TIMEOUT:
                        handleTimeout(offsetContext, (IfxCDCTimeoutRecord) record);
                        break;
                    case BEFORE_UPDATE:
                        handleBeforeUpdate(offsetContext, (IfxCDCOperationRecord) record, transCache, true);
                        break;
                    case AFTER_UPDATE:
                        handleAfterUpdate(cdcEngine, offsetContext, (IfxCDCOperationRecord) record, transCache, true);
                        break;
                    case BEGIN:
                        handleBegin(offsetContext, (IfxCDCBeginTransactionRecord) record, transCache, true);
                        break;
                    case INSERT:
                        handleInsert(cdcEngine, offsetContext, (IfxCDCOperationRecord) record, transCache, true);
                        break;
                    case COMMIT:
                        handleCommit(offsetContext, (IfxCDCCommitTransactionRecord) record, transCache, true);
                        break;
                    case ROLLBACK:
                        handleRollback(offsetContext, (IfxCDCRollbackTransactionRecord) record, transCache, false);
                        break;
                    case METADATA:
                        handleMetadata(cdcEngine, (IfxCDCMetaDataRecord) record);
                        break;
                    case TRUNCATE:
                        handleTruncate(cdcEngine, offsetContext, (IfxCDCTruncateRecord) record, transCache, true);
                        break;
                    case DELETE:
                        handleDelete(cdcEngine, offsetContext, (IfxCDCOperationRecord) record, transCache, true);
                        break;
                    default:
                        LOGGER.info("Handle unknown record-type = {}", record.getType());
                }
            }
            catch (SQLException e) {
                LOGGER.error("Caught SQLException", e);
                errorHandler.setProducerThrowable(e);
            }
            catch (IfxStreamException e) {
                LOGGER.error("Caught IfxStreamException", e);
                errorHandler.setProducerThrowable(e);
            }
        }

        /*
         * Main Handler Loop
         */
        try {
            while (context.isRunning()) {
                cdcEngine.stream((IfmxStreamRecord record) -> {
                    switch (record.getType()) {
                        case TIMEOUT:
                            handleTimeout(offsetContext, (IfxCDCTimeoutRecord) record);
                            break;
                        case BEFORE_UPDATE:
                            handleBeforeUpdate(offsetContext, (IfxCDCOperationRecord) record, transCache, false);
                            break;
                        case AFTER_UPDATE:
                            handleAfterUpdate(cdcEngine, offsetContext, (IfxCDCOperationRecord) record, transCache, false);
                            break;
                        case BEGIN:
                            handleBegin(offsetContext, (IfxCDCBeginTransactionRecord) record, transCache, false);
                            break;
                        case INSERT:
                            handleInsert(cdcEngine, offsetContext, (IfxCDCOperationRecord) record, transCache, false);
                            break;
                        case COMMIT:
                            handleCommit(offsetContext, (IfxCDCCommitTransactionRecord) record, transCache, false);
                            break;
                        case ROLLBACK:
                            handleRollback(offsetContext, (IfxCDCRollbackTransactionRecord) record, transCache, false);
                            break;
                        case METADATA:
                            handleMetadata(cdcEngine, (IfxCDCMetaDataRecord) record);
                            break;
                        case TRUNCATE:
                            handleTruncate(cdcEngine, offsetContext, (IfxCDCTruncateRecord) record, transCache, false);
                            break;
                        case DELETE:
                            handleDelete(cdcEngine, offsetContext, (IfxCDCOperationRecord) record, transCache, false);
                            break;
                        default:
                            LOGGER.info("Handle unknown record-type = {}", record.getType());
                    }

                    return false;
                });
            }
        }
        catch (SQLException e) {
            LOGGER.error("Caught SQLException", e);
            errorHandler.setProducerThrowable(e);
        }
        catch (IfxStreamException e) {
            LOGGER.error("Caught IfxStreamException", e);
            errorHandler.setProducerThrowable(e);
        }
        catch (Exception e) {
            LOGGER.error("Caught Unknown Exception", e);
            errorHandler.setProducerThrowable(e);
        }
        finally {
            cdcEngine.close();
        }
    }

    public void handleTimeout(InformixOffsetContext offsetContext, IfxCDCTimeoutRecord record) {
        offsetContext.setChangePosition(
                TxLogPosition.cloneAndSet(
                        offsetContext.getChangePosition(),
                        TxLogPosition.LSN_NULL,
                        record.getSequenceId(),
                        TxLogPosition.LSN_NULL,
                        TxLogPosition.LSN_NULL));
    }

    public void handleMetadata(InformixCDCEngine cdcEngine, IfxCDCMetaDataRecord record) {

        LOGGER.info("Received A Metadata: type={}, label={}, seqId={}",
                record.getType(), record.getLabel(), record.getSequenceId());

        /*
         * IfxCDCEngine engine = cdcEngine.getCdcEngine();
         * List<IfxCDCEngine.IfmxWatchedTable> watchedTables = engine.getBuilder().getWatchedTables();
         * List<IfxColumnInfo> cols = record.getColumns();
         * for (IfxColumnInfo cinfo : cols) {
         * LOGGER.info("ColumnInfo: colName={}, {}", cinfo.getColumnName(), cinfo.toString());
         * }
         * 
         * for (IfxCDCEngine.IfmxWatchedTable tbl : watchedTables) {
         * LOGGER.info("Engine Watched Table: label={}, tabName={}", tbl.getLabel(), tbl.getTableName());
         * }
         */
    }

    public void handleBeforeUpdate(InformixOffsetContext offsetContext, IfxCDCOperationRecord record, InformixTransactionCache transactionCache, boolean recover)
            throws IfxStreamException {

        Map<String, IfmxReadableType> data = record.getData();
        Long transId = (long) record.getTransactionId();
        transactionCache.beforeUpdate(transId, data);

        if (!recover) {
            offsetContext.setChangePosition(
                    TxLogPosition.cloneAndSet(
                            offsetContext.getChangePosition(),
                            TxLogPosition.LSN_NULL,
                            record.getSequenceId(),
                            transId,
                            TxLogPosition.LSN_NULL));
        }

    }

    public void handleAfterUpdate(InformixCDCEngine cdcEngine, InformixOffsetContext offsetContext, IfxCDCOperationRecord record,
                                  InformixTransactionCache transactionCache, boolean recover)
            throws IfxStreamException, SQLException {
        Long transId = (long) record.getTransactionId();

        Map<String, IfmxReadableType> newData = record.getData();
        Map<String, IfmxReadableType> oldData = transactionCache.afterUpdate(transId).get();

        Map<Integer, TableId> label2TableId = cdcEngine.convertLabel2TableId();
        TableId tid = label2TableId.get(Integer.parseInt(record.getLabel()));
        handleEvent(tid, offsetContext, transId, InformixChangeRecordEmitter.OP_UPDATE, oldData, newData, clock);

        if (!recover) {
            offsetContext.setChangePosition(
                    TxLogPosition.cloneAndSet(
                            offsetContext.getChangePosition(),
                            TxLogPosition.LSN_NULL,
                            record.getSequenceId(),
                            transId,
                            TxLogPosition.LSN_NULL));
        }
    }

    public void handleBegin(InformixOffsetContext offsetContext, IfxCDCBeginTransactionRecord record, InformixTransactionCache transactionCache, boolean recover)
            throws IfxStreamException {
        long _start = System.nanoTime();

        Long transId = (long) record.getTransactionId();
        Long beginTs = record.getTime();
        Long seqId = record.getSequenceId();

        Optional<InformixTransactionCache.TransactionCacheBuffer> transactionCacheBuffer = transactionCache.beginTxn(transId, beginTs, seqId);
        if (!recover) {
            Optional<InformixTransactionCache.TransactionCacheBuffer> minTransactionCache = transactionCache.getMinTransactionCache();
            Long minSeqId = minTransactionCache.isPresent() ? minTransactionCache.get().getBeginSeqId() : record.getSequenceId();

            if (!transactionCacheBuffer.isPresent()) {
                offsetContext.setChangePosition(
                        TxLogPosition.cloneAndSet(
                                offsetContext.getChangePosition(),
                                minSeqId,
                                record.getSequenceId(),
                                transId,
                                record.getSequenceId()));

                offsetContext.getTransactionContext().beginTransaction(String.valueOf(record.getTransactionId()));
            }
        }

        long _end = System.nanoTime();

        LOGGER.info("Received BEGIN :: transId={} seqId={} time={} userId={} elapsedTs={}ms",
                record.getTransactionId(), record.getSequenceId(),
                record.getTime(), record.getUserId(),
                (_end - _start) / 1000000d);
    }

    public void handleCommit(InformixOffsetContext offsetContext, IfxCDCCommitTransactionRecord record, InformixTransactionCache transactionCache, boolean recover)
            throws InterruptedException, IfxStreamException {
        long _start = System.nanoTime();
        Long transId = (long) record.getTransactionId();
        Long endTime = record.getTime();

        Optional<InformixTransactionCache.TransactionCacheBuffer> transactionCacheBuffer = transactionCache.commitTxn(transId, endTime);
        if (!recover) {
            Optional<InformixTransactionCache.TransactionCacheBuffer> minTransactionCache = transactionCache.getMinTransactionCache();
            Long minSeqId = minTransactionCache.isPresent() ? minTransactionCache.get().getBeginSeqId() : record.getSequenceId();

            if (transactionCacheBuffer.isPresent()) {
                offsetContext.setChangePosition(
                        TxLogPosition.cloneAndSet(
                                offsetContext.getChangePosition(),
                                minSeqId,
                                record.getSequenceId(),
                                transId,
                                TxLogPosition.LSN_NULL));

                for (InformixTransactionCache.TransactionCacheRecord r : transactionCacheBuffer.get().getTransactionCacheRecords()) {
                    dispatcher.dispatchDataChangeEvent(r.getTableId(), r.getInformixChangeRecordEmitter());
                }
                LOGGER.info("Handle Commit {} Events, transElapsedTime={}",
                        transactionCacheBuffer.get().size(), transactionCacheBuffer.get().getElapsed());
            }
            offsetContext.getTransactionContext().endTransaction();
        }

        long _end = System.nanoTime();
        LOGGER.info("Received COMMIT :: transId={} seqId={} time={} elapsedTime={} ms",
                record.getTransactionId(), record.getSequenceId(),
                record.getTime(),
                (_end - _start) / 1000000d);
    }

    public void handleInsert(InformixCDCEngine cdcEngine, InformixOffsetContext offsetContext, IfxCDCOperationRecord record, InformixTransactionCache transactionCache,
                             boolean recover)
            throws IfxStreamException, SQLException {
        long _start = System.nanoTime();
        Long transId = (long) record.getTransactionId();

        Optional<InformixTransactionCache.TransactionCacheBuffer> minTransactionCache = transactionCache.getMinTransactionCache();
        Long minSeqId = minTransactionCache.isPresent() ? minTransactionCache.get().getBeginSeqId() : record.getSequenceId();

        Map<String, IfmxReadableType> data = record.getData();
        Map<Integer, TableId> label2TableId = cdcEngine.convertLabel2TableId();
        TableId tid = label2TableId.get(Integer.parseInt(record.getLabel()));
        handleEvent(tid, offsetContext, transId, InformixChangeRecordEmitter.OP_INSERT, null, data, clock);

        if (!recover) {
            offsetContext.setChangePosition(
                    TxLogPosition.cloneAndSet(
                            offsetContext.getChangePosition(),
                            minSeqId,
                            record.getSequenceId(),
                            transId,
                            TxLogPosition.LSN_NULL));
        }

        long _end = System.nanoTime();
        LOGGER.info("Received INSERT :: transId={} seqId={} elapsedTime={} ms",
                record.getTransactionId(), record.getSequenceId(),
                (_end - _start) / 1000000d);
    }

    public void handleRollback(InformixOffsetContext offsetContext, IfxCDCRollbackTransactionRecord record, InformixTransactionCache transactionCache, boolean recover)
            throws IfxStreamException {
        long _start = System.nanoTime();
        Long transId = (long) record.getTransactionId();

        Optional<InformixTransactionCache.TransactionCacheBuffer> transactionCacheBuffer = transactionCache.rollbackTxn(transId);
        if (!recover) {
            Optional<InformixTransactionCache.TransactionCacheBuffer> minTransactionCache = transactionCache.getMinTransactionCache();
            Long minSeqId = minTransactionCache.isPresent() ? minTransactionCache.get().getBeginSeqId() : record.getSequenceId();

            if (minTransactionCache.isPresent()) {
                minSeqId = minTransactionCache.get().getBeginSeqId();
            }
            if (transactionCacheBuffer.isPresent()) {
                offsetContext.setChangePosition(
                        TxLogPosition.cloneAndSet(
                                offsetContext.getChangePosition(),
                                minSeqId,
                                record.getSequenceId(),
                                transId,
                                TxLogPosition.LSN_NULL));

                LOGGER.info("Rollback Txn: {}", record.getTransactionId());
            }
            offsetContext.getTransactionContext().endTransaction();
        }

        long _end = System.nanoTime();
        LOGGER.info("Received ROLLBACK :: transId={} seqId={} elapsedTime={} ms",
                record.getTransactionId(), record.getSequenceId(),
                (_end - _start) / 1000000d);
    }

    public void handleTruncate(InformixCDCEngine cdcEngine, InformixOffsetContext offsetContext, IfxCDCTruncateRecord record, InformixTransactionCache transactionCache,
                               boolean recover)
            throws IfxStreamException, SQLException {
        long _start = System.nanoTime();
        Long transId = (long) record.getTransactionId();

        Optional<InformixTransactionCache.TransactionCacheBuffer> minTransactionCache = transactionCache.getMinTransactionCache();
        Long minSeqId = minTransactionCache.isPresent() ? minTransactionCache.get().getBeginSeqId() : record.getSequenceId();

        Map<Integer, TableId> label2TableId = cdcEngine.convertLabel2TableId();
        TableId tid = label2TableId.get(record.getUserId());
        handleEvent(tid, offsetContext, transId, InformixChangeRecordEmitter.OP_TRUNCATE, null, null, clock);

        if (!recover) {
            offsetContext.setChangePosition(
                    TxLogPosition.cloneAndSet(
                            offsetContext.getChangePosition(),
                            minSeqId,
                            record.getSequenceId(),
                            transId,
                            TxLogPosition.LSN_NULL));
        }

        long _end = System.nanoTime();
        LOGGER.info("Received TRUNCATE :: transId={} seqId={} elapsedTime={} ms",
                record.getTransactionId(), record.getSequenceId(),
                (_end - _start) / 1000000d);
    }

    public void handleDelete(InformixCDCEngine cdcEngine, InformixOffsetContext offsetContext, IfxCDCOperationRecord record, InformixTransactionCache transactionCache,
                             boolean recover)
            throws IfxStreamException, SQLException {
        long _start = System.nanoTime();
        Long transId = (long) record.getTransactionId();

        Optional<InformixTransactionCache.TransactionCacheBuffer> minTransactionCache = transactionCache.getMinTransactionCache();
        Long minSeqId = minTransactionCache.isPresent() ? minTransactionCache.get().getBeginSeqId() : record.getSequenceId();

        Map<String, IfmxReadableType> data = record.getData();
        Map<Integer, TableId> label2TableId = cdcEngine.convertLabel2TableId();
        TableId tid = label2TableId.get(Integer.parseInt(record.getLabel()));
        handleEvent(tid, offsetContext, transId, InformixChangeRecordEmitter.OP_DELETE, data, null, clock);

        if (!recover) {
            offsetContext.setChangePosition(
                    TxLogPosition.cloneAndSet(
                            offsetContext.getChangePosition(),
                            minSeqId,
                            record.getSequenceId(),
                            transId,
                            TxLogPosition.LSN_NULL));
        }

        long _end = System.nanoTime();
        LOGGER.info("Received DELETE :: transId={} seqId={} elapsedTime={} ms",
                record.getTransactionId(), record.getSequenceId(),
                (_end - _start) / 1000000d);
    }

    public void handleEvent(TableId tableId,
                            InformixOffsetContext offsetContext,
                            Long txId,
                            Integer operation,
                            Map<String, IfmxReadableType> data,
                            Map<String, IfmxReadableType> dataNext,
                            Clock clock)
            throws SQLException {

        offsetContext.event(tableId, clock.currentTime());

        TableSchema tableSchema = schema.schemaFor(tableId);
        InformixChangeRecordEmitter informixChangeRecordEmitter = new InformixChangeRecordEmitter(offsetContext, operation,
                InformixChangeRecordEmitter.convertIfxData2Array(data, tableSchema),
                InformixChangeRecordEmitter.convertIfxData2Array(dataNext, tableSchema), clock);

        offsetContext.getInformixTransactionCache().addEvent2Tx(tableId, informixChangeRecordEmitter, txId);
    }
}
