package laoflch.debezium.connector.informix;

import com.informix.jdbc.IfmxReadableType;
import com.informix.stream.api.IfmxStreamRecord;
import com.informix.stream.cdc.records.IfxCDCBeginTransactionRecord;
import com.informix.stream.cdc.records.IfxCDCCommitTransactionRecord;
import com.informix.stream.cdc.records.IfxCDCMetaDataRecord;
import com.informix.stream.cdc.records.IfxCDCOperationRecord;
import com.informix.stream.cdc.records.IfxCDCRollbackTransactionRecord;
import com.informix.stream.cdc.records.IfxCDCTimeoutRecord;
import com.informix.stream.impl.IfxStreamException;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.time.Timestamp;
import io.debezium.util.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;


public class InformixStreamingChangeEventSource implements StreamingChangeEventSource {

    private static Logger LOGGER = LoggerFactory.getLogger(InformixStreamingChangeEventSource.class);

    private final InformixConnectorConfig config;
    private final InformixOffsetContext offsetContext;
    private final InformixConnection dataConnection;
    private final EventDispatcher<TableId> dispatcher;
    private final ErrorHandler errorHandler;
    private final Clock clock;
    private final InformixDatabaseSchema schema;

    public InformixStreamingChangeEventSource(InformixConnectorConfig connectorConfig,
                                              InformixOffsetContext offsetContext,
                                              InformixConnection dataConnection,
                                              //metadataConnection: InformixConnection,
                                              EventDispatcher<TableId> dispatcher,
                                              ErrorHandler errorHandler,
                                              Clock clock,
                                              InformixDatabaseSchema schema) {
        this.config = connectorConfig;
        this.offsetContext = offsetContext;
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
    public void execute(ChangeEventSourceContext context) throws InterruptedException {
        InformixCDCEngine cdcEngine = dataConnection.getCdcEngine();
        InformixTransactionCache transCache = offsetContext.getInformixTransactionCache();

        /*
         * Initialize CDC Engine before main loop;
         */
        TxLogPosition lastPosition = offsetContext.getChangePosition();
        Long fromLsn = lastPosition.getCommitLsn();
        cdcEngine.setStartLsn(fromLsn);
        cdcEngine.init(schema);

        try {
            /*
             * Main Handler Loop
             */
            while (context.isRunning()) {
                cdcEngine.stream((IfmxStreamRecord record) -> {
                    switch (record.getType()) {
                        case TIMEOUT:
                            handleTimeout(cdcEngine, (IfxCDCTimeoutRecord) record);
                            break;
                        case BEFORE_UPDATE:
                            handleBeforeUpdate(cdcEngine, (IfxCDCOperationRecord) record, transCache);
                            break;
                        case AFTER_UPDATE:
                            handleAfterUpdate(cdcEngine, (IfxCDCOperationRecord) record, transCache);
                            break;
                        case BEGIN:
                            handleBegin(cdcEngine, (IfxCDCBeginTransactionRecord) record, transCache);
                            break;
                        case INSERT:
                            handleInsert(cdcEngine, (IfxCDCOperationRecord) record, transCache);
                            break;
                        case COMMIT:
                            handleCommit(cdcEngine, (IfxCDCCommitTransactionRecord) record, transCache);
                            break;
                        case ROLLBACK:
                            handleRollback(cdcEngine, (IfxCDCRollbackTransactionRecord) record, transCache);
                            break;
                        case METADATA:
                            handleMetadata(cdcEngine, (IfxCDCMetaDataRecord) record);
                            break;
                        case DELETE:
                            break;
                        default:
                            LOGGER.info("Handle unknown record-type = {}", record.getType());
                    }

                    return false;
                });
            }
        } catch (SQLException e) {
            LOGGER.error("Caught SQLException", e);
            errorHandler.setProducerThrowable(e);
        } catch (IfxStreamException e) {
            LOGGER.error("Caught IfxStreamException", e);
            errorHandler.setProducerThrowable(e);
        }  catch (Exception e) {
            LOGGER.error("Caught Unknown Exception", e);
            errorHandler.setProducerThrowable(e);
        } finally {
            cdcEngine.close();
        }
    }

    public void handleTimeout(InformixCDCEngine cdcEngine, IfxCDCTimeoutRecord record) {
        offsetContext.setChangePosition(
                TxLogPosition.cloneAndSet(
                        offsetContext.getChangePosition(),
                        TxLogPosition.LSN_NULL,
                        record.getSequenceId(),
                        TxLogPosition.LSN_NULL,
                        TxLogPosition.LSN_NULL
                )
        );
    }

    public void handleMetadata(InformixCDCEngine cdcEngine, IfxCDCMetaDataRecord record) {

        LOGGER.info("Received A Metadata: type={}, label={}, seqId={}",
                record.getType(), record.getLabel(), record.getSequenceId());

        /*
        IfxCDCEngine engine = cdcEngine.getCdcEngine();
        List<IfxCDCEngine.IfmxWatchedTable> watchedTables = engine.getBuilder().getWatchedTables();
        List<IfxColumnInfo> cols = record.getColumns();
        for (IfxColumnInfo cinfo : cols) {
            LOGGER.info("ColumnInfo: colName={}, {}", cinfo.getColumnName(), cinfo.toString());
        }

        for (IfxCDCEngine.IfmxWatchedTable tbl : watchedTables) {
            LOGGER.info("Engine Watched Table: label={}, tabName={}", tbl.getLabel(), tbl.getTableName());
        }
        */
    }

    public void handleBeforeUpdate(InformixCDCEngine cdcEngine, IfxCDCOperationRecord record, InformixTransactionCache transactionCache) throws IfxStreamException {

        Map<String, IfmxReadableType> data = record.getData();
        Long transId = (long) record.getTransactionId();

        offsetContext.setChangePosition(
                TxLogPosition.cloneAndSet(
                        offsetContext.getChangePosition(),
                        TxLogPosition.LSN_NULL,
                        record.getSequenceId(),
                        transId,
                        TxLogPosition.LSN_NULL)
        );

        transactionCache.beforeUpdate(transId, data);
    }

    public void handleAfterUpdate(InformixCDCEngine cdcEngine, IfxCDCOperationRecord record, InformixTransactionCache transactionCache) throws IfxStreamException, SQLException {
        Long transId = (long) record.getTransactionId();

        Map<String, IfmxReadableType> newData = record.getData();
        Map<String, IfmxReadableType> oldData = transactionCache.afterUpdate(transId).get();

        offsetContext.setChangePosition(
                TxLogPosition.cloneAndSet(
                        offsetContext.getChangePosition(),
                        TxLogPosition.LSN_NULL,
                        record.getSequenceId(),
                        transId,
                        TxLogPosition.LSN_NULL)
        );

        Map<Integer, TableId> label2TableId = cdcEngine.convertLabel2TableId();
        TableId tid = label2TableId.get(Integer.parseInt(record.getLabel()));

        handleEvent(tid, offsetContext, InformixChangeRecordEmitter.OP_UPDATE, oldData, newData, clock, null);
    }

    public void handleBegin(InformixCDCEngine cdcEngine, IfxCDCBeginTransactionRecord record, InformixTransactionCache transactionCache) throws IfxStreamException {
        long _start = System.nanoTime();

        Long transId = (long) record.getTransactionId();
        Long beginTs = record.getTime();

        Optional<InformixTransactionCache.TransactionCacheBuffer> transactionCacheBuffer = transactionCache.beginTxn(transId, beginTs);
        if (!transactionCacheBuffer.isPresent()) {
            offsetContext.setChangePosition(
                    TxLogPosition.cloneAndSet(
                            offsetContext.getChangePosition(),
                            TxLogPosition.LSN_NULL,
                            record.getSequenceId(),
                            transId,
                            record.getSequenceId())
            );

            offsetContext.getTransactionContext().beginTransaction(String.valueOf(record.getTransactionId()));
        }

        long _end = System.nanoTime();

        LOGGER.info("Received BEGIN :: transId={} seqId={} time={} userId={} elapsedTs={}ms",
                record.getTransactionId(), record.getSequenceId(),
                record.getTime(), record.getUserId(),
                (_end - _start) / 1000000d);
    }

    public void handleCommit(InformixCDCEngine cdcEngine, IfxCDCCommitTransactionRecord record, InformixTransactionCache transactionCache) throws InterruptedException, IfxStreamException {
        long _start = System.nanoTime();
        Long transId = (long) record.getTransactionId();
        Long endTime = record.getTime();

        Optional<InformixTransactionCache.TransactionCacheBuffer> transactionCacheBuffer = transactionCache.commitTxn(transId, endTime);
        if (transactionCacheBuffer.isPresent()) {
            offsetContext.setChangePosition(
                    TxLogPosition.cloneAndSet(
                            offsetContext.getChangePosition(),
                            record.getSequenceId(),
                            record.getSequenceId(),
                            transId,
                            TxLogPosition.LSN_NULL)
            );

            // Originated from handleCommitEvent()
            for (InformixTransactionCache.TransactionCacheRecord r : transactionCacheBuffer.get().getTransactionCacheRecords()) {
                dispatcher.dispatchDataChangeEvent(r.getTableId(), r.getInformixChangeRecordEmitter());
            }
            LOGGER.info("Handle Commit {} Events, transElapsedTime={}",
                    transactionCacheBuffer.get().size(), transactionCacheBuffer.get().getElapsed());
        }
        offsetContext.getTransactionContext().endTransaction();

        long _end = System.nanoTime();
        LOGGER.info("Received COMMIT :: transId={} seqId={} time={} elapsedTime={} ms",
                record.getTransactionId(), record.getSequenceId(),
                record.getTime(),
                (_end - _start) / 1000000d);
    }

    public void handleInsert(InformixCDCEngine cdcEngine, IfxCDCOperationRecord record, InformixTransactionCache transactionCache) throws IfxStreamException, SQLException {
        long _start = System.nanoTime();
        Long transId = (long) record.getTransactionId();

        Map<String, IfmxReadableType> data = record.getData();
        offsetContext.setChangePosition(
                TxLogPosition.cloneAndSet(
                        offsetContext.getChangePosition(),
                        TxLogPosition.LSN_NULL,
                        record.getSequenceId(),
                        transId,
                        TxLogPosition.LSN_NULL));

        Map<Integer, TableId> label2TableId = cdcEngine.convertLabel2TableId();
        TableId tid = label2TableId.get(Integer.parseInt(record.getLabel()));
        handleEvent(tid, offsetContext, InformixChangeRecordEmitter.OP_INSERT, null, data, clock, null);

        long _end = System.nanoTime();
        LOGGER.info("Received INSERT :: transId={} seqId={} elapsedTime={} ms",
                record.getTransactionId(), record.getSequenceId(),
                (_end - _start) / 1000000d);
    }

    public void handleRollback(InformixCDCEngine cdcEngine, IfxCDCRollbackTransactionRecord record, InformixTransactionCache transactionCache) throws IfxStreamException {
        long _start = System.nanoTime();
        Long transId = (long) record.getTransactionId();

        Optional<InformixTransactionCache.TransactionCacheBuffer> transactionCacheBuffer = transactionCache.rollbackTxn(transId);
        if (transactionCacheBuffer.isPresent()) {
            offsetContext.setChangePosition(
                    TxLogPosition.cloneAndSet(
                            offsetContext.getChangePosition(),
                            TxLogPosition.LSN_NULL,
                            record.getSequenceId(),
                            transId,
                            TxLogPosition.LSN_NULL));

            /*
             * when rollbackTxn do nothing handle but log the discarded records
             */
            LOGGER.info("Rollback Txn: {}", record.getTransactionId());
        }
        offsetContext.getTransactionContext().endTransaction();

        long _end = System.nanoTime();
        LOGGER.info("Received ROLLBACK :: transId={} seqId={} elapsedTime={} ms",
                record.getTransactionId(), record.getSequenceId(),
                (_end - _start) / 1000000d);
    }

    public void handleEvent(TableId tableId,
                            InformixOffsetContext offsetContext,
                            //txn:Long,
                            Integer operation,
                            Map<String, IfmxReadableType> data,
                            Map<String, IfmxReadableType> dataNext,
                            Clock clock,
                            Timestamp timestamp) throws SQLException {

        offsetContext.event(tableId, clock.currentTime());

        InformixChangeRecordEmitter informixChangeRecordEmitter = new InformixChangeRecordEmitter(offsetContext, operation,
                InformixChangeRecordEmitter.convertIfxData2Array(data),
                InformixChangeRecordEmitter.convertIfxData2Array(dataNext), clock);

        offsetContext.getInformixTransactionCache().addEvent2Tx(tableId, informixChangeRecordEmitter, offsetContext.getChangePosition().getTxId());
    }
}
