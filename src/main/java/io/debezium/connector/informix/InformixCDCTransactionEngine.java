/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import static com.informix.stream.api.IfmxStreamRecordType.AFTER_UPDATE;
import static com.informix.stream.api.IfmxStreamRecordType.BEFORE_UPDATE;
import static com.informix.stream.api.IfmxStreamRecordType.COMMIT;
import static com.informix.stream.api.IfmxStreamRecordType.DELETE;
import static com.informix.stream.api.IfmxStreamRecordType.INSERT;
import static com.informix.stream.api.IfmxStreamRecordType.ROLLBACK;
import static com.informix.stream.api.IfmxStreamRecordType.TRUNCATE;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.informix.stream.api.IfmxStreamRecord;
import com.informix.stream.api.IfmxStreamRecordType;
import com.informix.stream.api.IfxTransactionEngine;
import com.informix.stream.cdc.IfxCDCEngine;
import com.informix.stream.cdc.records.IfxCDCBeginTransactionRecord;
import com.informix.stream.impl.IfxStreamException;

import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.relational.TableId;

/**
 * An implementation of the IfxTransactionEngine interface that takes a wider view of which operation types we are interested in.
 *
 * @author Lars M Johansson
 *
 */
public class InformixCDCTransactionEngine implements IfxTransactionEngine {

    private static final Logger LOGGER = LoggerFactory.getLogger(InformixCDCTransactionEngine.class);
    private static final String PROCESSING_RECORD = "Processing {} record";
    private static final String MISSING_TRANSACTION_START_FOR_RECORD = "Missing transaction start for record: {}";
    protected final Map<Integer, TransactionHolder> transactionMap;
    protected final IfxCDCEngine engine;
    private final ChangeEventSourceContext context;
    protected EnumSet<IfmxStreamRecordType> operationFilters;
    protected EnumSet<IfmxStreamRecordType> transactionFilters;
    protected boolean returnEmptyTransactions;
    private Map<String, TableId> tableIdByLabelId;

    public InformixCDCTransactionEngine(ChangeEventSourceContext context, IfxCDCEngine engine) {
        this.context = context;
        this.operationFilters = EnumSet.of(INSERT, DELETE, BEFORE_UPDATE, AFTER_UPDATE, TRUNCATE);
        this.transactionFilters = EnumSet.of(COMMIT, ROLLBACK);
        this.transactionMap = new ConcurrentHashMap<>();
        this.returnEmptyTransactions = true;
        this.engine = engine;
    }

    @Override
    public IfmxStreamRecord getRecord() throws SQLException, IfxStreamException {
        IfmxStreamRecord streamRecord;
        while (context.isRunning() && (streamRecord = engine.getRecord()) != null) {

            TransactionHolder holder = transactionMap.get(streamRecord.getTransactionId());
            if (holder != null) {
                LOGGER.debug("Processing [{}] record for transaction id: {}", streamRecord.getType(), streamRecord.getTransactionId());
            }
            switch (streamRecord.getType()) {
                case BEGIN:
                    holder = new TransactionHolder();
                    holder.beginRecord = (IfxCDCBeginTransactionRecord) streamRecord;
                    transactionMap.put(streamRecord.getTransactionId(), holder);
                    LOGGER.debug("Watching transaction id: {}", streamRecord.getTransactionId());
                    break;
                case INSERT:
                case DELETE:
                case BEFORE_UPDATE:
                case AFTER_UPDATE:
                case TRUNCATE:
                    if (holder == null) {
                        LOGGER.warn(MISSING_TRANSACTION_START_FOR_RECORD, streamRecord);
                        break;
                    }
                    LOGGER.debug(PROCESSING_RECORD, streamRecord.getType());
                    holder.records.add(streamRecord);
                    break;
                case DISCARD:
                    if (holder == null) {
                        LOGGER.warn(MISSING_TRANSACTION_START_FOR_RECORD, streamRecord);
                        break;
                    }
                    LOGGER.debug(PROCESSING_RECORD, streamRecord.getType());
                    long sequenceId = streamRecord.getSequenceId();

                    if (holder.records.removeIf(r -> r.getSequenceId() >= sequenceId)) {
                        LOGGER.debug("Discarding records with sequence >={}", sequenceId);
                    }
                    break;
                case COMMIT:
                case ROLLBACK:
                    if (holder == null) {
                        LOGGER.warn(MISSING_TRANSACTION_START_FOR_RECORD, streamRecord);
                        break;
                    }
                    LOGGER.debug(PROCESSING_RECORD, streamRecord.getType());
                    holder.closingRecord = streamRecord;
                    break;
                case METADATA:
                case TIMEOUT:
                case ERROR:
                    if (holder == null) {
                        return streamRecord;
                    }
                    LOGGER.debug(PROCESSING_RECORD, streamRecord.getType());
                    holder.records.add(streamRecord);
                    break;
                default:
                    LOGGER.warn("Unknown operation for record: {}", streamRecord);
            }
            if (holder != null && holder.closingRecord != null) {
                transactionMap.remove(streamRecord.getTransactionId());
                if (!holder.records.isEmpty() || returnEmptyTransactions) {
                    return new InformixStreamTransactionRecord(holder.beginRecord, holder.closingRecord, holder.records);
                }
            }
        }

        return null;
    }

    @Override
    public InformixStreamTransactionRecord getTransaction() throws SQLException, IfxStreamException {
        IfmxStreamRecord streamRecord;
        while ((streamRecord = getRecord()) != null && !(streamRecord instanceof InformixStreamTransactionRecord)) {
            LOGGER.debug("Discard non-transaction record: {}", streamRecord);
        }
        return (InformixStreamTransactionRecord) streamRecord;
    }

    @Override
    public InformixCDCTransactionEngine setOperationFilters(IfmxStreamRecordType... recordTypes) {
        operationFilters = EnumSet.copyOf(Set.of(recordTypes));
        return this;
    }

    @Override
    public InformixCDCTransactionEngine setTransactionFilters(IfmxStreamRecordType... recordTypes) {
        transactionFilters = EnumSet.copyOf(Set.of(recordTypes));
        return this;
    }

    @Override
    public InformixCDCTransactionEngine returnEmptyTransactions(boolean returnEmptyTransactions) {
        this.returnEmptyTransactions = returnEmptyTransactions;
        return this;
    }

    @Override
    public void init() throws SQLException, IfxStreamException {
        engine.init();

        /*
         * Build Map of Label_id to TableId.
         */
        tableIdByLabelId = engine.getBuilder().getWatchedTables().stream()
                .collect(Collectors.toUnmodifiableMap(
                        o -> String.valueOf(o.getLabel()),
                        t -> new TableId(t.getDatabaseName(), t.getNamespace(), t.getTableName())));
    }

    @Override
    public void close() throws IfxStreamException {
        engine.close();
    }

    public OptionalLong getLowestBeginSequence() {
        return transactionMap.values().stream().mapToLong(t -> t.beginRecord.getSequenceId()).min();
    }

    public Map<String, TableId> getTableIdByLabelId() {
        return tableIdByLabelId;
    }

    protected static class TransactionHolder {
        final List<IfmxStreamRecord> records = new ArrayList<>();
        IfxCDCBeginTransactionRecord beginRecord;
        IfmxStreamRecord closingRecord;
    }
}
