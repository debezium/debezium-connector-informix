/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import static com.informix.jdbc.stream.api.StreamRecordType.AFTER_UPDATE;
import static com.informix.jdbc.stream.api.StreamRecordType.BEFORE_UPDATE;
import static com.informix.jdbc.stream.api.StreamRecordType.COMMIT;
import static com.informix.jdbc.stream.api.StreamRecordType.DELETE;
import static com.informix.jdbc.stream.api.StreamRecordType.INSERT;
import static com.informix.jdbc.stream.api.StreamRecordType.ROLLBACK;
import static com.informix.jdbc.stream.api.StreamRecordType.TRUNCATE;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.informix.jdbc.IfmxTableDescriptor;
import com.informix.jdbc.stream.api.StreamRecord;
import com.informix.jdbc.stream.api.StreamRecordType;
import com.informix.jdbc.stream.api.TransactionEngine;
import com.informix.jdbc.stream.cdc.CDCEngine;
import com.informix.jdbc.stream.cdc.records.CDCBeginTransactionRecord;
import com.informix.jdbc.stream.impl.StreamException;

import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.relational.TableId;

/**
 * An implementation of the IfxTransactionEngine interface that takes a wider view of which operation types we are interested in.
 *
 * @author Lars M Johansson
 *
 */
public class InformixCdcTransactionEngine implements TransactionEngine {

    private static final Logger LOGGER = LoggerFactory.getLogger(InformixCdcTransactionEngine.class);
    private static final String PROCESSING_RECORD = "Processing {} record";
    private static final String MISSING_TRANSACTION_START_FOR_RECORD = "Missing transaction start for record: {}";
    protected final Builder builder;
    protected final CDCEngine engine;
    protected final ChangeEventSourceContext context;
    protected boolean returnEmptyTransactions;
    protected EnumSet<StreamRecordType> operationFilters;
    protected EnumSet<StreamRecordType> transactionFilters;
    protected final Map<Integer, TransactionHolder> transactionMap;
    protected Map<String, TableId> tableIdByLabelId;

    public static Builder builder(DataSource ds) {
        return new Builder(ds);
    }

    protected InformixCdcTransactionEngine(Builder builder) {
        this.builder = builder;
        this.engine = builder.engine;
        this.context = builder.context;
        this.returnEmptyTransactions = builder.returnEmptyTransactions;
        this.operationFilters = EnumSet.of(INSERT, DELETE, BEFORE_UPDATE, AFTER_UPDATE, TRUNCATE);
        this.transactionFilters = EnumSet.of(COMMIT, ROLLBACK);
        this.transactionMap = new ConcurrentSkipListMap<>();
    }

    @Override
    public StreamRecord getRecord() throws SQLException, StreamException {
        StreamRecord streamRecord;
        while (context.isRunning() && (streamRecord = engine.getRecord()) != null) {

            TransactionHolder holder = transactionMap.get(streamRecord.getTransactionId());
            if (holder != null) {
                LOGGER.debug("Processing [{}] record for transaction id: {}", streamRecord.getType(), streamRecord.getTransactionId());
            }
            switch (streamRecord.getType()) {
                case BEGIN -> {
                    holder = new TransactionHolder();
                    holder.beginRecord = (CDCBeginTransactionRecord) streamRecord;
                    transactionMap.put(streamRecord.getTransactionId(), holder);
                    LOGGER.debug("Watching transaction id: {}", streamRecord.getTransactionId());
                }
                case INSERT, DELETE, BEFORE_UPDATE, AFTER_UPDATE, TRUNCATE -> {
                    if (holder == null) {
                        LOGGER.warn(MISSING_TRANSACTION_START_FOR_RECORD, streamRecord);
                        break;
                    }
                    LOGGER.debug(PROCESSING_RECORD, streamRecord.getType());
                    holder.records.add(streamRecord);
                }
                case DISCARD -> {
                    if (holder == null) {
                        LOGGER.warn(MISSING_TRANSACTION_START_FOR_RECORD, streamRecord);
                        break;
                    }
                    LOGGER.debug(PROCESSING_RECORD, streamRecord.getType());
                    long sequenceId = streamRecord.getSequenceId();

                    if (holder.records.removeIf(r -> r.getSequenceId() >= sequenceId)) {
                        LOGGER.debug("Discarding records with sequence >={}", sequenceId);
                    }
                }
                case COMMIT, ROLLBACK -> {
                    if (holder == null) {
                        LOGGER.warn(MISSING_TRANSACTION_START_FOR_RECORD, streamRecord);
                        break;
                    }
                    LOGGER.debug(PROCESSING_RECORD, streamRecord.getType());
                    holder.closingRecord = streamRecord;
                }
                case METADATA, TIMEOUT, ERROR -> {
                    if (holder == null) {
                        return streamRecord;
                    }
                    LOGGER.debug(PROCESSING_RECORD, streamRecord.getType());
                    holder.records.add(streamRecord);
                }
                default -> LOGGER.warn("Unknown operation for record: {}", streamRecord);
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
    public InformixStreamTransactionRecord getTransaction() throws SQLException, StreamException {
        StreamRecord streamRecord;
        while ((streamRecord = getRecord()) != null && !(streamRecord instanceof InformixStreamTransactionRecord)) {
            LOGGER.debug("Discard non-transaction record: {}", streamRecord);
        }
        return (InformixStreamTransactionRecord) streamRecord;
    }

    @Override
    public InformixCdcTransactionEngine setOperationFilters(StreamRecordType... recordTypes) {
        operationFilters = EnumSet.copyOf(Set.of(recordTypes));
        return this;
    }

    @Override
    public InformixCdcTransactionEngine setTransactionFilters(StreamRecordType... recordTypes) {
        transactionFilters = EnumSet.copyOf(Set.of(recordTypes));
        return this;
    }

    @Override
    public InformixCdcTransactionEngine returnEmptyTransactions(boolean returnEmptyTransactions) {
        this.returnEmptyTransactions = returnEmptyTransactions;
        return this;
    }

    @Override
    public void init() throws SQLException, StreamException {
        engine.init();

        /*
         * Build Map of Label_id to TableId.
         */
        tableIdByLabelId = builder.getWatchedTables().stream()
                .collect(Collectors.toUnmodifiableMap(
                        t -> String.valueOf(t.getLabel()),
                        t -> TableId.parse("%s.%s.%s".formatted(t.getDatabaseName(), t.getNamespace(), t.getTableName()))));
    }

    @Override
    public void close() throws StreamException {
        engine.close();
    }

    public OptionalLong getLowestBeginSequence() {
        return transactionMap.values().stream().mapToLong(t -> t.beginRecord.getSequenceId()).min();
    }

    public Map<String, TableId> getTableIdByLabelId() {
        return tableIdByLabelId;
    }

    protected static class TransactionHolder {
        final List<StreamRecord> records = new ArrayList<>();
        CDCBeginTransactionRecord beginRecord;
        StreamRecord closingRecord;
    }

    public Builder getBuilder() {
        return builder;
    }

    public static class Builder {

        private final CDCEngine.Builder builder;
        private CDCEngine engine;
        private ChangeEventSourceContext context;
        private boolean returnEmptyTransactions = false;

        protected Builder(DataSource ds) {
            builder = CDCEngine.builder(ds);
        }

        public DataSource getDataSource() {
            return builder.getDataSource();
        }

        public Builder context(ChangeEventSourceContext context) {
            this.context = context;
            return this;
        }

        public Builder buffer(int bufferSize) {
            builder.buffer(bufferSize);
            return this;
        }

        public int getBufferSize() {
            return builder.getBufferSize();
        }

        public Builder sequenceId(long position) {
            builder.sequenceId(position);
            return this;
        }

        public long getSequenceId() {
            return builder.getSequenceId();
        }

        public Builder timeout(int timeout) {
            builder.timeout(timeout);
            return this;
        }

        public int getTimeout() {
            return builder.getTimeout();
        }

        public Builder watchTable(String canonicalTableName, String... columns) {
            builder.watchTable(canonicalTableName, columns);
            return this;
        }

        public Builder watchTable(IfmxTableDescriptor desc, String... columns) {
            builder.watchTable(desc, columns);
            return this;
        }

        public Builder watchTable(CDCEngine.IfmxWatchedTable table) {
            builder.watchTable(table);
            return this;
        }

        public List<CDCEngine.IfmxWatchedTable> getWatchedTables() {
            return builder.getWatchedTables();
        }

        public Builder stopLoggingOnClose(boolean stopOnClose) {
            builder.stopLoggingOnClose(stopOnClose);
            return this;
        }

        public Builder returnEmptyTransactions(boolean returnEmptyTransactions) {
            this.returnEmptyTransactions = returnEmptyTransactions;
            return this;
        }

        public InformixCdcTransactionEngine build() throws SQLException {
            engine = builder.build();
            return new InformixCdcTransactionEngine(this);
        }
    }
}
