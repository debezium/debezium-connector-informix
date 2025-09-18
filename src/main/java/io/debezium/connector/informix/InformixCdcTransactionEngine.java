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

import java.net.URI;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.spi.CachingProvider;

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
public class InformixCdcTransactionEngine implements IfxTransactionEngine {

    private static final Logger LOGGER = LoggerFactory.getLogger(InformixCdcTransactionEngine.class);
    private static final String PROCESSING_RECORD = "Processing {} record";
    private static final String MISSING_TRANSACTION_START_FOR_RECORD = "Missing transaction start for record: {}";
    protected final ChangeEventSourceContext context;
    protected final IfxCDCEngine engine;
    protected final InformixConnectorConfig connectorConfig;
    protected final Optional<CachingProvider> cachingProvider;
    protected final Optional<CacheManager> cacheManager;
    protected Optional<Cache<Integer, TransactionHolder>> transactionCache;
    protected Map<Integer, TransactionHolder> transactionMap = new ConcurrentSkipListMap<>();
    protected EnumSet<IfmxStreamRecordType> operationFilters = EnumSet.of(INSERT, DELETE, BEFORE_UPDATE, AFTER_UPDATE, TRUNCATE);
    protected EnumSet<IfmxStreamRecordType> transactionFilters = EnumSet.of(COMMIT, ROLLBACK);
    protected Map<String, TableId> tableIdByLabelId;
    protected boolean returnEmptyTransactions = true;

    public InformixCdcTransactionEngine(ChangeEventSourceContext context, IfxCDCEngine engine, InformixConnectorConfig connectorConfig) {
        this.context = context;
        this.engine = engine;
        this.connectorConfig = connectorConfig;
        this.cachingProvider = Optional.ofNullable(connectorConfig.getJCacheProviderClassName()).map(Caching::getCachingProvider);
        Optional<URI> jCacheURI = Optional.ofNullable(connectorConfig.getJCacheUri()).map(ClassLoader::getSystemResource).map(url -> {
            try {
                return url.toURI();
            }
            catch (Exception e) {
                return null;
            }
        });
        this.cacheManager = jCacheURI.flatMap(uri -> cachingProvider.map(cp -> cp.getCacheManager(uri, null)))
                .or(() -> cachingProvider.map(CachingProvider::getCacheManager));
    }

    @Override
    public IfmxStreamRecord getRecord() throws SQLException, IfxStreamException {
        IfmxStreamRecord streamRecord;
        while (context.isRunning() && (streamRecord = engine.getRecord()) != null) {

            TransactionHolder holder = getTransactionHolder(streamRecord.getTransactionId());
            if (holder != null) {
                LOGGER.debug("Processing [{}] record for transaction id: {}", streamRecord.getType(), streamRecord.getTransactionId());
            }
            switch (streamRecord.getType()) {
                case BEGIN:
                    holder = new TransactionHolder();
                    holder.beginRecord = (IfxCDCBeginTransactionRecord) streamRecord;
                    putTransactionHolder(streamRecord.getTransactionId(), holder);
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
                removeTransactionHolder(streamRecord.getTransactionId());
                if (!holder.records.isEmpty() || returnEmptyTransactions) {
                    return new InformixStreamTransactionRecord(holder.beginRecord, holder.closingRecord, holder.records);
                }
            }
        }

        return null;
    }

    private TransactionHolder getTransactionHolder(int transactionId) {
        return transactionCache.map(cache -> cache.get(transactionId))
                .orElse(transactionMap.get(transactionId));
    }

    private void putTransactionHolder(int transactionId, TransactionHolder holder) {
        transactionCache.ifPresentOrElse(cache -> cache.put(transactionId, holder),
                () -> transactionMap.put(transactionId, holder));
    }

    private void removeTransactionHolder(int transactionId) {
        transactionCache.ifPresentOrElse(cache -> cache.remove(transactionId),
                () -> transactionMap.remove(transactionId));
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
    public InformixCdcTransactionEngine setOperationFilters(IfmxStreamRecordType... recordTypes) {
        operationFilters = EnumSet.copyOf(Set.of(recordTypes));
        return this;
    }

    @Override
    public InformixCdcTransactionEngine setTransactionFilters(IfmxStreamRecordType... recordTypes) {
        transactionFilters = EnumSet.copyOf(Set.of(recordTypes));
        return this;
    }

    @Override
    public InformixCdcTransactionEngine returnEmptyTransactions(boolean returnEmptyTransactions) {
        this.returnEmptyTransactions = returnEmptyTransactions;
        return this;
    }

    @Override
    public void init() throws SQLException, IfxStreamException {
        engine.init();

        transactionCache = cacheManager.map(cm -> cm.getCache(connectorConfig.getTransactionCacheName()));

        /*
         * Build Map of Label_id to TableId.
         */
        tableIdByLabelId = engine.getBuilder().getWatchedTables().stream()
                .collect(Collectors.toUnmodifiableMap(
                        t -> String.valueOf(t.getLabel()),
                        t -> TableId.parse("%s.%s.%s".formatted(t.getDatabaseName(), t.getNamespace(), t.getTableName()))));
    }

    @Override
    public void close() throws IfxStreamException {
        if (engine != null) {
            engine.close();
        }
        cachingProvider.ifPresent(CachingProvider::close);
    }

    public OptionalLong getLowestBeginSequence() {
        return transactionCache
                .map(cache -> StreamSupport.stream(cache.spliterator(), false)
                        .map(Cache.Entry::getValue))
                .orElseGet(() -> transactionMap.values().stream())
                .mapToLong(t -> t.beginRecord.getSequenceId()).min();
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
