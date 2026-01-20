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
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
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
    protected final CachingProvider cachingProvider;
    protected final CacheManager cacheManager;
    protected Cache<Long, IfmxStreamRecord> streamRecordCache;
    protected Map<Integer, InformixTransactionHolder> transactionMap = new ConcurrentSkipListMap<>();
    protected EnumSet<IfmxStreamRecordType> operationFilters = EnumSet.of(INSERT, DELETE, BEFORE_UPDATE, AFTER_UPDATE, TRUNCATE);
    protected EnumSet<IfmxStreamRecordType> transactionFilters = EnumSet.of(COMMIT, ROLLBACK);
    protected Map<String, TableId> tableIdByLabelId;
    protected boolean returnEmptyTransactions = true;

    public InformixCdcTransactionEngine(ChangeEventSourceContext context, IfxCDCEngine engine, InformixConnectorConfig connectorConfig) {
        this.context = context;
        this.engine = engine;
        this.connectorConfig = connectorConfig;
        Optional<CachingProvider> cachingProvider = Optional.ofNullable(connectorConfig.getJCacheProviderClassName()).map(Caching::getCachingProvider);
        Optional<URI> jCacheURI = Optional.ofNullable(connectorConfig.getJCacheUri()).map(ClassLoader::getSystemResource).map(url -> {
            try {
                return url.toURI();
            }
            catch (Exception e) {
                return null;
            }
        });
        this.cacheManager = jCacheURI.flatMap(uri -> cachingProvider.map(cp -> cp.getCacheManager(uri, null)))
                .or(() -> cachingProvider.map(CachingProvider::getCacheManager)).orElse(null);
        this.cachingProvider = cachingProvider.orElse(null);
    }

    @Override
    public IfmxStreamRecord getRecord() throws SQLException, IfxStreamException {
        IfmxStreamRecord streamRecord;
        while (context.isRunning() && (streamRecord = engine.getRecord()) != null) {

            InformixTransactionHolder holder = transactionMap.get(streamRecord.getTransactionId());
            if (holder != null) {
                LOGGER.debug("Processing [{}] record for transaction id: {}", streamRecord.getType(), streamRecord.getTransactionId());
            }
            switch (streamRecord.getType()) {
                case BEGIN:
                    holder = new InformixTransactionHolder();
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
                    holder.appendRecord(streamRecord);
                    break;
                case DISCARD:
                    if (holder == null) {
                        LOGGER.warn(MISSING_TRANSACTION_START_FOR_RECORD, streamRecord);
                        break;
                    }
                    LOGGER.debug(PROCESSING_RECORD, streamRecord.getType());
                    long sequenceId = streamRecord.getSequenceId();

                    if (holder.discardRecordsAfter(sequenceId)) {
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
                    holder.appendRecord(streamRecord);
                    break;
                default:
                    LOGGER.warn("Unknown operation for record: {}", streamRecord);
            }
            if (holder != null && holder.closingRecord != null) {
                transactionMap.remove(streamRecord.getTransactionId());
                if (!holder.isRecordsEmpty() || returnEmptyTransactions) {
                    return new InformixStreamTransactionRecord(holder.beginRecord, holder.closingRecord, holder.getRecords());
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

        if (cacheManager != null && !cacheManager.isClosed()) {
            streamRecordCache = cacheManager.getCache(connectorConfig.getTransactionCacheName());
            if (streamRecordCache != null) {
                streamRecordCache.registerCacheEntryListener(
                        new MutableCacheEntryListenerConfiguration<>(
                                FactoryBuilder.factoryOf(IfxCacheEntryExpiredListener.class),
                                null, true, true));
            }
        }

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
        if (cachingProvider != null) {
            cachingProvider.close();
        }
    }

    public OptionalLong getLowestBeginSequence() {
        return transactionMap.values().stream().mapToLong(t -> t.beginRecord.getSequenceId()).min();
    }

    public Map<String, TableId> getTableIdByLabelId() {
        return tableIdByLabelId;
    }

    class InformixTransactionHolder {
        final List<IfmxStreamRecord> records = new ArrayList<>();
        final Set<Long> sequenceIds = new ConcurrentSkipListSet<>();
        IfxCDCBeginTransactionRecord beginRecord;
        IfmxStreamRecord closingRecord;

        public List<IfmxStreamRecord> getRecords() {
            if (streamRecordCache != null) {
                return List.copyOf(streamRecordCache.getAll(sequenceIds).values());
            }
            return records;
        }

        boolean appendRecord(IfmxStreamRecord streamRecord) {
            if (streamRecordCache != null) {
                streamRecordCache.putIfAbsent(streamRecord.getSequenceId(), streamRecord);
                return sequenceIds.add(streamRecord.getSequenceId());
            }
            return records.add(streamRecord);
        }

        boolean discardRecordsAfter(long sequenceId) {
            if (streamRecordCache != null) {
                streamRecordCache.removeAll(sequenceIds.stream().filter(id -> id >= sequenceId).collect(Collectors.toSet()));
                return sequenceIds.removeIf(id -> id >= sequenceId);
            }
            return records.removeIf(r -> r.getSequenceId() >= sequenceId);
        }

        boolean isRecordsEmpty() {
            return records.isEmpty() && sequenceIds.isEmpty();
        }
    }
}
