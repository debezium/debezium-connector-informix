/*
 * Copyright Debezium-Informix-Connector Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package laoflch.debezium.connector.informix;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.informix.jdbc.IfmxReadableType;

import io.debezium.relational.TableId;

public class InformixTransactionCache {

    private static Logger LOGGER = LoggerFactory.getLogger(InformixTransactionCache.class);

    private Map<Long, TransactionCacheBuffer> transactionCacheBufferMap;
    private Map<Long, Map<String, IfmxReadableType>> beforeAndAfter;

    public InformixTransactionCache() {
        // TODO: try HPPC or FastUtil?
        this.transactionCacheBufferMap = new Hashtable<>();
        this.beforeAndAfter = new HashMap<>();
    }

    public Optional<TransactionCacheBuffer> beginTxn(Long txn, Long beginTs) {
        if (transactionCacheBufferMap.containsKey(txn)) {
            LOGGER.warn("Transaction key={} already exists in InformixTransactionCache", txn);
            return Optional.empty();
        }
        TransactionCacheBuffer tb = new TransactionCacheBuffer(4096, beginTs);
        return Optional.ofNullable(transactionCacheBufferMap.put(txn, tb));
    }

    public Optional<TransactionCacheBuffer> commitTxn(Long txn, Long endTime) {
        if (!transactionCacheBufferMap.containsKey(txn)) {
            LOGGER.warn("Transaction key={} does not exist in InformixTransactionCache while commitTxn()", txn);
            return Optional.empty();
        }

        TransactionCacheBuffer transactionCacheBuffer = transactionCacheBufferMap.remove(txn);
        transactionCacheBuffer.setEndTime(endTime);
        return Optional.of(transactionCacheBuffer);
    }

    public Optional<TransactionCacheBuffer> rollbackTxn(Long txn) {
        if (!transactionCacheBufferMap.containsKey(txn)) {
            LOGGER.warn("Transaction key={} does not exist in InformixTransactionCache while rollbackTxn()", txn);
            return Optional.empty();
        }

        return Optional.ofNullable(transactionCacheBufferMap.remove(txn));
    }

    public void addEvent2Tx(TableId tableId, InformixChangeRecordEmitter event, Long txn) {
        if (event != null) {
            TransactionCacheBuffer buffer = transactionCacheBufferMap.get(txn);

            if (buffer != null) {
                buffer.getTransactionCacheRecords().add(
                        new TransactionCacheRecord(tableId, event));
            }
        }
    }

    public Optional<Map<String, IfmxReadableType>> beforeUpdate(Long txn, Map<String, IfmxReadableType> data) {
        if (beforeAndAfter.containsKey(txn)) {
            LOGGER.warn("Transaction key={} already exists in BeforeAfterCache", txn);
            return Optional.empty();
        }

        return Optional.ofNullable(beforeAndAfter.put(txn, data));
    }

    public Optional<Map<String, IfmxReadableType>> afterUpdate(Long txn) {
        if (!beforeAndAfter.containsKey(txn)) {
            LOGGER.warn("Transaction key={} does not exist in BeforeAfterCache", txn);
            return Optional.empty();
        }

        return Optional.ofNullable(beforeAndAfter.remove(txn));
    }

    public static class TransactionCacheBuffer {

        private final List<TransactionCacheRecord> transactionCacheRecordList;
        private Long beginTime; // Begin time of transaction
        private Long endTime; // Commit/Rollback of the transaction

        public TransactionCacheBuffer(int initialSize, Long beginTs) {
            transactionCacheRecordList = new ArrayList<>(initialSize);
            beginTime = beginTs;
            endTime = -1L;
        }

        public List<TransactionCacheRecord> getTransactionCacheRecords() {
            return transactionCacheRecordList;
        }

        public Long getBeginTime() {
            return beginTime;
        }

        public Long getEndTime() {
            return endTime;
        }

        public void setEndTime(Long endTime) {
            this.endTime = endTime;
        }

        public Long getElapsed() {
            return this.endTime - this.beginTime;
        }

        public int size() {
            return transactionCacheRecordList.size();
        }
    }

    public static class TransactionCacheRecord {

        private TableId tableId;
        private InformixChangeRecordEmitter informixChangeRecordEmitter;

        public TransactionCacheRecord(TableId tableId, InformixChangeRecordEmitter informixChangeRecordEmitter) {
            this.tableId = tableId;
            this.informixChangeRecordEmitter = informixChangeRecordEmitter;
        }

        public TableId getTableId() {
            return tableId;
        }

        public void setTableId(TableId tableId) {
            this.tableId = tableId;
        }

        public InformixChangeRecordEmitter getInformixChangeRecordEmitter() {
            return informixChangeRecordEmitter;
        }

        public void setInformixChangeRecordEmitter(InformixChangeRecordEmitter informixChangeRecordEmitter) {
            this.informixChangeRecordEmitter = informixChangeRecordEmitter;
        }
    }
}
