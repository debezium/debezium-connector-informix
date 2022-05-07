package laoflch.debezium.connector.informix;

import com.informix.jdbc.IfmxReadableType;
import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class InformixTransactionCache {

    private static Logger LOGGER = LoggerFactory.getLogger(InformixTransactionCache.class);

    private Map<Long, TransactionCacheBuffer> transactionCacheBufferMap;
    private Map<Long, Map<String, IfmxReadableType>> beforeAndAfter;

    public InformixTransactionCache() {
        // TODO: try HPPC or FastUtil?
        this.transactionCacheBufferMap = new Hashtable<>();
        this.beforeAndAfter = new HashMap<>();
    }

    public Optional<TransactionCacheBuffer> beginTxn(Long txn) {
        if (transactionCacheBufferMap.containsKey(txn)) {
            LOGGER.warn("Transaction key={} already exists in InformixTransactionCache", txn);
            return Optional.empty();
        }
        TransactionCacheBuffer cre = new TransactionCacheBuffer(4096);
        return Optional.ofNullable(transactionCacheBufferMap.put(txn, cre));
    }

    public Optional<TransactionCacheBuffer> commitTxn(Long txn) {
        if (!transactionCacheBufferMap.containsKey(txn)) {
            LOGGER.warn("Transaction key={} does not exist in InformixTransactionCache while commitTxn()", txn);
            return Optional.empty();
        }

        return Optional.ofNullable(transactionCacheBufferMap.remove(txn));
    }

    public Optional<TransactionCacheBuffer> rollbackTxn(Long txn) {
        if (!transactionCacheBufferMap.containsKey(txn)) {
            LOGGER.warn("Transaction key={} does not exist in InformixTransactionCache while rollbackTxn()", txn);
            return Optional.empty();
        }

        return Optional.ofNullable(transactionCacheBufferMap.remove(txn));
    }

    public TransactionCacheBuffer addEvent2Tx(TableId tableId, InformixChangeRecordEmitter event, Long txn) {
        if (event != null) {
            TransactionCacheBuffer buffer = transactionCacheBufferMap.get(txn);

            if (buffer != null) {
                buffer.getTransactionCacheRecords().add(
                        new TransactionCacheRecord(tableId, event)
                );
                return buffer;
            }
        }

        return null;
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

        // TODO: Transaction BeginTime & EndTime
        private List<TransactionCacheRecord> transactionCacheRecordList;

        public TransactionCacheBuffer(int initialSize) {
            transactionCacheRecordList = new ArrayList<TransactionCacheRecord>(initialSize);
        }

        public List<TransactionCacheRecord> getTransactionCacheRecords() {
            return transactionCacheRecordList;
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
