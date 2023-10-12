/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import java.util.List;
import java.util.stream.Collectors;

import com.informix.stream.api.IfmxStreamOperationRecord;
import com.informix.stream.api.IfmxStreamRecord;
import com.informix.stream.cdc.records.IfxCDCBeginTransactionRecord;
import com.informix.stream.transactions.IfmxStreamTransactionRecord;

/**
 * An extension of IfmxStreamTransactionRecord that takes a wider view of which operation types we are interested in.
 *
 * @author Lars M Johansson
 */
public class InformixStreamTransactionRecord extends IfmxStreamTransactionRecord implements IfmxStreamRecord {

    private final List<IfmxStreamRecord> records;

    public InformixStreamTransactionRecord(IfxCDCBeginTransactionRecord beginRecord, IfmxStreamRecord closingRecord, List<IfmxStreamRecord> records) {
        super(beginRecord, closingRecord, List.of());
        this.records = records;
    }

    @Override
    public List<IfmxStreamOperationRecord> getOperationRecords() {
        return records.stream().filter(IfmxStreamRecord::hasOperationData).map(IfmxStreamOperationRecord.class::cast).collect(Collectors.toList());
    }

    public List<IfmxStreamRecord> getRecords() {
        return records;
    }

    public String toString() {
        return super.getBeginRecord().toString() + "\n" + this.getRecords() + "\n" + super.getEndRecord().toString();
    }
}
