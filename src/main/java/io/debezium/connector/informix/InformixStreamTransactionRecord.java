/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import java.util.List;
import java.util.stream.Collectors;

import com.informix.jdbc.stream.api.StreamOperationRecord;
import com.informix.jdbc.stream.api.StreamRecord;
import com.informix.jdbc.stream.cdc.records.CDCBeginTransactionRecord;
import com.informix.jdbc.stream.transactions.StreamTransactionRecord;

/**
 * An extension of IfmxStreamTransactionRecord that takes a wider view of which operation types we are interested in.
 *
 * @author Lars M Johansson
 */
public class InformixStreamTransactionRecord extends StreamTransactionRecord implements StreamRecord {

    private final List<StreamRecord> records;

    public InformixStreamTransactionRecord(CDCBeginTransactionRecord beginRecord, StreamRecord closingRecord, List<StreamRecord> records) {
        super(beginRecord, closingRecord, List.of());
        this.records = records;
    }

    @Override
    public List<StreamOperationRecord> getOperationRecords() {
        return records.stream().filter(StreamRecord::hasOperationData).map(StreamOperationRecord.class::cast).collect(Collectors.toList());
    }

    public List<StreamRecord> getRecords() {
        return records;
    }

    public String toString() {
        return super.getBeginRecord().toString() + "\n" + this.getRecords() + "\n" + super.getEndRecord().toString();
    }
}
