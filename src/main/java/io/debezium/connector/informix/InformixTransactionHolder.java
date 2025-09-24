/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import java.util.ArrayList;
import java.util.List;

import com.informix.stream.api.IfmxStreamRecord;
import com.informix.stream.cdc.records.IfxCDCBeginTransactionRecord;

public class InformixTransactionHolder {
    final List<IfmxStreamRecord> records = new ArrayList<>();
    IfxCDCBeginTransactionRecord beginRecord;
    IfmxStreamRecord closingRecord;
}
