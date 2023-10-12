/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.kafka.connect.data.Field;

import com.informix.jdbc.IfmxReadableType;

import io.debezium.data.Envelope.Operation;
import io.debezium.relational.RelationalChangeRecordEmitter;
import io.debezium.relational.TableSchema;
import io.debezium.util.Clock;

/**
 * Emits change data based on a single (or two in case of updates) CDC data row(s).
 *
 * @author Laoflch Luo, Xiaolin Zhang, Lars M Johansson
 *
 */
public class InformixChangeRecordEmitter extends RelationalChangeRecordEmitter<InformixPartition> {

    private final Operation operation;
    private final Object[] before;
    private final Object[] after;

    public InformixChangeRecordEmitter(InformixPartition partition, InformixOffsetContext offsetContext, Clock clock, Operation operation, Object[] before,
                                       Object[] after, InformixConnectorConfig connectorConfig) {
        super(partition, offsetContext, clock, connectorConfig);

        this.operation = operation;
        this.before = before;
        this.after = after;
    }

    @Override
    public Operation getOperation() {
        return operation;
    }

    @Override
    protected Object[] getOldColumnValues() {
        return before;
    }

    @Override
    protected Object[] getNewColumnValues() {
        return after;
        }

    @Override
    protected void emitTruncateRecord(Receiver<InformixPartition> receiver, TableSchema tableSchema) throws InterruptedException {
        receiver.changeRecord(getPartition(), tableSchema, Operation.TRUNCATE, null,
                tableSchema.getEnvelopeSchema().truncate(getOffset().getSourceInfo(), getClock().currentTimeAsInstant()), getOffset(), null);
    }

    /**
     * Convert columns data from Map[String,IfmxReadableType] to Object[].
     * Debezium can't convert the IfmxReadableType object to kafka direct,so use map[AnyRef](x=>x.toObject) to extract the java
     * type value from IfmxReadableType and pass to debezium for kafka
     *
     * @param data the data from informix cdc map[String,IfmxReadableType].
     * @author Laoflch Luo, Xiaolin Zhang
     */
    public static Object[] convertIfxData2Array(Map<String, IfmxReadableType> data, TableSchema tableSchema) {
        return data == null ? new Object[0]
                : tableSchema.valueSchema().fields().stream()
                        .map(Field::name)
                        .map(data::get)
                        .map(irt -> propagate(irt::toObject)).toArray();
        }

    private static <X> X propagate(Callable<X> callable) {
        try {
            return callable.call();
        }
        catch (Exception e) {
            throw (e instanceof RuntimeException) ? (RuntimeException) e : new RuntimeException(e);
    }
    }
}
