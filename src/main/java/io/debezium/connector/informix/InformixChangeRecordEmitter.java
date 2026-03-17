/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import java.util.Map;
import java.util.concurrent.Callable;

import com.informix.jdbc.IfmxReadableType;
import com.informix.jdbc.IfxUDT;
import com.informix.jdbc.udt.BasicUdt;

import io.debezium.DebeziumException;
import io.debezium.data.Envelope.Operation;
import io.debezium.relational.RelationalChangeRecordEmitter;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.util.Clock;

/**
 * Emits change data based on a single (or two in case of updates) CDC data row(s).
 *
 * @author Laoflch Luo, Xiaolin Zhang, Lars M Johansson
 *
 */
public class InformixChangeRecordEmitter extends RelationalChangeRecordEmitter<InformixPartition> {

    private final InformixDatabaseSchema schema;
    private final TableId tableId;
    private final Operation operation;
    private final Map<String, IfmxReadableType> before;
    private final Map<String, IfmxReadableType> after;
    private final IfxUDT placeholderValue;

    public InformixChangeRecordEmitter(InformixPartition partition, InformixOffsetContext offsetContext, Clock clock,
                                       InformixConnectorConfig connectorConfig, InformixDatabaseSchema schema, TableId tableId,
                                       Operation operation, Map<String, IfmxReadableType> before, Map<String, IfmxReadableType> after) {
        super(partition, offsetContext, clock, connectorConfig);

        this.schema = schema;
        this.tableId = tableId;
        this.operation = operation;
        this.before = before;
        this.after = after;
        this.placeholderValue = new BasicUdt(connectorConfig.getUnavailableValuePlaceholder());
    }

    @Override
    public Operation getOperation() {
        return operation;
    }

    @Override
    protected Object[] getOldColumnValues() {
        return columnValues(before, schema.tableFor(tableId));
    }

    @Override
    protected Object[] getNewColumnValues() {
        return columnValues(after, schema.tableFor(tableId));
    }

    @Override
    protected void emitTruncateRecord(Receiver<InformixPartition> receiver, TableSchema tableSchema) throws InterruptedException {
        receiver.changeRecord(getPartition(), tableSchema, Operation.TRUNCATE, null,
                tableSchema.getEnvelopeSchema().truncate(getOffset().getSourceInfo(), getClock().currentTimeAsInstant()),
                getOffset(), null);
    }

    protected Object[] columnValues(Map<String, IfmxReadableType> data, Table table) {
        // based on the schema columns, create the values on the same position as the columns
        return data == null ? new Object[table.columns().size()]
                : table.retrieveColumnNames().stream()
                        .map(key -> data.getOrDefault(key, placeholderValue))
                        .map(type -> propagate(type::toObject)).toArray();
    }

    private static <X> X propagate(Callable<X> callable) {
        try {
            return callable.call();
        }
        catch (RuntimeException rex) {
            throw rex;
        }
        catch (Exception ex) {
            throw new DebeziumException(ex);
        }
    }
}
