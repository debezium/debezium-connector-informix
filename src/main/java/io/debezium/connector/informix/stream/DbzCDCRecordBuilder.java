/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix.stream;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.informix.jdbc.IfxColumnInfo;
import com.informix.jdbc.stream.api.StreamRecord;
import com.informix.jdbc.stream.api.StreamRecordType;
import com.informix.jdbc.stream.cdc.CDCRecordBuilder;
import com.informix.jdbc.stream.impl.StreamException;

import io.debezium.DebeziumException;

/**
 * A reimplementation of CDCRecordBuilder that returns DbzCDCOperationRecords instead of CDCOperationRecords.
 *
 * @author Lars M Johansson
 */
public class DbzCDCRecordBuilder extends CDCRecordBuilder {

    private static final Logger LOGGER = LoggerFactory.getLogger(DbzCDCRecordBuilder.class);

    protected final Connection connection;
    protected final Map<String, List<IfxColumnInfo>> columnMap;

    public DbzCDCRecordBuilder(Connection connection) {
        super(connection);
        this.connection = connection;
        try {
            Field columnMap = getClass().getSuperclass().getDeclaredField("columnMap");
            columnMap.setAccessible(true);
            this.columnMap = (Map<String, List<IfxColumnInfo>>) columnMap.get(this);
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            throw new DebeziumException(e);
        }
    }

    public StreamRecord buildRecord(byte[] bytes) throws SQLException, StreamException {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        int headerSize = buffer.getInt() - 16;
        int payloadSize = buffer.getInt();
        int packetScheme = buffer.getInt();
        int recordType = buffer.getInt();
        byte[] header = new byte[headerSize];
        byte[] payload = new byte[payloadSize];
        buffer.get(header);
        buffer.get(payload);
        LOGGER.trace("Record type [{}]", recordType);
        return switch (recordType) {
            case 40 -> new DbzCDCOperationRecord(StreamRecordType.INSERT, header, payload, columnMap, connection);
            case 41 -> new DbzCDCOperationRecord(StreamRecordType.DELETE, header, payload, columnMap, connection);
            case 42 -> new DbzCDCOperationRecord(StreamRecordType.BEFORE_UPDATE, header, payload, columnMap, connection);
            case 43 -> new DbzCDCOperationRecord(StreamRecordType.AFTER_UPDATE, header, payload, columnMap, connection);
            default -> super.buildRecord(bytes);
        };
    }

    @Override
    public void close() throws SQLException {
        connection.close();
    }
}