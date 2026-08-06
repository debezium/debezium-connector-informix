/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix.stream;

import java.sql.Connection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.informix.jdbc.IfxBigInt;
import com.informix.jdbc.IfxBoolean;
import com.informix.jdbc.IfxChar;
import com.informix.jdbc.IfxColumnInfo;
import com.informix.jdbc.IfxConnection;
import com.informix.jdbc.IfxDate;
import com.informix.jdbc.IfxDateTime;
import com.informix.jdbc.IfxDecimal;
import com.informix.jdbc.IfxFloat;
import com.informix.jdbc.IfxInt8;
import com.informix.jdbc.IfxInteger;
import com.informix.jdbc.IfxLocator;
import com.informix.jdbc.IfxLvarchar;
import com.informix.jdbc.IfxObject;
import com.informix.jdbc.IfxShort;
import com.informix.jdbc.IfxSmBlob;
import com.informix.jdbc.IfxSmallFloat;
import com.informix.jdbc.IfxSqliConnect;
import com.informix.jdbc.IfxValue;
import com.informix.jdbc.IfxVarChar;
import com.informix.jdbc.stream.api.StreamRecordType;
import com.informix.jdbc.stream.cdc.records.CDCOperationRecord;
import com.informix.jdbc.stream.impl.StreamException;
import com.informix.jdbc.types.ReadableType;
import com.informix.lang.IfxToJavaType;

/**
 * An extension of CDCOperationRecord to fix faulty handling of null-valued boolean.
 *
 * @author Lars M Johansson
 */
public class DbzCDCOperationRecord extends CDCOperationRecord {

    private static final Logger LOGGER = LoggerFactory.getLogger(DbzCDCOperationRecord.class);

    protected final byte[] buffer;
    protected final List<IfxColumnInfo> columns;
    protected final Connection connection;
    protected final Map<String, ReadableType> objects;

    public DbzCDCOperationRecord(StreamRecordType type, byte[] header, byte[] buffer, Map<String, List<IfxColumnInfo>> columns, Connection connection) {
        super(type, header, buffer, columns, connection);
        this.buffer = buffer;
        this.connection = connection;
        this.columns = columns.get(this.label);
        this.objects = new LinkedHashMap<>((int) (columns.size() / 0.7F));
        LOGGER.debug("New DbzCDCOperationRecord created. Label: {}", this.label);
    }

    @Override
    public Map<String, ReadableType> getData() throws StreamException {
        synchronized (objects) {
            if (objects.isEmpty()) {
                try {
                    int offset = 0;

                    for (IfxColumnInfo column : columns) {
                        IfxObject ifxObject = IfxValue.makeInstance((IfxSqliConnect) connection, column);
                        LOGGER.debug("Column type: {}", ifxObject.getClass().getSimpleName());
                        if (ifxObject instanceof IfxBoolean ifxBoolean) {
                            if (buffer[offset] == 1) {
                                ifxBoolean.nullify();
                            }
                            else {
                                ifxBoolean.fromIfx(buffer, offset);
                            }
                            offset += 2;
                        }
                        else if (ifxObject instanceof IfxShort ifxShort) {
                            ifxShort.fromIfx(buffer, offset, 2);
                            offset += 2;
                        }
                        else if (ifxObject instanceof IfxInteger ifxInteger) {
                            ifxInteger.fromIfx(buffer, offset, 4);
                            offset += 4;
                        }
                        else if (ifxObject instanceof IfxBigInt ifxBigInt) {
                            ifxBigInt.fromIfx(buffer, offset, 8);
                            offset += 8;
                        }
                        else if (ifxObject instanceof IfxInt8 ifxInt8) {
                            ifxInt8.fromIfx(buffer, offset, 10);
                            offset += 10;
                        }
                        else if (ifxObject instanceof IfxDecimal ifxDecimal) {
                            short encodedLength = (short) column.getColumnLength();
                            int length = ((encodedLength >> 8 & 255) + (encodedLength & 255 & 1) + 3) / 2 - 1 + 1;
                            ifxDecimal.fromIfx(buffer, offset, length, encodedLength);
                            offset += length;
                        }
                        else if (ifxObject instanceof IfxSmallFloat ifxSmallFloat) {
                            ifxSmallFloat.fromIfx(buffer, offset, 4);
                            offset += 4;
                        }
                        else if (ifxObject instanceof IfxFloat ifxFloat) {
                            ifxFloat.fromIfx(buffer, offset, 8);
                            offset += 8;
                        }
                        else if (ifxObject instanceof IfxDate ifxDate) {
                            ifxDate.fromIfx(buffer, offset, 4);
                            offset += 4;
                        }
                        else if (ifxObject instanceof IfxDateTime ifxDateTime) {
                            ifxDateTime.fromIfx(buffer, offset, column.getNumberOfBytes(), (short) column.getColumnLength());
                            offset += column.getNumberOfBytes();
                        }
                        else if (ifxObject instanceof IfxChar ifxChar) {
                            int length = column.getColumnLength();
                            ifxChar.fromIfx(buffer, offset, length);
                            offset += length;
                        }
                        else if (ifxObject instanceof IfxLvarchar ifxLvarchar) {
                            short length = IfxToJavaType.IfxToJavaSmallInt(buffer, offset);
                            ifxLvarchar.fromCDC(buffer, offset, length - 1);
                            offset += length + 2;
                        }
                        else if (ifxObject instanceof IfxVarChar ifxVarChar) {
                            int length = buffer[offset] & 255;
                            ifxVarChar.fromIfx(buffer, offset, length);
                            offset += length + 1;
                        }
                        else if (ifxObject instanceof IfxSmBlob) {
                            byte[] bytes = new byte[72];
                            System.arraycopy(buffer, offset + 4, bytes, 0, 72);
                            ifxObject = new IfxSmBlob((IfxConnection) connection, new IfxLocator(bytes, connection));
                            offset += 76;
                        }
                        else {
                            throw new StreamException("Unsupported column type: " + column);
                        }

                        objects.put(column.getColumnName(), ifxObject);
                    }
                }
                catch (Exception e) {
                    throw new StreamException("Error processing CDC data stream", e);
                }
            }
        }

        return objects;
    }
}