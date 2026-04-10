/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix.stream;

import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.informix.jdbc.IfmxTableDescriptor;
import com.informix.jdbc.IfxSmartBlob;
import com.informix.jdbc.stream.api.StreamEngine;
import com.informix.jdbc.stream.api.StreamRecord;
import com.informix.jdbc.stream.cdc.CDCEngine.IfmxWatchedTable;
import com.informix.jdbc.stream.cdc.CDCRecordBuilder;
import com.informix.jdbc.stream.impl.StreamException;
import com.informix.lang.Messages;

import io.debezium.DebeziumException;

public class DbzCDCEngine implements StreamEngine {

    private static final Logger LOGGER = LoggerFactory.getLogger(DbzCDCEngine.class);

    protected final Builder builder;
    protected final DataSource dataSource;
    protected final Connection connection;
    protected final int bufferSize;
    protected final int maxRecords;
    protected final long position;
    protected final int timeout;
    protected final List<IfmxWatchedTable> watchedTables;
    protected final boolean stopLoggingOnClose;
    protected final CDCRecordBuilder recordBuilder;
    protected final byte[] buffer;
    protected int sessionId;
    protected IfxSmartBlob smartBlob;
    protected int bytesPending;
    protected boolean closed;

    public static Builder builder(DataSource connection) {
        return new Builder(connection);
    }

    protected DbzCDCEngine(Builder builder) throws SQLException {
        this.builder = builder;
        this.dataSource = builder.dataSource;
        this.connection = this.dataSource.getConnection();
        this.bufferSize = builder.bufferSize;
        this.maxRecords = builder.maxRecords;
        this.position = builder.position;
        this.timeout = builder.timeout;
        this.watchedTables = builder.watchedTables;
        this.stopLoggingOnClose = builder.stopLoggingOnClose;
        this.recordBuilder = new CDCRecordBuilder(dataSource.getConnection());
        this.buffer = new byte[bufferSize];
        this.bytesPending = 0;
        this.closed = false;
    }

    @Override
    public StreamRecord getRecord() throws SQLException, StreamException {
        if (bytesPending < 16) {
            readFromLob();
        }
        ByteBuffer byteBuffer = ByteBuffer.wrap(buffer, 0, bytesPending);

        StreamRecord record = buildRecord(byteBuffer);

        if (record != null) {
            bytesPending = byteBuffer.remaining();
            System.arraycopy(buffer, byteBuffer.position(), buffer, 0, bytesPending);
        }

        return record;
    }

    @Override
    public List<StreamRecord> getRecords() throws SQLException, StreamException {
        List<StreamRecord> records = new ArrayList<>();
        readFromLob();
        ByteBuffer byteBuffer = ByteBuffer.wrap(buffer, 0, bytesPending);

        StreamRecord record;
        while (byteBuffer.remaining() >= 16 && (record = buildRecord(byteBuffer)) != null) {
            records.add(record);
        }

        bytesPending = byteBuffer.remaining();
        byteBuffer.get(buffer, 0, bytesPending);

        return records;
    }

    protected void readFromLob() throws SQLException, StreamException {
        int bytesToRead = bufferSize - bytesPending;
        ByteBuffer byteBuffer = ByteBuffer.wrap(buffer, bytesPending, bytesToRead);
        OutputStream byteStream = new ByteBufferOutputStream(byteBuffer);
        int bytesRead = smartBlob.IfxLoRead(sessionId, byteStream, bytesToRead);
        if (bytesRead < 0) {
            throw new StreamException("IfxLoRead returned -1, no more data?");
        }
        bytesPending = byteBuffer.position();
    }

    protected @Nullable StreamRecord buildRecord(ByteBuffer byteBuffer) throws SQLException, StreamException {
        byteBuffer.mark();
        int headerSize = byteBuffer.getInt();
        int payloadSize = byteBuffer.getInt();
        byteBuffer.reset();

        int recordSize = headerSize + payloadSize;
        if (byteBuffer.remaining() < recordSize) {
            return null;
        }

        byte[] recordBytes = new byte[recordSize];
        byteBuffer.get(recordBytes);
        return recordBuilder.buildRecord(recordBytes);
    }

    @Override
    public void init() throws StreamException {
        openSession();

        this.smartBlob = new IfxSmartBlob(this.connection);

        for (IfmxWatchedTable table : this.watchedTables) {
            watchTable(table);
        }

        activateSession();
    }

    private void openSession() throws StreamException {
        try (CallableStatement cs = this.connection.prepareCall("select env_value from sysmaster:sysenv where env_name = 'INFORMIXSERVER'");
                PreparedStatement ps = this.connection.prepareStatement("execute function informix.cdc_opensess(?,?,?,?,?,?)")) {
            ResultSet rs = cs.executeQuery();
            String serverName = rs.next() ? rs.getString(1).trim() : "";
            LOGGER.debug("Server name detected: {}", serverName);
            ps.setString(1, serverName);
            ps.setInt(2, 0);
            ps.setInt(3, this.timeout);
            ps.setInt(4, this.maxRecords);
            ps.setInt(5, 1);
            ps.setInt(6, 1);
            rs = ps.executeQuery();
            this.sessionId = rs.next() ? rs.getInt(1) : -1;
            if (this.sessionId < 0) {
                throw new StreamException("Unable to create CDC session: %s".formatted(Messages.getMessage(this.sessionId)), this.sessionId);
            }
        }
        catch (SQLException e) {
            throw new StreamException("Unable to create CDC session ", e);
        }
    }

    private void watchTable(IfmxWatchedTable table) throws StreamException {
        LOGGER.debug("Starting watch on table [{}]", table);
        setFullRowLogging(table.getDesciptorString(), true);
        startCapture(table);
    }

    private void setFullRowLogging(String tableName, boolean enable) throws StreamException {
        LOGGER.debug("Setting full row logging on [{}] to '{}'", tableName, enable);
        try (PreparedStatement ps = this.connection.prepareStatement("execute function informix.cdc_set_fullrowlogging(?,?)")) {
            ps.setString(1, tableName);
            ps.setInt(2, enable ? 1 : 0);
            ResultSet rs = ps.executeQuery();
            int resultCode = rs.next() ? rs.getInt(1) : -1;
            if (resultCode != 0) {
                throw new StreamException("Unable to set full row logging: %s".formatted(Messages.getMessage(resultCode)), resultCode);
            }
        }
        catch (SQLException e) {
            throw new StreamException("Unable to set full row logging ", e);
        }
    }

    private void startCapture(IfmxWatchedTable table) throws StreamException {
        try (CallableStatement cs = this.connection.prepareCall("SELECT FIRST 1 * FROM %s".formatted(table.getDesciptorString()));
                PreparedStatement ps = this.connection.prepareStatement("execute function informix.cdc_startcapture(?,?,?,?,?)")) {

            if (table.getColumnDescriptorString().equals("*")) {
                LOGGER.debug("Starting column lookup for [{}]", table.getDesciptorString());
                ResultSet rs = cs.executeQuery();
                ResultSetMetaData md = rs.getMetaData();
                String[] columns = new String[md.getColumnCount()];
                for (int i = 1; i <= columns.length; i++) {
                    columns[i - 1] = md.getColumnName(i).trim();
                }
                LOGGER.debug("Dynamically adding to table [{}] columns: {}", table.getDesciptorString(), columns);
                table.columns(columns);
            }

            LOGGER.debug("Starting capture on [{}]", table);
            ps.setInt(1, this.sessionId);
            ps.setLong(2, 0L);
            ps.setString(3, table.getDesciptorString());
            ps.setString(4, table.getColumnDescriptorString());
            ps.setInt(5, table.getLabel());
            ResultSet rs = ps.executeQuery();
            int resultCode = rs.next() ? rs.getInt(1) : -1;
            if (resultCode != 0) {
                throw new StreamException("Unable to start cdc capture: %s".formatted(Messages.getMessage(resultCode)), resultCode);
            }
        }
        catch (SQLException e) {
            throw new StreamException("Unable to start cdc capture ", e);
        }
    }

    private void activateSession() throws StreamException {
        LOGGER.debug("Activating CDC session");
        try (PreparedStatement ps = this.connection.prepareStatement("execute function informix.cdc_activatesess(?,?)")) {
            ps.setInt(1, this.sessionId);
            ps.setLong(2, this.position);
            ResultSet rs = ps.executeQuery();
            int resultCode = rs.next() ? rs.getInt(1) : -1;
            if (resultCode != 0) {
                throw new StreamException("Unable to activate session: %s".formatted(Messages.getMessage(resultCode)), resultCode);
            }
        }
        catch (SQLException e) {
            throw new StreamException("Unable to activate session ", e);
        }
    }

    private void unwatchTable(IfmxWatchedTable table) throws StreamException {
        LOGGER.debug("Ending watch on table [{}]", table);
        endCapture(table);
        if (this.stopLoggingOnClose) {
            setFullRowLogging(table.getDesciptorString(), false);
        }
    }

    private void endCapture(IfmxWatchedTable table) throws StreamException {
        LOGGER.debug("Ending capture on [{}]", table);
        try (PreparedStatement ps = this.connection.prepareStatement("execute function informix.cdc_endcapture(?,?,?)")) {
            ps.setInt(1, this.sessionId);
            ps.setLong(2, 0L);
            ps.setString(3, table.getDesciptorString());
            ResultSet rs = ps.executeQuery();
            int resultCode = rs.next() ? rs.getInt(1) : -1;
            if (resultCode != 0) {
                throw new StreamException("Unable to end cdc capture: %s".formatted(Messages.getMessage(resultCode)), resultCode);
            }
        }
        catch (SQLException e) {
            throw new StreamException("Unable to end cdc capture ", e);
        }
    }

    private void closeSession() throws StreamException {
        LOGGER.debug("Closing CDC session");
        try (PreparedStatement ps = this.connection.prepareStatement("execute function informix.cdc_closesess(?)")) {
            ps.setInt(1, this.sessionId);
            ResultSet rs = ps.executeQuery();
            int resultCode = rs.next() ? rs.getInt(1) : -1;
            if (resultCode != 0) {
                throw new StreamException("Unable to close session: %s".formatted(Messages.getMessage(resultCode)), resultCode);
            }
        }
        catch (SQLException e) {
            throw new StreamException("Unable to close session ", e);
        }
    }

    @Override
    public void close() {
        if (!this.closed) {
            LOGGER.debug("Closing down CDC engine");
            StreamException ex = null;
            try {
                for (IfmxWatchedTable capturedTable : this.watchedTables) {
                    unwatchTable(capturedTable);
                }
                closeSession();
            }
            catch (StreamException se) {
                ex = se;
            }
            finally {
                try {
                    this.connection.close();
                }
                catch (SQLException e) {
                    StreamException se = new StreamException("Could not close main connection", e);
                    if (ex == null) {
                        ex = se;
                    }
                    else {
                        ex.addSuppressed(se);
                    }
                }
                try {
                    this.recordBuilder.close();
                }
                catch (SQLException e) {
                    StreamException se = new StreamException("Could not close record builder", e);
                    if (ex == null) {
                        ex = se;
                    }
                    else {
                        ex.addSuppressed(se);
                    }
                }
                this.closed = true;
            }
            if (ex != null) {
                throw new DebeziumException("Exception caught when closing CDC engine ", ex);
            }
        }
    }

    static class ByteBufferOutputStream extends OutputStream {

        protected final ByteBuffer buffer;

        ByteBufferOutputStream(ByteBuffer buffer) {
            this.buffer = buffer;
        }

        @Override
        public void write(int b) {
            buffer.put((byte) b);
        }

        @Override
        public void write(byte @NonNull [] b) {
            buffer.put(b);
        }

        @Override
        public void write(byte @NonNull [] b, int off, int len) {
            buffer.put(b, off, len);
        }
    }

    public Builder getBuilder() {
        return builder;
    }

    public static class Builder {

        private final DataSource dataSource;
        private int bufferSize;
        private int maxRecords;
        private long position;
        private int timeout;
        private final List<IfmxWatchedTable> watchedTables = new ArrayList<>();
        private boolean stopLoggingOnClose = true;

        protected Builder(DataSource dataSource) {
            this.dataSource = dataSource;
        }

        public DataSource getDataSource() {
            return dataSource;
        }

        public Builder buffer(int bufferSize) {
            this.bufferSize = bufferSize;
            return this;
        }

        public int getBufferSize() {
            return bufferSize;
        }

        public Builder maxRecords(int maxRecords) {
            this.maxRecords = maxRecords;
            return this;
        }

        public int getMaxRecords() {
            return maxRecords;
        }

        public Builder sequenceId(long position) {
            this.position = position;
            return this;
        }

        public long getSequenceId() {
            return position;
        }

        public Builder timeout(int timeout) {
            this.timeout = timeout;
            return this;
        }

        public int getTimeout() {
            return timeout;
        }

        public Builder watchTable(String canonicalTableName, String... columns) {
            return this.watchTable(IfmxTableDescriptor.parse(canonicalTableName), columns);
        }

        public Builder watchTable(IfmxTableDescriptor desc, String... columns) {
            return this.watchTable((new IfmxWatchedTable(desc)).columns(columns));
        }

        public Builder watchTable(IfmxWatchedTable table) {
            this.watchedTables.add(table);
            return this;
        }

        public List<IfmxWatchedTable> getWatchedTables() {
            return watchedTables;
        }

        public Builder stopLoggingOnClose(boolean stopOnClose) {
            this.stopLoggingOnClose = stopOnClose;
            return this;
        }

        public DbzCDCEngine build() throws SQLException {
            return new DbzCDCEngine(this);
        }
    }
}
