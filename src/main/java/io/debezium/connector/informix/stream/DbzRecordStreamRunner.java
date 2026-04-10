/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix.stream;

import static java.lang.Thread.currentThread;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.informix.jdbc.stream.api.RecordStream;
import com.informix.jdbc.stream.api.StreamListener;
import com.informix.jdbc.stream.api.StreamRecord;
import com.informix.jdbc.stream.impl.StreamException;

import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;

public class DbzRecordStreamRunner implements RecordStream {
    private static final Logger LOGGER = LoggerFactory.getLogger(DbzRecordStreamRunner.class);

    protected final ChangeEventSourceContext context;
    protected final DbzTransactionEngine engine;
    protected final List<Consumer<StreamRecord>> listeners = new CopyOnWriteArrayList<>();
    protected final List<StreamException> exceptions = new ArrayList<>();
    protected final boolean stopOnError;

    public DbzRecordStreamRunner(ChangeEventSourceContext context, DbzTransactionEngine engine) {
        this(context, engine, true);
    }

    public DbzRecordStreamRunner(ChangeEventSourceContext context, DbzTransactionEngine engine, boolean stopOnError) {
        this.context = context;
        this.engine = engine;
        this.stopOnError = stopOnError;
    }

    @Override
    public void run() {
        try {
            engine.init();
        }
        catch (StreamException e) {
            exceptions.add(e);
            return;
        }

        while (context.isRunning() && !currentThread().isInterrupted() && (!stopOnError || exceptions.isEmpty())) {
            try {
                if (context.isPaused()) {
                    LOGGER.info("Streaming will now pause");
                    context.streamingPaused();
                    context.waitSnapshotCompletion();
                    LOGGER.info("Streaming resumed");
                }
                engine.getRecords().stream().map(engine::processRecord).filter(Objects::nonNull)
                        .forEach(record -> listeners
                                .forEach(listener -> listener.accept(record)));
            }
            catch (SQLException e) {
                exceptions.add(new StreamException("SQL exception caught processing records ", e));
            }
            catch (StreamException e) {
                exceptions.add(e);
            }
            catch (InterruptedException e) {
                LOGGER.error("Caught InterruptedException", e);
                currentThread().interrupt();
            }
        }
    }

    RecordStream addListener(Consumer<StreamRecord> listener) {
        listeners.add(listener);
        return this;
    }

    @Override
    public RecordStream addListener(StreamListener listener) {
        return addListener((Consumer<StreamRecord>) record -> {
            try {
                listener.accept(record);
            }
            catch (SQLException e) {
                exceptions.add(new StreamException("SQL exception occurred in listener [%s] while processing record [%s]".formatted(listener, record), e));
            }
            catch (StreamException e) {
                exceptions.add(e);
            }
        });
    }

    public RecordStream addListener(DbzStreamListener listener) {
        return addListener((Consumer<StreamRecord>) record -> {
            try {
                listener.accept(record);
            }
            catch (SQLException e) {
                exceptions.add(new StreamException("SQL exception occurred in listener [%s] while processing record [%s]".formatted(listener, record), e));
            }
            catch (StreamException e) {
                exceptions.add(e);
            }
            catch (InterruptedException e) {
                LOGGER.error("Caught InterruptedException", e);
                currentThread().interrupt();
            }
        });
    }

    @Override
    public List<StreamException> getExceptions() {
        return exceptions;
    }

    @Override
    public void close() {
        engine.close();
    }
}
