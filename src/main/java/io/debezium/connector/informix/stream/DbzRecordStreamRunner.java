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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
    protected final long stallTimeoutMs;
    private volatile long lastActivityNanos;
    private final AtomicBoolean aborted = new AtomicBoolean();

    public DbzRecordStreamRunner(ChangeEventSourceContext context, DbzTransactionEngine engine) {
        this(context, engine, true);
    }

    public DbzRecordStreamRunner(ChangeEventSourceContext context, DbzTransactionEngine engine, boolean stopOnError) {
        this(context, engine, stopOnError, 0L);
    }

    public DbzRecordStreamRunner(ChangeEventSourceContext context, DbzTransactionEngine engine, boolean stopOnError, long stallTimeoutMs) {
        this.context = context;
        this.engine = engine;
        this.stopOnError = stopOnError;
        this.stallTimeoutMs = stallTimeoutMs;
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

        lastActivityNanos = System.nanoTime();
        ScheduledExecutorService watchdog = startStallWatchdog();

        try {
            while (context.isRunning() && !currentThread().isInterrupted() && (!stopOnError || exceptions.isEmpty())) {
                try {
                    List<StreamRecord> records = engine.getRecords();
                    // A successful return (even an empty batch or a heartbeat TIMEOUT record) means the source
                    // connection is alive and responding, so refresh the stall watchdog before processing.
                    lastActivityNanos = System.nanoTime();
                    records.stream().map(engine::processRecord).filter(Objects::nonNull)
                            .forEach(record -> listeners
                                    .forEach(listener -> listener.accept(record)));
                }
                catch (SQLException e) {
                    exceptions.add(new StreamException("SQL exception caught processing records ", e));
                }
                catch (StreamException e) {
                    exceptions.add(e);
                }
            }
        }
        finally {
            stopStallWatchdog(watchdog);
        }
    }

    /**
     * Starts a background watchdog that forces the CDC engine connection closed if no record (not even the
     * periodic {@code cdc.timeout} heartbeat) arrives within {@code stallTimeoutMs}. This breaks the native
     * socket read in {@link DbzCDCEngine#readFromLob()} that would otherwise block forever when the source
     * host disappears without a clean TCP close. Returns {@code null} when the watchdog is disabled
     * ({@code stallTimeoutMs <= 0}).
     */
    private ScheduledExecutorService startStallWatchdog() {
        if (stallTimeoutMs <= 0) {
            return null;
        }
        final long stallNanos = TimeUnit.MILLISECONDS.toNanos(stallTimeoutMs);
        final long checkMs = Math.max(1000L, stallTimeoutMs / 4);
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "informix-cdc-stall-watchdog");
            thread.setDaemon(true);
            return thread;
        });
        executor.scheduleAtFixedRate(() -> {
            if (aborted.get()) {
                return;
            }
            long idleNanos = System.nanoTime() - lastActivityNanos;
            if (idleNanos > stallNanos && aborted.compareAndSet(false, true)) {
                LOGGER.warn("No CDC activity for {} ms (stall threshold {} ms); forcing the streaming connection "
                        + "closed so the connector can reconnect", TimeUnit.NANOSECONDS.toMillis(idleNanos), stallTimeoutMs);
                try {
                    engine.abort();
                }
                catch (Throwable t) {
                    LOGGER.error("Failed to abort stalled CDC engine", t);
                }
            }
        }, checkMs, checkMs, TimeUnit.MILLISECONDS);
        return executor;
    }

    private void stopStallWatchdog(ScheduledExecutorService watchdog) {
        if (watchdog != null) {
            watchdog.shutdownNow();
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
