/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.ConnectException;
import java.sql.SQLException;

import org.junit.jupiter.api.Test;

import com.informix.asf.IfxASFException;
import com.informix.stream.impl.IfxStreamException;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.base.DefaultQueueProvider;
import io.debezium.pipeline.DataChangeEvent;

public class InformixErrorHandlerTest {

    private final InformixErrorHandler errorHandler = new InformixErrorHandler(
            new InformixConnectorConfig(Configuration.create()
                    .with(CommonConnectorConfig.TOPIC_PREFIX, "informix")
                    .build()),
            new ChangeEventQueue.Builder<DataChangeEvent>().queueProvider(new DefaultQueueProvider<>(1000)).build(), null);

    @Test
    public void communicationExceptionIsRetryable() {
        assertThat(errorHandler.isRetriable(new ConnectException())).isTrue();
    }

    @Test
    public void nonCommunicationExceptionIsNotRetryable() {
        assertThat(errorHandler.isRetriable(new NullPointerException())).isFalse();
    }

    @Test
    public void encapsulatedCommunicationExceptionIsRetryable() {
        ConnectException testException = new ConnectException("Connection refused");
        assertThat(errorHandler.isRetriable(new IfxASFException("Socket connection to server failed", testException))).isTrue();
    }

    @Test
    public void deepEncapsulatedCommunicationExceptionIsRetryable() {
        Exception testException = new ConnectException("Connection refused");
        testException = new IfxASFException("Socket connection to server failed", testException);
        testException = new SQLException(testException);
        testException = new DebeziumException("Couldn't obtain database name", testException);
        assertThat(errorHandler.isRetriable(testException)).isTrue();
    }

    @Test
    public void sqlExceptionIsRetriable() {
        assertThat(errorHandler.isRetriable(new SQLException("definitely not a informix error"))).isTrue();
    }

    @Test
    public void encapsulatedSQLExceptionIsRetriable() {
        SQLException sqlException = new SQLException("definitely not a informix error");
        assertThat(errorHandler.isRetriable(new IllegalArgumentException(sqlException))).isTrue();
    }

    @Test
    public void ifxStreamExceptionIsNotRetryable() {
        assertThat(errorHandler.isRetriable(new IfxStreamException("Unable to create CDC session. Error code: -83713"))).isFalse();
    }

    @Test
    public void ifxEncapsulatedSQLExceptionIsRetriable() {
        SQLException sqlException = new SQLException("definitely not a informix error");
        assertThat(errorHandler.isRetriable(new IfxStreamException("Unable to create CDC session ", sqlException))).isTrue();
    }

    @Test
    public void randomUnhandledExceptionIsNotRetryable() {
        assertThat(errorHandler.isRetriable(new RuntimeException())).isFalse();
    }

    @Test
    public void nullThrowableIsNotRetryable() {
        assertThat(errorHandler.isRetriable(null)).isFalse();
    }
}
