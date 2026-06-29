/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;

public class InformixConnectorConfigTest {

    @Test
    public void shouldValidateWithStallWatchdogDisabledByDefault() {
        final List<String> problems = new ArrayList<>();
        final InformixConnectorConfig connectorConfig = new InformixConnectorConfig(defaultConfig().build());

        assertThat(connectorConfig.validateAndRecord(List.of(InformixConnectorConfig.CDC_STALL_TIMEOUT_MS), problems::add)).isTrue();
        assertThat(problems).isEmpty();
    }

    @Test
    public void shouldValidateStallTimeoutLargerThanCdcTimeout() {
        final List<String> problems = new ArrayList<>();
        final InformixConnectorConfig connectorConfig = new InformixConnectorConfig(defaultConfig()
                .with(InformixConnectorConfig.CDC_TIMEOUT, 5)
                .with(InformixConnectorConfig.CDC_STALL_TIMEOUT_MS, 15_000)
                .build());

        assertThat(connectorConfig.validateAndRecord(List.of(InformixConnectorConfig.CDC_STALL_TIMEOUT_MS), problems::add)).isTrue();
        assertThat(problems).isEmpty();
    }

    @Test
    public void shouldNotValidateStallTimeoutSmallerThanCdcTimeout() {
        final List<String> problems = new ArrayList<>();
        final InformixConnectorConfig connectorConfig = new InformixConnectorConfig(defaultConfig()
                .with(InformixConnectorConfig.CDC_STALL_TIMEOUT_MS, 3_000)
                .build());

        assertThat(connectorConfig.validateAndRecord(List.of(InformixConnectorConfig.CDC_STALL_TIMEOUT_MS), problems::add)).isFalse();
        assertThat(problems).hasSize(1);
        assertThat(problems.get(0)).contains("Must be larger than 'cdc.timeout'");
    }

    @Test
    public void shouldNotValidateStallTimeoutEqualToCdcTimeout() {
        final List<String> problems = new ArrayList<>();
        final InformixConnectorConfig connectorConfig = new InformixConnectorConfig(defaultConfig()
                .with(InformixConnectorConfig.CDC_TIMEOUT, 5)
                .with(InformixConnectorConfig.CDC_STALL_TIMEOUT_MS, 5_000)
                .build());

        assertThat(connectorConfig.validateAndRecord(List.of(InformixConnectorConfig.CDC_STALL_TIMEOUT_MS), problems::add)).isFalse();
        assertThat(problems).hasSize(1);
        assertThat(problems.get(0)).contains("Must be larger than 'cdc.timeout'");
    }

    @Test
    public void shouldNotValidateStallTimeoutWithoutHeartbeat() {
        final List<String> problems = new ArrayList<>();
        final InformixConnectorConfig connectorConfig = new InformixConnectorConfig(defaultConfig()
                .with(InformixConnectorConfig.CDC_TIMEOUT, 0)
                .with(InformixConnectorConfig.CDC_STALL_TIMEOUT_MS, 60_000)
                .build());

        assertThat(connectorConfig.validateAndRecord(List.of(InformixConnectorConfig.CDC_STALL_TIMEOUT_MS), problems::add)).isFalse();
        assertThat(problems).hasSize(1);
        assertThat(problems.get(0)).contains("requires 'cdc.timeout' > 0");
    }

    @Test
    public void shouldNotValidateNegativeStallTimeout() {
        final List<String> problems = new ArrayList<>();
        final InformixConnectorConfig connectorConfig = new InformixConnectorConfig(defaultConfig()
                .with(InformixConnectorConfig.CDC_STALL_TIMEOUT_MS, -1)
                .build());

        assertThat(connectorConfig.validateAndRecord(List.of(InformixConnectorConfig.CDC_STALL_TIMEOUT_MS), problems::add)).isFalse();
    }

    private Configuration.Builder defaultConfig() {
        return Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "server")
                .with(InformixConnectorConfig.HOSTNAME, "localhost")
                .with(InformixConnectorConfig.PORT, 9088)
                .with(InformixConnectorConfig.USER, "informix")
                .with(InformixConnectorConfig.PASSWORD, "secret")
                .with(InformixConnectorConfig.DATABASE_NAME, "testdb");
    }

}
