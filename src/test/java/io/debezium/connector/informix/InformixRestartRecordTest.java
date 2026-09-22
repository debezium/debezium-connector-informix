/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.data.Envelope;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;

class InformixRestartRecordTest extends AbstractAsyncEngineConnectorTest {

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void shouldAcceptExpectedSnapshotOrStreamingRecord(boolean snapshot) throws Exception {
        final int id = snapshot ? 1 : -1;
        final SourceRecords records = receive(record("tablea", id, id, snapshot));
        InformixConnectorIT.assertRestartRecord(records, id, snapshot);
    }

    @Test
    void shouldRejectMissingMarkerBeforeLateDelivery() throws Exception {
        setConsumeTimeout(1, TimeUnit.MILLISECONDS);
        final SourceRecords records = consumeRecordsByTopic(1, 1);
        assertThatThrownBy(() -> InformixConnectorIT.assertRestartRecord(records, -1, false))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("tablea id=-1");

        consumedLines.add(record("tablea", -1, -1, false));
        assertThat(records.allRecordsInOrder()).isEmpty();
        InformixConnectorIT.assertRestartRecord(consumeRecordsByTopic(1), -1, false);
    }

    @Test
    void shouldRejectUnexpectedTopic() throws Exception {
        final SourceRecords records = receive(record("tableb", -1, -1, false));
        assertThatThrownBy(() -> InformixConnectorIT.assertRestartRecord(records, -1, false)).isInstanceOf(AssertionError.class);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void shouldRejectUnexpectedOperation(boolean snapshot) throws Exception {
        final SourceRecords records = receive(record("tablea", -1, -1, snapshot));
        assertThatThrownBy(() -> InformixConnectorIT.assertRestartRecord(records, -1, !snapshot)).isInstanceOf(AssertionError.class);
    }

    @Test
    void shouldRejectUnexpectedKey() throws Exception {
        final SourceRecords records = receive(record("tablea", 200, -1, false));
        assertThatThrownBy(() -> InformixConnectorIT.assertRestartRecord(records, -1, false)).isInstanceOf(AssertionError.class);
    }

    @Test
    void shouldRejectUnexpectedRowId() throws Exception {
        final SourceRecords records = receive(record("tablea", -1, 200, false));
        assertThatThrownBy(() -> InformixConnectorIT.assertRestartRecord(records, -1, false)).isInstanceOf(AssertionError.class);
    }

    private SourceRecords receive(SourceRecord record) throws InterruptedException {
        consumedLines.add(record);
        return consumeRecordsByTopic(1);
    }

    private SourceRecord record(String table, int keyId, int rowId, boolean snapshot) {
        final var keySchema = SchemaBuilder.struct().name("testdb.informix." + table + ".Key").field("id", Schema.INT32_SCHEMA).build();
        final var rowSchema = SchemaBuilder.struct().name("testdb.informix." + table + ".Value").optional().field("id", Schema.INT32_SCHEMA).build();
        final var sourceSchema = SchemaBuilder.struct().name("io.debezium.connector.informix.Source").build();
        final var envelope = Envelope.defineSchema().withName("testdb.informix." + table + ".Envelope")
                .withRecord(rowSchema).withSource(sourceSchema).build();
        final var row = new Struct(rowSchema).put("id", rowId);
        final var source = new Struct(sourceSchema);
        final var value = snapshot ? envelope.read(row, source, Instant.EPOCH) : envelope.create(row, source, Instant.EPOCH);
        return new SourceRecord(Map.of("databaseName", "testdb"), Map.of("change_lsn", 1), "testdb.informix." + table,
                keySchema, new Struct(keySchema).put("id", keyId), value.schema(), value);
    }
}
