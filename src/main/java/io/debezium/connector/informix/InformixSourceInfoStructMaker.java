/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.AbstractSourceInfoStructMaker;

public class InformixSourceInfoStructMaker extends AbstractSourceInfoStructMaker<SourceInfo> {

    private Schema schema;

    @Override
    public void init(String connector, String version, CommonConnectorConfig connectorConfig) {
        super.init(connector, version, connectorConfig);
        schema = commonSchemaBuilder()
                .name("io.debezium.connector.informix.Source")
                .field(SourceInfo.SCHEMA_NAME_KEY, Schema.STRING_SCHEMA)
                .field(SourceInfo.TABLE_NAME_KEY, Schema.STRING_SCHEMA)
                .field(SourceInfo.COMMIT_LSN_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(SourceInfo.CHANGE_LSN_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(SourceInfo.TX_ID, Schema.OPTIONAL_STRING_SCHEMA)
                .field(SourceInfo.BEGIN_LSN_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .build();
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public Struct struct(SourceInfo sourceInfo) {
        final Struct ret = super.commonStruct(sourceInfo);

        if (sourceInfo.getTableId() != null) {
            ret.put(SourceInfo.SCHEMA_NAME_KEY, sourceInfo.getTableId().schema()).put(SourceInfo.TABLE_NAME_KEY, sourceInfo.getTableId().table());
        }
        if (sourceInfo.getCommitLsn() != null && sourceInfo.getCommitLsn().isAvailable()) {
            ret.put(SourceInfo.COMMIT_LSN_KEY, sourceInfo.getCommitLsn().toString());
        }
        if (sourceInfo.getChangeLsn() != null && sourceInfo.getChangeLsn().isAvailable()) {
            ret.put(SourceInfo.CHANGE_LSN_KEY, sourceInfo.getChangeLsn().toString());
        }
        if (sourceInfo.getTxId() >= 0x00) {
            ret.put(SourceInfo.TX_ID, sourceInfo.getTxId().toString());
        }
        if (sourceInfo.getBeginLsn() != null && sourceInfo.getBeginLsn().isAvailable()) {
            ret.put(SourceInfo.BEGIN_LSN_KEY, sourceInfo.getBeginLsn().toString());
        }
        return ret;
    }

}
