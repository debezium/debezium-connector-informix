/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix.converters;

import java.util.Set;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.converters.recordandmetadata.RecordAndMetadata;
import io.debezium.converters.spi.CloudEventsMaker;
import io.debezium.converters.spi.SerializerType;
import io.debezium.data.Envelope;
import io.debezium.util.Collect;

/**
 * CloudEvents maker for records produced by the Informix connector.
 *
 * @author Chris Cranford, Lars M Johansson
 *
 */
public class InformixCloudEventsMaker extends CloudEventsMaker {

    static final String CHANGE_LSN_KEY = "change_lsn";
    static final String COMMIT_LSN_KEY = "commit_lsn";

    static final Set<String> IFX_SOURCE_FIELDS = Collect.unmodifiableSet(CHANGE_LSN_KEY, COMMIT_LSN_KEY);

    public InformixCloudEventsMaker(RecordAndMetadata recordAndMetadata, SerializerType contentType, String dataSchemaUriBase, String cloudEventsSchemaName) {
        super(recordAndMetadata, contentType, dataSchemaUriBase, cloudEventsSchemaName, Envelope.FieldName.BEFORE, Envelope.FieldName.AFTER);
    }

    @Override
    public String ceId() {
        return "name:" + sourceField(AbstractSourceInfo.SERVER_NAME_KEY)
                + ";change_lsn:" + sourceField(CHANGE_LSN_KEY)
                + ";commit_lsn:" + sourceField(COMMIT_LSN_KEY);
    }

    @Override
    public Set<String> connectorSpecificSourceFields() {
        return IFX_SOURCE_FIELDS;
    }
}
