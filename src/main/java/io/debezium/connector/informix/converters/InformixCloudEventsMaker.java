/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix.converters;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.converters.spi.CloudEventsMaker;
import io.debezium.converters.spi.RecordParser;
import io.debezium.converters.spi.SerializerType;

/**
 * CloudEvents maker for records produced by the Informix connector.
 *
 * @author Chris Cranford, Lars M Johansson
 *
 */
public class InformixCloudEventsMaker extends CloudEventsMaker {

    public InformixCloudEventsMaker(RecordParser parser, SerializerType contentType, String dataSchemaUriBase) {
        super(parser, contentType, dataSchemaUriBase);
    }

    @Override
    public String ceId() {
        return "name:" + recordParser.getMetadata(AbstractSourceInfo.SERVER_NAME_KEY)
                + ";change_lsn:" + recordParser.getMetadata(InformixRecordParser.CHANGE_LSN_KEY)
                + ";commit_lsn:" + recordParser.getMetadata(InformixRecordParser.COMMIT_LSN_KEY);
    }
}
