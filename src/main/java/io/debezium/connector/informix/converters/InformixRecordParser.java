/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix.converters;

import java.util.Set;

import org.apache.kafka.connect.errors.DataException;

import io.debezium.converters.recordandmetadata.RecordAndMetadata;
import io.debezium.converters.spi.RecordParser;
import io.debezium.data.Envelope.FieldName;
import io.debezium.util.Collect;

/**
 * Parser for records produced by the Informix connector.
 *
 * @author Chris Cranford, Lars M Johansson
 *
 */
public class InformixRecordParser extends RecordParser {

    static final String CHANGE_LSN_KEY = "change_lsn";
    static final String COMMIT_LSN_KEY = "commit_lsn";

    static final Set<String> IFX_SOURCE_FIELD = Collect.unmodifiableSet(CHANGE_LSN_KEY, COMMIT_LSN_KEY);

    public InformixRecordParser(RecordAndMetadata recordAndMetadata) {
        super(recordAndMetadata, FieldName.BEFORE, FieldName.AFTER);
    }

    @Override
    public Object getMetadata(String name) {
        if (SOURCE_FIELDS.contains(name)) {
            return source().get(name);
        }
        if (IFX_SOURCE_FIELD.contains(name)) {
            return source().get(name);
        }

        throw new DataException("No such field \"" + name + "\" in the \"source\" field of events from Informix connector");
    }
}
