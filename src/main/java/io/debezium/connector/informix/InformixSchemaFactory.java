/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import io.debezium.schema.SchemaFactory;

public class InformixSchemaFactory extends SchemaFactory {

    private static final InformixSchemaFactory SCHEMA_FACTORY = new InformixSchemaFactory();

    public InformixSchemaFactory() {
        super();
    }

    public static InformixSchemaFactory get() {
        return SCHEMA_FACTORY;
    }
}
