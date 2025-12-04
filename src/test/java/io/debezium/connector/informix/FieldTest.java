/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import org.junit.jupiter.api.BeforeEach;

import io.debezium.config.AbstractFieldTest;

public class FieldTest extends AbstractFieldTest {

    @BeforeEach
    public void before() {
        setAllConnectorFields(InformixConnectorConfig.ALL_FIELDS);
    }

}
