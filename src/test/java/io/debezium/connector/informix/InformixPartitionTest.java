/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import io.debezium.connector.common.AbstractPartitionTest;

public class InformixPartitionTest extends AbstractPartitionTest<InformixPartition> {

    @Override
    protected InformixPartition createPartition1() {
        return new InformixPartition("database1");
    }

    @Override
    protected InformixPartition createPartition2() {
        return new InformixPartition("database2");
    }
}
