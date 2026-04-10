/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix.stream;

import java.sql.SQLException;

import com.informix.jdbc.stream.api.StreamRecord;
import com.informix.jdbc.stream.impl.StreamException;

@FunctionalInterface
public interface DbzStreamListener {
    void accept(StreamRecord record) throws StreamException, SQLException, InterruptedException;
}
