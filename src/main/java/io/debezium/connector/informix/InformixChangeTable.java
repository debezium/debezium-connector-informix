/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import io.debezium.relational.TableId;

public class InformixChangeTable {

    private static final String CDC_SCHEMA = "syscdcsv1";

    /**
     * A LSN from which the data in the change table are relevant
     */
    private final Lsn startLsn;

    /**
     * A LSN to which the data in the change table are relevant
     */
    private Lsn stopLsn;

    /**
     * The table in the CDC schema that captures changes, suitably quoted for Informix
     */
    private final String captureInstance;

    private int changeTableObjectId;
    private TableId sourceTableId;
    private TableId changeTableId;

    public InformixChangeTable(TableId sourceTableId, String captureInstance, int changeTableObjectId, Lsn startLsn, Lsn stopLsn) {
        this.sourceTableId = sourceTableId;
        this.changeTableObjectId = changeTableObjectId;
        this.startLsn = startLsn;
        this.stopLsn = stopLsn;
        this.captureInstance = captureInstance;
        this.changeTableId = (sourceTableId != null) ? new TableId(InformixChangeTable.CDC_SCHEMA, sourceTableId.schema(), captureInstance) : null;
    }

    public InformixChangeTable(String captureInstance, int changeTableObjectId, Lsn startLsn, Lsn stopLsn) {
        this(null, captureInstance, changeTableObjectId, startLsn, stopLsn);
    }

    public String getCaptureInstance() {
        return captureInstance;
    }

    public Lsn getStartLsn() {
        return startLsn;
    }

    public Lsn getStopLsn() {
        return stopLsn;
    }

    public void setStopLsn(Lsn stopLsn) {
        this.stopLsn = stopLsn;
    }

    public TableId getSourceTableId() {
        return sourceTableId;
    }

    public TableId getChangeTableId() {
        return changeTableId;
    }

    public int getChangeTableObjectId() {
        return changeTableObjectId;
    }

    @Override
    public String toString() {
        return "Capture instance \"" + getCaptureInstance() + "\" [sourceTableId=" + getSourceTableId()
                + ", changeTableId=" + getChangeTableId() + ", startLsn=" + startLsn + ", changeTableObjectId="
                + getChangeTableObjectId() + ", stopLsn=" + stopLsn + "]";
    }
}
