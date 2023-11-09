/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import java.time.Instant;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.connector.common.BaseSourceInfo;
import io.debezium.relational.TableId;

/**
 * Coordinates from the database log to establish the relation between the change streamed and the source log position.
 * Maps to {@code source} field in {@code Envelope}.
 *
 * @author Laoflch Luo, Xiaolin Zhang, Lars M Johansson
 *
 */
@NotThreadSafe
public class SourceInfo extends BaseSourceInfo {
    public static final String CHANGE_LSN_KEY = "change_lsn";
    public static final String COMMIT_LSN_KEY = "commit_lsn";
    public static final String BEGIN_LSN_KEY = "begin_lsn";
    public static final String TX_ID_KEY = "txId"; // Schema name mapping collision

    private Lsn commitLsn = Lsn.NULL;
    private Lsn changeLsn = Lsn.NULL;
    private Integer txId = -1;
    private Lsn beginLsn = Lsn.NULL;
    private Instant timestamp;
    private TableId tableId;

    private final InformixConnectorConfig config;

    public SourceInfo(InformixConnectorConfig config) {
        super(config);
        this.config = config;
    }

    public Lsn getCommitLsn() {
        return commitLsn;
    }

    /**
     * @param commitLsn - LSN of the { @code COMMIT} of the transaction whose part the change is
     */
    public void setCommitLsn(Lsn commitLsn) {
        this.commitLsn = commitLsn;
    }

    public Lsn getChangeLsn() {
        return changeLsn;
    }

    /**
     * @param changeLsn - LSN of the change in the database log
     */
    public void setChangeLsn(Lsn changeLsn) {
        this.changeLsn = changeLsn;
    }

    public Integer getTxId() {
        return this.txId;
    }

    /**
     * @param txId - Id of the transaction whose part the change is
     */
    public void setTxId(Integer txId) {
        this.txId = txId;
    }

    public Lsn getBeginLsn() {
        return beginLsn;
    }

    /**
     * @param beginLsn - LSN of the { @code BEGIN} of the transaction whose part the change is
     */
    public void setBeginLsn(Lsn beginLsn) {
        this.beginLsn = beginLsn;
    }

    /**
     * @param timestamp a time at which the transaction commit was executed
     */
    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    public TableId getTableId() {
        return tableId;
    }

    /**
     * @param tableId - source table of the event
     */
    public void setTableId(TableId tableId) {
        this.tableId = tableId;
    }

    @Override
    public String toString() {
        return "SourceInfo [" +
                "serverName=" + serverName() +
                ", timestamp=" + timestamp +
                ", db=" + database() +
                ", snapshot=" + snapshotRecord +
                ", commitLsn=" + commitLsn +
                ", changeLsn=" + changeLsn +
                ", txId=" + txId +
                ", beginLsn=" + beginLsn + "]";
    }

    /**
     * @return timestamp of the event
     */
    @Override
    protected Instant timestamp() {
        return timestamp;
    }

    /**
     * @return name of the database
     */
    @Override
    protected String database() {
        return config.getDatabaseName();
    }
}
