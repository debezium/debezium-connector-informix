/*
 * Copyright Debezium-Informix-Connector Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.informix;

public class TxLogPosition implements Nullable, Comparable<TxLogPosition> {
    public static TxLogPosition NULL = new TxLogPosition(-1L, -1L, -1L, -1L);
    public static Long LSN_NULL = -1L;

    private final Long commitLsn;
    private final Long changeLsn;
    private final Long txId;
    private final Long beginLsn;

    public TxLogPosition(Long commitLsn, Long changeLsn, Long txId, Long beginLsn) {
        this.commitLsn = commitLsn;
        this.changeLsn = changeLsn;
        this.txId = txId;
        this.beginLsn = beginLsn;
    }

    public Long getCommitLsn() {
        return commitLsn;
    }

    public Long getChangeLsn() {
        return changeLsn;
    }

    public Long getTxId() {
        return txId;
    }

    public Long getBeginLsn() {
        return beginLsn;
    }

    @Override
    public String toString() {
        return this == NULL ? "NULL" : commitLsn + ":" + changeLsn + ":" + txId + ":" + beginLsn;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TxLogPosition that = (TxLogPosition) o;

        if (commitLsn != null ? !commitLsn.equals(that.commitLsn) : that.commitLsn != null) {
            return false;
        }
        if (changeLsn != null ? !changeLsn.equals(that.changeLsn) : that.changeLsn != null) {
            return false;
        }
        if (txId != null ? !txId.equals(that.txId) : that.txId != null) {
            return false;
        }
        return beginLsn != null ? beginLsn.equals(that.beginLsn) : that.beginLsn == null;
    }

    @Override
    public int hashCode() {
        int result = commitLsn != null ? commitLsn.hashCode() : 0;
        result = 31 * result + (changeLsn != null ? changeLsn.hashCode() : 0);
        result = 31 * result + (txId != null ? txId.hashCode() : 0);
        result = 31 * result + (beginLsn != null ? beginLsn.hashCode() : 0);
        return result;
    }

    @Override
    public int compareTo(TxLogPosition o) {
        final int comparison = commitLsn.compareTo(o.getCommitLsn());
        return comparison == 0 ? changeLsn.compareTo(o.changeLsn) : comparison;
    }

    @Override
    public boolean isAvailable() {
        return changeLsn != null && commitLsn != null && beginLsn != null && txId != null;
    }

    public static TxLogPosition valueOf(Long commitLsn) {
        return valueOf(commitLsn, 0x00L);
    }

    public static TxLogPosition valueOf(Long commitLsn, Long changeLsn) {
        if (commitLsn == null && changeLsn == null) {
            return NULL;
        }
        else {
            return new TxLogPosition(commitLsn, changeLsn, 0x00L, 0x00L);
        }
    }

    public static TxLogPosition valueOf(Long commitLsn, Long changeLsn, Long beginLsn) {
        return valueOf(commitLsn, changeLsn, 0x00L, beginLsn);
    }

    public static TxLogPosition valueOf(Long commitLsn, Long changeLsn, Long txId, Long beginLsn) {
        return new TxLogPosition(commitLsn, changeLsn, txId, beginLsn);
    }

    public static TxLogPosition cloneAndSet(TxLogPosition position, Long commitLsn, Long changeLsn, Long txId, Long beginLsn) {
        Long _commitLsn;
        Long _changeLsn;
        Long _txId;
        Long _beginLsn;

        if (commitLsn > LSN_NULL) {
            _commitLsn = commitLsn;
        }
        else {
            _commitLsn = position.getCommitLsn();
        }

        if (changeLsn > LSN_NULL) {
            _changeLsn = changeLsn;
        }
        else {
            _changeLsn = position.getChangeLsn();
        }

        if (txId > LSN_NULL) {
            _txId = txId;
        }
        else {
            _txId = position.getTxId();
        }

        if (beginLsn > LSN_NULL) {
            _beginLsn = beginLsn;
        }
        else {
            _beginLsn = position.getBeginLsn();
        }

        return valueOf(_commitLsn, _changeLsn, _txId, _beginLsn);
    }
}
