/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.informix;

import java.util.Objects;

import io.debezium.connector.Nullable;

/**
 * Defines a position of change in the transaction log. The position is defined as a combination of commit LSN
 * and sequence number of the change in the given transaction.
 * The sequence number is monotonically increasing in transaction but it is not guaranteed across multiple
 * transactions so the combination is necessary to get total order.
 *
 * @author Laoflch Luo, Xiaolin Zhang, Lars M Johansson
 *
 */
public class TxLogPosition implements Nullable, Comparable<TxLogPosition> {

    public static final TxLogPosition NULL = new TxLogPosition(Lsn.NULL, Lsn.NULL, -1, Lsn.NULL);

    private final Lsn commitLsn;
    private final Lsn changeLsn;
    private final Integer txId;
    private final Lsn beginLsn;

    public TxLogPosition(Lsn commitLsn, Lsn changeLsn, Integer txId, Lsn beginLsn) {
        this.commitLsn = commitLsn;
        this.changeLsn = changeLsn;
        this.txId = txId;
        this.beginLsn = beginLsn;
    }

    public static TxLogPosition valueOf(Lsn commitLsn) {
        if (commitLsn == null || commitLsn.equals(Lsn.NULL)) {
            return NULL;
        }
        return valueOf(commitLsn, Lsn.valueOf(0x00L));
    }

    public static TxLogPosition valueOf(Lsn commitLsn, Lsn changeLsn) {
        if ((commitLsn == null || commitLsn.equals(Lsn.NULL) && (changeLsn == null || changeLsn.equals(Lsn.NULL)))) {
            return NULL;
        }
        else {
            return valueOf(commitLsn, changeLsn, Lsn.valueOf(0x00L));
        }
    }

    public static TxLogPosition valueOf(Lsn commitLsn, Lsn changeLsn, Lsn beginLsn) {
        if ((commitLsn == null || commitLsn.equals(Lsn.NULL) && (changeLsn == null || changeLsn.equals(Lsn.NULL)))) {
            return NULL;
        }
        else {
            return valueOf(commitLsn, changeLsn, 0x00, beginLsn);
        }
    }

    public static TxLogPosition valueOf(Lsn commitLsn, Lsn changeLsn, Integer txId, Lsn beginLsn) {
        return new TxLogPosition(commitLsn, changeLsn, txId, beginLsn);
    }

    public static TxLogPosition cloneAndSet(TxLogPosition position, Lsn commitLsn, Lsn changeLsn, Integer txId, Lsn beginLsn) {

        return valueOf(
                commitLsn.compareTo(position.commitLsn) > 0 ? commitLsn : position.commitLsn,
                changeLsn.compareTo(position.changeLsn) > 0 ? changeLsn : position.changeLsn,
                txId >= 0 ? txId : position.txId,
                beginLsn.compareTo(position.beginLsn) > 0 ? beginLsn : position.beginLsn);
    }

    public static TxLogPosition clone(TxLogPosition position) {
        return valueOf(position.commitLsn, position.changeLsn, position.txId, position.beginLsn);
    }

    public Lsn getCommitLsn() {
        return commitLsn;
    }

    public Lsn getChangeLsn() {
        return changeLsn;
    }

    public Integer getTxId() {
        return txId;
    }

    public Lsn getBeginLsn() {
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

        return Objects.equals(commitLsn, that.commitLsn)
                && Objects.equals(changeLsn, that.changeLsn)
                && Objects.equals(txId, that.txId)
                && Objects.equals(beginLsn, that.beginLsn);
    }

    @Override
    public int hashCode() {
        return Objects.hash(commitLsn, changeLsn, txId, beginLsn);
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
}
