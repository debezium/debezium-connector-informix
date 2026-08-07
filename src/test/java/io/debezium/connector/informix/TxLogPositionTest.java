/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;

/**
 * Regression tests for a bug where a transaction's own {@code changeLsn} gets stuck reporting a
 * leftover value left behind by a different, concurrently-committed transaction that was
 * processed earlier.
 *
 * <p>Scenario: transaction A opens first and stays open while transaction B opens later and
 * commits first, then A commits. A's own true log positions are all smaller than B's (A's
 * operations were logged before B even started), but B is processed first (transactions are
 * processed in commit order), bumping {@code changeLsn} up to B's own commit LSN as it finishes.
 * {@link TxLogPosition#cloneAndSet} enforces monotonicity on {@code changeLsn} with no notion of
 * transaction boundaries, so once B's bump lands, it becomes a floor that A's own smaller values
 * can never get back below — every one of A's records whose own position is smaller than that
 * floor gets silently reported as the floor value instead of its own true position.
 */
public class TxLogPositionTest {

    @Test
    @FixFor("dbz#2336")
    public void changeLsnIsMonotonicWithinSameTransaction() {
        TxLogPosition position = TxLogPosition.valueOf(Lsn.of(100L), Lsn.of(10L), 1, Lsn.of(5L));

        position = TxLogPosition.cloneAndSet(position, Lsn.NULL, Lsn.of(20L), 1, Lsn.NULL);
        assertThat(position.getChangeLsn()).isEqualTo(Lsn.of(20L));

        position = TxLogPosition.cloneAndSet(position, Lsn.NULL, Lsn.of(30L), 1, Lsn.NULL);
        assertThat(position.getChangeLsn()).isEqualTo(Lsn.of(30L));

        // A smaller value within the same transaction is still rejected — real log positions
        // never actually go backwards within one transaction, but this is a harmless safety net.
        position = TxLogPosition.cloneAndSet(position, Lsn.NULL, Lsn.of(25L), 1, Lsn.NULL);
        assertThat(position.getChangeLsn()).isEqualTo(Lsn.of(30L));
    }

    @Test
    @FixFor("dbz#2336")
    public void commitLsnRemainsMonotonicAcrossTransactions() {
        // commitLsn must still only move forward — it's only ever set once per transaction to
        // that transaction's own commit LSN, and transactions are processed in commit order, so
        // this is already correct and must not regress.
        TxLogPosition position = TxLogPosition.valueOf(Lsn.of(100L), Lsn.of(100L), 2, Lsn.of(90L));

        position = TxLogPosition.cloneAndSet(position, Lsn.of(50L), Lsn.of(10L), 1, Lsn.of(5L));

        assertThat(position.getCommitLsn())
                .as("commitLsn must never regress, even for a new transaction")
                .isEqualTo(Lsn.of(100L));
    }

    @Test
    @FixFor("dbz#2336")
    public void changeLsnOfNewTransactionIsNotStuckBehindPreviousTransactionsLeftover() {
        // Transaction B (txId=2) opens later than A but commits first. Its own operations
        // (changeLsn 200, 210) are followed by its own commit bump to 220 — this mirrors
        // InformixStreamingChangeEventSource bumping changeLsn to the transaction's own
        // commit_lsn/endSeq once it finishes.
        TxLogPosition position = TxLogPosition.valueOf(Lsn.of(50L), Lsn.of(50L), -1, Lsn.of(50L));
        position = TxLogPosition.cloneAndSet(position, Lsn.NULL, Lsn.of(200L), 2, Lsn.NULL);
        position = TxLogPosition.cloneAndSet(position, Lsn.NULL, Lsn.of(210L), 2, Lsn.NULL);
        position = TxLogPosition.cloneAndSet(position, Lsn.of(220L), Lsn.of(220L), 2, Lsn.of(190L));
        assertThat(position.getChangeLsn()).isEqualTo(Lsn.of(220L));

        // Transaction A (txId=1) is processed next. Its own operations were logged before B even
        // started, so its true positions (100, 110) are smaller than B's leftover changeLsn
        // (220). Each should be reported as its own true value, not clamped to 220.
        position = TxLogPosition.cloneAndSet(position, Lsn.of(999L), Lsn.of(100L), 1, Lsn.of(90L));
        assertThat(position.getChangeLsn())
                .as("first record of a new transaction must not inherit the previous transaction's leftover changeLsn")
                .isEqualTo(Lsn.of(100L));

        position = TxLogPosition.cloneAndSet(position, Lsn.NULL, Lsn.of(110L), 1, Lsn.NULL);
        assertThat(position.getChangeLsn())
                .as("subsequent records of the new transaction should keep reporting their own true value")
                .isEqualTo(Lsn.of(110L));
    }
}
