/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import io.debezium.connector.Nullable;

/**
 * A logical representation of LSN (log sequence number) position. When LSN is not available
 * it is replaced with {@link Lsn#NULL} constant.
 *
 * @author Laoflch Luo, Xiaolin Zhang, Lars M Johansson
 *
 */
public class Lsn implements Comparable<Lsn>, Nullable {

    public static final Lsn NULL = new Lsn(-1L);

    private static final long LO_MASK = Long.parseUnsignedLong("00000000ffffffff", 16);
    private static final long HI_MASK = Long.parseUnsignedLong("ffffffff00000000", 16);

    private final Long lsn;

    Lsn(Long lsn) {
        this.lsn = lsn;
    }

    /**
     * @param lsn - signed long integer string. We consider "NULL" and "-1L"
     *            as same as "new Lsn(-1)".
     * @return LSN converted from its textual representation
     */
    public static Lsn valueOf(String lsn) {
        return (lsn == null || lsn.equalsIgnoreCase("NULL")) ? NULL : Lsn.valueOf(Long.parseLong(lsn));

    }

    public static Lsn valueOf(Long lsn) {
        return lsn == null ? NULL : new Lsn(lsn);
    }

    /**
     * @return true if this is a real LSN or false it it is {@code NULL}
     */
    @Override
    public boolean isAvailable() {
        return lsn != null && lsn >= 0;
    }

    /**
     * @return textual representation of the stored LSN
     */
    public String toString() {
        return Long.toString(lsn);
    }

    /**
     * Return the LSN String for an official representation, like "LSN(7,8a209c)". Reference
     * <a href="https://www.oninit.com/manual/informix/117/documentation/ids_cdc_bookmap.pdf">Informix 11.70 CDC API Programmer's Guide</a> page 62 for
     * more details.
     *
     * @return official textual representation of LSN.
     */
    public String toLongString() {
        return String.format("LSN(%d,%x)", loguniq(), logpos());
    }

    /** 32bit position within the current log page */
    public long logpos() {
        return LO_MASK & lsn;
    }

    /** 32bit log page unique identifier */
    public long loguniq() {
        return lsn >> 32;
    }

    public long longValue() {
        return lsn != null ? lsn : -1L;
    }

    @Override
    public int hashCode() {
        return lsn.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return this == obj || obj != null && this.getClass().equals(obj.getClass()) && lsn.equals(((Lsn) obj).lsn);
    }

    /**
     * Enables ordering of LSNs. The {@code NULL} LSN is always the smallest one.
     */
    @Override
    public int compareTo(Lsn o) {
        if (this == o) {
            return 0;
        }
        if (!this.isAvailable()) {
            if (!o.isAvailable()) {
                return 0;
            }
            return -1;
        }
        if (!o.isAvailable()) {
            return 1;
        }
        return lsn.compareTo(o.lsn);
    }

    /**
     * Verifies whether the LSN falls into a LSN interval
     *
     * @param from start of the interval (included)
     * @param to end of the interval (excluded)
     *
     * @return true if the LSN falls into the interval
     */
    public boolean isBetween(Lsn from, Lsn to) {
        return this.compareTo(from) >= 0 && this.compareTo(to) < 0;
    }

    /**
     * Return the next LSN in sequence
     */
    public Lsn increment() {
        return Lsn.valueOf(lsn + 1);
    }
}
