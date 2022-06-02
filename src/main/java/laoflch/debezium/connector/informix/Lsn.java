/*
 * Copyright Debezium-Informix-Connector Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package laoflch.debezium.connector.informix;

import java.util.Objects;

/**
 * A logical representation of LSN (log sequence number) position. When LSN is not available
 * it is replaced with {@link Lsn#NULL} constant.
 *
 * @author laoflch Luo, Xiaolin Zhang
 *
 */
public class Lsn implements Comparable<Lsn>, Nullable {
    private static final String NULL_STRING = "NULL";
    private static final String NEGATIVE_ONE = "-1";

    public static final Lsn NULL = new Lsn(null);

    private static long LO_MASK = Long.parseUnsignedLong("ffffffff", 16);
    private static long HI_MASK = Long.parseUnsignedLong("ffffffff00000000", 16);

    private Long lsn;
    private boolean isInitialized;

    public Lsn() {
        lsn = -2L;
        isInitialized = false;
    }

    public Lsn(Long lsn) {
        this.lsn = lsn == null ? -1 : lsn;
        this.isInitialized = true;
    }

    /**
     * @return true if this is a real LSN or false it it is {@code NULL}
     */
    @Override
    public boolean isAvailable() {
        return isInitialized;
    }

    /**
     * @return textual representation of the stored LSN
     */
    public String toString() {
        return Long.toString(lsn);
    }

    /**
     * Return the LSN String for an official representation, like "LSN(7:0x8a209c)".
     * @return official textual representation of LSN.
     */
    public String toLongString() {
        long lo = LO_MASK & lsn;
        long hi = lsn >> 32;
        return String.format("LSN(%d:0x%x)", hi, lo);
    }

    /**
     * @param lsnString - signed long integer string. We consider "NULL" and "-1L"
     *                  as same as "new Lsn(-1)".
     * @return LSN converted from its textual representation
     */
    public static Lsn valueOf(String lsnString) {
        if (lsnString == null || Objects.equals(lsnString.toUpperCase(), Lsn.NULL_STRING)) {
            return Lsn.valueOf(-1L);
        }

        return Lsn.valueOf(Long.parseLong(lsnString));
    }

    public static Lsn valueOf(Long val) {
        return new Lsn(val);
    }

    @Override
    public int hashCode() {
        return lsn.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        Lsn other = (Lsn) obj;
        return lsn.equals(other.lsn);
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
        lsn = lsn + 1;
        return Lsn.valueOf(lsn);
    }
}
