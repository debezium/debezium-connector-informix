/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class LsnTest {

    @Test
    public void testLsnMinusOne() {
        Lsn recorded = Lsn.valueOf("1000");
        Lsn desired = Lsn.valueOf("1046");
        int ret = recorded.compareTo(desired);
        System.out.println(ret);
        // assertThat(ret).isEqualTo(-1);
        System.out.println(146031505564L);
        System.out.println(Long.MAX_VALUE);
        Long l1 = 0L;
        Long l2 = 146031505564L;
        System.out.println(Long.toHexString(l1));
        System.out.println(Long.toHexString(l2));
    }

    @Test
    public void testLsnNULL() {
        Lsn recorded = Lsn.NULL;
        Lsn desired = Lsn.valueOf("146031505564");
        assertThat(recorded).isLessThan(desired);

        Lsn recorded1 = Lsn.valueOf("NULL");
        Lsn desired1 = Lsn.valueOf("146031505564");
        assertThat(recorded1).isLessThan(desired1);
    }

    @Test
    public void testValueOf() {
        String lsnStr = "146039087264";
        Lsn lsn1 = Lsn.valueOf(lsnStr);
        Lsn lsn2 = Lsn.valueOf(146039087264L);
        Lsn lsn3 = new Lsn(146039087264L);

        assertThat(lsn1).isEqualTo(lsn2);
        assertThat(lsn2).isEqualTo(lsn3);
    }

    @Test
    public void testLsnCompare() {
        Lsn lsnNegativeOne = new Lsn(-1L);
        Lsn lsnOne = new Lsn(1L);
        Lsn lsn1 = new Lsn(146039087264L);
        Lsn lsn2 = new Lsn(146039087265L);
        Lsn lsn3 = new Lsn(1460390872640L);
        Lsn lsnMaxVal = new Lsn(Long.MAX_VALUE);

        assertThat(lsnNegativeOne).isLessThan(lsnOne);
        assertThat(lsnOne).isLessThan(lsn1);
        assertThat(lsn1).isLessThan(lsn2);
        assertThat(lsn2).isLessThan(lsn3);
        assertThat(lsn3).isLessThan(lsnMaxVal);

        assertThat(lsnOne).isGreaterThan(lsnNegativeOne);
        assertThat(lsn1).isGreaterThan(lsnOne);
        assertThat(lsn2).isGreaterThan(lsn1);
        assertThat(lsn3).isGreaterThan(lsn2);
        assertThat(lsnMaxVal).isGreaterThan(lsn3);
    }

    @Test
    public void testLsnValueOf() {
        Lsn lsnNegativeOne = Lsn.valueOf(-1L);
        Lsn lsnNegativeOneStr = Lsn.valueOf("-1");
        Lsn lsnDigit1 = Lsn.valueOf("1");
        Lsn lsnDigit2 = Lsn.valueOf("12");
        Lsn lsnDigit3 = Lsn.valueOf("123");
        Lsn lsnDigit4 = Lsn.valueOf("1234");
        Lsn lsnDigit5 = Lsn.valueOf("12345");
        Lsn lsnDigit6 = Lsn.valueOf("123456");

        assertThat(lsnNegativeOne).isEqualTo(lsnNegativeOneStr).isEqualByComparingTo(lsnNegativeOneStr);

        assertThat(lsnDigit1).isLessThan(lsnDigit2);
        assertThat(lsnDigit2).isLessThan(lsnDigit3);
        assertThat(lsnDigit3).isLessThan(lsnDigit4);
        assertThat(lsnDigit4).isLessThan(lsnDigit5);
        assertThat(lsnDigit5).isLessThan(lsnDigit6);

        assertThat(lsnDigit2).isGreaterThan(lsnDigit1);
        assertThat(lsnDigit3).isGreaterThan(lsnDigit2);
        assertThat(lsnDigit4).isGreaterThan(lsnDigit3);
        assertThat(lsnDigit5).isGreaterThan(lsnDigit4);
        assertThat(lsnDigit6).isGreaterThan(lsnDigit5);
    }

    @Test
    public void testLsnLongString() {
        Lsn lsn = Lsn.valueOf("30073823388");

        assertThat(lsn.toLongString()).isEqualTo("LSN(7:0x8a209c)");
    }
}
