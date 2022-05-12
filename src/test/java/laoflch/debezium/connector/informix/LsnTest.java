package laoflch.debezium.connector.informix;

import static org.fest.assertions.Assertions.assertThat;

import org.junit.Test;

import java.util.ArrayList;

public class LsnTest {

    @Test
    public void testLsnMinusOne() {
        Lsn recorded = Lsn.valueOf("10:00");
        Lsn desired = Lsn.valueOf("10:46");
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
        int ret = recorded.compareTo(desired);
        assertThat(recorded.compareTo(desired)).isEqualTo(-1);

        Lsn recorded1 = Lsn.valueOf("NULL");
        Lsn desired1 = Lsn.valueOf("146031505564");
        assertThat(recorded1.compareTo(desired1)).isEqualTo(-1);
    }

    @Test
    public void testValueOf() {
        String lsnStr1 = "146039087264";
        String lsnStr2 = "14603908:7264";
        Lsn lsn1 = Lsn.valueOf(lsnStr1);
        Lsn lsn2 = Lsn.valueOf(lsnStr2);
        assertThat(lsn1).isEqualTo(lsn2);
    }

    @Test
    public void testLsnCompare() {
        Lsn lsnUninitialized = new Lsn();
        Lsn lsnNegativeOne = new Lsn(-1L);
        Lsn lsnOne = new Lsn(1L);
        Lsn lsn1 = new Lsn(146039087264L);
        Lsn lsn2 = new Lsn(146039087265L);
        Lsn lsn3 = new Lsn(1460390872640L);
        Lsn lsnMaxVal = new Lsn(Long.MAX_VALUE);
        assertThat(lsnUninitialized.compareTo(lsnNegativeOne)).isEqualTo(-1);
        assertThat(lsnNegativeOne.compareTo(lsnOne)).isEqualTo(-1);
        assertThat(lsnOne.compareTo(lsn1)).isEqualTo(-1);
        assertThat(lsn1.compareTo(lsn2)).isEqualTo(-1);
        assertThat(lsn2.compareTo(lsn3)).isEqualTo(-1);
        assertThat(lsn3.compareTo(lsnMaxVal)).isEqualTo(-1);

        assertThat(lsnNegativeOne.compareTo(lsnUninitialized)).isEqualTo(1);
        assertThat(lsnOne.compareTo(lsnNegativeOne)).isEqualTo(1);
        assertThat(lsn1.compareTo(lsnOne)).isEqualTo(1);
        assertThat(lsn2.compareTo(lsn1)).isEqualTo(1);
        assertThat(lsn3.compareTo(lsn2)).isEqualTo(1);
        assertThat(lsnMaxVal.compareTo(lsn3)).isEqualTo(1);
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

        assertThat(lsnNegativeOne).isEqualTo(lsnNegativeOneStr);
        assertThat(lsnNegativeOne.compareTo(lsnNegativeOneStr)).isEqualTo(0);

        assertThat(lsnDigit1.compareTo(lsnDigit2)).isEqualTo(-1);
        assertThat(lsnDigit2.compareTo(lsnDigit3)).isEqualTo(-1);
        assertThat(lsnDigit3.compareTo(lsnDigit4)).isEqualTo(-1);
        assertThat(lsnDigit4.compareTo(lsnDigit5)).isEqualTo(-1);
        assertThat(lsnDigit5.compareTo(lsnDigit6)).isEqualTo(-1);

        assertThat(lsnDigit2.compareTo(lsnDigit1)).isEqualTo(1);
        assertThat(lsnDigit3.compareTo(lsnDigit2)).isEqualTo(1);
        assertThat(lsnDigit4.compareTo(lsnDigit3)).isEqualTo(1);
        assertThat(lsnDigit5.compareTo(lsnDigit4)).isEqualTo(1);
        assertThat(lsnDigit6.compareTo(lsnDigit5)).isEqualTo(1);
    }
}
