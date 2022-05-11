package laoflch.debezium.connector.informix;

import static org.fest.assertions.Assertions.assertThat;

import org.junit.Test;

public class LsnTest {

    @Test
    void testLsnMinusOne() {
        Lsn recorded = Lsn.valueOf("-1");
        Lsn desired = Lsn.valueOf("146031505564");
        int ret = recorded.compareTo(desired);
        System.out.println(ret);
    }

    @Test
    void testLsnNULL() {
        Lsn recorded = Lsn.NULL;
        Lsn desired = Lsn.valueOf("146031505564");
        int ret = recorded.compareTo(desired);
        assertThat(recorded.compareTo(desired)).isEqualTo(-1);

        Lsn recorded1 = Lsn.valueOf("NULL");
        Lsn desired1 = Lsn.valueOf("146031505564");
        assertThat(recorded1.compareTo(desired1)).isEqualTo(-1);
    }

    @Test
    void testValueOf() {
        String lsnStr1 = "146039087264";
        String lsnStr2 = "14603908:7264";
        Lsn lsn1 = Lsn.valueOf(lsnStr1);
        Lsn lsn2 = Lsn.valueOf(lsnStr2);
        assertThat(lsn1).isEqualTo(lsn2);
    }
}
