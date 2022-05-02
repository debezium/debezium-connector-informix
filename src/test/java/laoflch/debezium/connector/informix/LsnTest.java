package laoflch.debezium.connector.informix;

import io.debezium.relational.history.HistoryRecordComparator;
import org.testng.annotations.Test;

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
        Lsn recorded = Lsn.NULL();
        Lsn desired = Lsn.valueOf("146031505564");
        int ret = recorded.compareTo(desired);
        System.out.println(ret);

        Lsn recorded1 = Lsn.valueOf("null");
        Lsn desired1 = Lsn.valueOf("146031505564");
        int ret1 = recorded.compareTo(desired);
        System.out.println(ret1);
    }

    @Test
    void testLsnZero() {
        Lsn recorded = Lsn.valueOf("1");
        Lsn desired = Lsn.valueOf("146031505564");
        int ret = recorded.compareTo(desired);
        System.out.println(ret);
    }
}
