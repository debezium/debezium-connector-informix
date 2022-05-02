package laoflch.debezium.connector.informix;

import io.debezium.util.IoUtil;

import java.util.Properties;

public class Module {
    private static final Properties INFO = IoUtil.loadProperties(Module.class, "laoflch/debezium/connector/informix/build.version");

    public static String version() {
        return INFO.getProperty("version");
    }

    /**
     * @return symbolic name of the connector plugin
     */
    public static String name() {
        return "Informix";
    }

    /**
     * @return context name used in log MDC and JMX metrics
     */
    public static String contextName() {
        return "Informix_Server";
    }
}
