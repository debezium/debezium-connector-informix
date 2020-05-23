package laoflch.debezium.connector.informix



import java.util.Properties
import io.debezium.util.IoUtil


object Module {
    private val INFO = IoUtil.loadProperties(classOf[Module], "laoflch/debezium/connector/informix/build.version")

    def version: String = {INFO.getProperty("version")}

    /**
     * @return symbolic name of the connector plugin
     */
    def name:String = {"Informix"}

    /**
     * @return context name used in log MDC and JMX metrics
     */
    def contextName:String = {"Informix_Server"}
  }

class Module {}



