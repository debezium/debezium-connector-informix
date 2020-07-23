package laoflch.debezium.connector.informix.integrtest

import java.sql.{DriverManager, PreparedStatement, SQLException}
import java.util
import java.util.Collections

import com.informix.stream.api.IfmxStreamRecord
import io.debezium.connector.db2.{Db2ConnectorConfig, Db2ConnectorTask}
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.source.SourceConnector

import scala.collection.JavaConverters

object InformixConnector {

  /*def main(args: Array[String]): Unit ={

    val hostIp = "127.0.0.1"
    val dbPort = "9998"
    val instanceName = "syscdcv1"
    val server = "IFM_NET"
    val url = "jdbc:informix-sqli://"+hostIp+":"+dbPort+"/"+instanceName+":INFORMIXSERVER="+server+";user=informix;password=informix"
    println(url)
    val dbUser = "informix"
    val dbPass = "informix"
    try {
      Class.forName("com.informix.jdbc.IfxDriver")
    }catch{
      case e:ClassNotFoundException=>e.printStackTrace()
      case e:IllegalAccessException=>e.printStackTrace()
      case e:InstantiationException=>e.printStackTrace()
    }
    try {
      //val conn= DriverManager.getConnection(url,dbUser,dbPass)
      val conn=DriverManager.getConnection(url)



/*
      val rs = conn.prepareStatement("select * from customer").executeQuery()

      val col = rs.getMetaData.getColumnCount
      println("============================")
      while ( {
        rs.next
      }) {
        for (i <- 1 to col) {
          print(rs.getString(i) + "\t")
          //if ((i == 2) && (rs.getString(i).length < 8)) print("\t")
        }
        println("")
      }
      println("============================")*/

      import java.sql.Types
      val sql_cdc_opnsess = "{ execute function cdc_opensess(\"IFM_NET\",0,300,1,1,1) }"
      val sql_cdc_set_fullrowlogging = "{ ? = call cdc_set_fullrowlogging(\"test:informix.customer\",1)}"
      //val cdcOpnsessCallableStatement = conn.prepareCall(sql_cdc_opnsess)

      // 2. 通过 CallableStatement 对象的
      //reisterOutParameter() 方法注册 OUT 参数.
      //callableStatement.registerOutParameter(1, Types.NUMERIC)
      //callableStatement.registerOutParameter(3, Types.NUMERIC)

      // 3. 通过 CallableStatement 对象的 setXxx() 方法设定 IN 或 IN OUT 参数. 若想将参数默认值设为
      // null, 可以使用 setNull() 方法.
      //callableStatement.setInt(2, 80)

      // 4. 通过 CallableStatement 对象的 execute() 方法执行存储过程
      val rs_cdc_opensess=conn.prepareCall(sql_cdc_opnsess).executeQuery()

      // 5. 如果所调用的是带返回参数的存储过程,
      //还需要通过 CallableStatement 对象的 getXxx() 方法获取其返回值.
      //val sumSalary = callableStatement.getDouble(1)
      //val empCount = callableStatement.getLong(1)

      if (rs_cdc_opensess.next){
        //rs.next()
        val sessionId = rs_cdc_opensess.getLong(1)
        println(sessionId)
        val rs_cdc_set_fullrowlogging = conn.prepareCall(sql_cdc_set_fullrowlogging).executeQuery()

        if(rs_cdc_set_fullrowlogging.next() && rs_cdc_set_fullrowlogging.getInt(1) == 0){
          println("set_fullrowlogging successfully")


        }


      }
     // println(rs)
      //println(empCount)
    }catch {
      case e:SQLException=>e.printStackTrace()
    }


    println("success");


  }*/

  import com.informix.jdbcx.IfxDataSource
  import com.informix.stream.api.IfmxStreamRecord
  import com.informix.stream.cdc.IfxCDCEngine
  import com.informix.stream.cdc.records.IfxCDCOperationRecord

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val ds = new IfxDataSource("jdbc:informix-sqli://localhost:9998/syscdcv1:user=informix;password=informix")
    val builder = new IfxCDCEngine.Builder(ds)
    builder.watchTable("test:informix:customer", "customer_num", "fname")
    builder.timeout(10)
    try {
      val engine = builder.build
      try {
        engine.init()

        while ( {
          handleRecord(engine.getRecord)

        }) println()

        def handleRecord(record:IfmxStreamRecord): Boolean ={

          if(record != null) {
            println(record)
          }else {
            return false
          }
          if(record.hasOperationData) println(record.asInstanceOf[IfxCDCOperationRecord].getData)

          true
        }

      } finally if (engine != null) engine.close()
    }
  }
}

class InformixConnector extends SourceConnector {

  private var properties: util.Map[String, String] = null

  override def version: String = {
  return Module.version
}

  override def start (props: util.Map[String, String] ): Unit = {
  this.properties = props
}

  override def taskClass: Class[_ <: Task] = {
  return classOf[InformixConnectorTask]
}

  override def taskConfigs (maxTasks: Int): util.List[util.Map[String, String]] = {
  if (maxTasks > 1) {
  throw new IllegalArgumentException ("Only a single connector task may be started")
  }
  return Collections.singletonList(properties)
}

  override def stop (): Unit = {
}

  override def config: ConfigDef = {
  return InformixConnectorConfig.configDef
}
}

