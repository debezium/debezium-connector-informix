package laoflch.debezium.connector.informix

import com.informix.jdbcx.IfxDataSource
import com.informix.stream.api.IfmxStreamRecord
import com.informix.stream.cdc.IfxCDCEngine
import com.informix.stream.cdc.records.IfxCDCOperationRecord
import io.debezium.embedded.Connect
import io.debezium.engine.DebeziumEngine
import io.netty.buffer.ByteBuf
import org.apache.kafka.connect.source.SourceRecord

import java.nio.charset.Charset
import java.sql.{Connection, DriverManager, SQLException}
import java.util.concurrent.{Executors, TimeUnit}
import java.util.{Date, Properties}
//import org.bson.assertions.Assertions

import io.netty.buffer.Unpooled
import org.scalatest.Assertions
import org.testng.Assert
import org.testng.annotations.Test

import scala.jdk.CollectionConverters


object InformixConnectorTest {

  /*  val  RECORD_KEY_SCHEMA:String =  "{"+
      "\"type\":\"record\","+
      "\"name\":\"Iteblog\","+
      "\"fields\":["+
      "  { \"name\":\"str1\", \"type\":\"string\" },"+
      "  { \"name\":\"str2\", \"type\":\"string\" },"+
      "  { \"name\":\"int1\", \"type\":\"int\" }"+
      "]}"

    val  RECORD_VALUE_SCHEMA:String = "{"+
      "\"type\":\"record\","+
      "\"name\":\"Iteblog\","+
      "\"fields\":["+
      "  { \"name\":\"str1\", \"type\":\"string\" },"+
      "  { \"name\":\"str2\", \"type\":\"string\" },"+
      "  { \"name\":\"int1\", \"type\":\"int\" }"+
      "]}"*/

  def main(args: Array[String]): Unit = {
    // testInfromixNative()

    val strBuffer = Unpooled.buffer()
    testInformixdz(null, strBuffer)

    //testInformixJdbc()
  }


  def testMysql(): Unit = {
    val props: Properties = new Properties()
    props.setProperty("name", "engine")
    props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore")
    props.setProperty("connector.class", "io.debezium.connector.mysql.MySqlConnector")
    props.setProperty("offset.storage.file.filename", "/tmp/informix/offsets.dat")
    props.setProperty("offset.flush.interval.ms", "60000")
    /* begin connector properties */
    props.setProperty("database.hostname", "192.168.0.213")
    props.setProperty("database.port", "3306")
    props.setProperty("database.user", "laoflch")
    props.setProperty("database.password", "arsenal")
    props.setProperty("database.server.id", "89")
    props.setProperty("database.server.name", "my-app-connector")
    props.setProperty("database.history", "io.debezium.relational.history.FileDatabaseHistory")
    props.setProperty("database.history.file.filename", "/tmp/informix/dbhistory.dat")

    //Contoh konfigurasi
    //
    /*    val config = Configuration.create()
    //            /* begin engine properties */
                .`with`("connector.class","io.debezium.connector.mysql.PostgresConnector")
                .`with`("offset.storage",
                        "org.apache.kafka.connect.storage.FileOffsetBackingStore")
                .`with`("offset.storage.file.filename",
                        "/tmp/offsets.dat")
                .`with`("offset.flush.interval.ms", 60000)
                /* begin connector properties */
                .`with`("name", "mysql-connector")
                .`with`("database.hostname", "192.168.0.213")
                .`with`("database.port", 3306)
                .`with`("database.user", "laoflch")
                .`with`("database.password", "arsenal")
                .`with`("database.server.id", "89")
                .with("database.server.name", "my-app-connector")
    .with"database.history", "io.debezium.relational.history.FileDatabaseHistory")
    props.setProperty("database.history.file.filename", "/tmp/dbhistory.dat")
                .build();*/

    // Create the engine with this configuration ...

    val engine = DebeziumEngine.create(classOf[Connect]).using(props).notifying((records: java.util.List[SourceRecord], committer) => {
      //println("1234")
      var r = null
      for (r <- CollectionConverters.ListHasAsScala(records).asScala) {
        println(String.format("record[key:%s,value:%s:string:%s]", r.key(), r.value(), r.toString))
      }

      //println(record)
    }).build

    //Executors.newSingleThreadExecutor.execute(engine)
    // Run the engine asynchronously ...
    val executor = Executors.newSingleThreadExecutor
    executor.execute(engine)

    // executor.shutdown()
    println("asdfasdf")

    //if (engine != null) engine.close()

    // Engine is stopped when the main code is finished
  }

  // def testInformixdz(): Unit ={
  def testInformixdz(props: Properties, strBuf: ByteBuf): Unit = {
    val props: Properties = new Properties()
    props.setProperty("name", "engine")
    props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore")
    props.setProperty("offset.commit.policy", "io.debezium.embedded.spi.OffsetCommitPolicy$PeriodicCommitOffsetPolicy")
    props.setProperty("connector.class", "laoflch.debezium.connector.informix.InformixConnector")
    props.setProperty("offset.storage.file.filename", "/tmp/informix/offsets.dat")
    props.setProperty("offset.flush.interval.ms", "100")  // 60000
    /* begin connector properties */
    props.setProperty("database.server.name", "informix")
    //props.setProperty("database.hostname","192.168.0.213")
    props.setProperty("database.hostname", "172.20.3.242")
    props.setProperty("database.port", "9088")
    props.setProperty("database.user", "informix")
    props.setProperty("database.password", "in4mix")
    props.setProperty("database.dbname", "testdb")
    //props.setProperty("database.server.id", "89")
    //props.setProperty("database.server.name", "my-app-connector")
    props.setProperty("database.history", "io.debezium.relational.history.FileDatabaseHistory")
    props.setProperty("database.history.file.filename", "/tmp/informix/dbhistory.dat")

    props.setProperty("message.key.columns", "test.informix.customer:customer_num")
    //Contoh konfigurasi
    //
    /*    val config = Configuration.create()
    //            /* begin engine properties */
                .`with`("connector.class","io.debezium.connector.mysql.PostgresConnector")
                .`with`("offset.storage",
                        "org.apache.kafka.connect.storage.FileOffsetBackingStore")
                .`with`("offset.storage.file.filename",
                        "/tmp/offsets.dat")
                .`with`("offset.flush.interval.ms", 60000)
                /* begin connector properties */
                .`with`("name", "mysql-connector")
                .`with`("database.hostname", "192.168.0.213")
                .`with`("database.port", 3306)
                .`with`("database.user", "laoflch")
                .`with`("database.password", "arsenal")
                .`with`("database.server.id", "89")
                .with("database.server.name", "my-app-connector")
    .with"database.history", "io.debezium.relational.history.FileDatabaseHistory")
    props.setProperty("database.history.file.filename", "/tmp/dbhistory.dat")
                .build();*/

    // Create the engine with this configuration ...

    val engine = DebeziumEngine.create(classOf[Connect]).using(props).notifying((records: java.util.List[SourceRecord], committer) => {

      var r = null
      for (r <- CollectionConverters.ListHasAsScala(records).asScala) {
        // println(JSON.toJSON(r.value()).toString)

        val str = String.format("record[key:%s,value:%s];", r.key(), r.value())
        println(str)
        strBuf.writeBytes(str.getBytes)

        //println(String.format("record[key:%s,value:%s:string:%s]",r.key(),r.value(),r.toString))
        //println(strBuf.capacity())

        committer.markProcessed(r)
      }


      //println(record)
    }).build

    //Executors.newSingleThreadExecutor.execute(engine)
    // Run the engine asynchronously ...
    val executor = Executors.newSingleThreadExecutor
    executor.execute(engine)

    executor.awaitTermination(1000, TimeUnit.MINUTES)

    executor.shutdown()
    //println("testInformixdz")
  }

  def testInfromixNative(): Unit = {
    val ds = new IfxDataSource("jdbc:informix-sqli://192.168.0.213:9998/syscdcv1:user=informix;password=informix")
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

        def handleRecord(record: IfmxStreamRecord): Boolean = {

          if (record != null) {
            println(record)
          } else {
            return false
          }
          if (record.hasOperationData) println(record.asInstanceOf[IfxCDCOperationRecord].getData)

          true
        }

      } finally if (engine != null) engine.close()
    }


  }

  def testInformixJdbc(): Unit = {

    val hostIp = "192.168.0.213"
    val dbPort = "9998"
    //val instanceName = "/syscdcv1"
    val instanceName = ""
    val server = "IFM_NET"
    val url = "jdbc:informix-sqli://" + hostIp + ":" + dbPort + instanceName + ":user=informix;password=informix"
    println(url)
    val dbUser = "informix"
    val dbPass = "informix"
    try {
      Class.forName("com.informix.jdbc.IfxDriver")
    } catch {
      case e: ClassNotFoundException => e.printStackTrace()
      case e: IllegalAccessException => e.printStackTrace()
      case e: InstantiationException => e.printStackTrace()
    }
    try {
      //val conn= DriverManager.getConnection(url,dbUser,dbPass)
      val conn = DriverManager.getConnection(url)


      conn.createStatement().execute("database syscdcv1")

      val rs = conn.prepareStatement("select * from test:customer").executeQuery()

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
      println("============================")




      // println(rs)
      //println(empCount)
    } catch {
      case e: SQLException => e.printStackTrace()
    }


    println("success");


  }


}

class InformixConnectorTest extends Assertions {


  @Test(groups = Array("integrationtest"))
  def testInformixdz(): Unit = {
    println("test Informix by debezium")

    // val pis = new PipedInputStream
    //val pos = new PipedOutputStream

    val props: Properties = new Properties()
    props.setProperty("name", "engine")
    props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore")
    props.setProperty("connector.class", "laoflch.debezium.connector.informix.InformixConnector")
    props.setProperty("offset.storage.file.filename", "/tmp/informix/offsets.dat")
    props.setProperty("offset.flush.interval.ms", "60000")
    /* begin connector properties */
    props.setProperty("database.server.name", "informix-test")
    //props.setProperty("database.hostname","192.168.0.213")
    props.setProperty("database.hostname", "172.20.3.242")
    props.setProperty("database.port", "9088")
    props.setProperty("database.user", "informix")
    props.setProperty("database.password", "in4mix")
    props.setProperty("database.dbname", "testdb")
    //props.setProperty("database.server.id", "89")
    //props.setProperty("database.server.name", "my-app-connector")
    props.setProperty("database.history", "io.debezium.relational.history.FileDatabaseHistory")
    props.setProperty("database.history.file.filename", "/tmp/informix/dbhistory.dat")

    props.setProperty("message.key.columns", "test.informix.customer:customer_num")

    val strBuffer = Unpooled.buffer()

    //val decoder = Base64.getDecoder
    // val encoder = Base64.getEncoder

    //strBuffer.writeBytes(encoder.encode("testsssssssss".getBytes))

    //println(strBuffer)

    // pos.connect(pis)

    val jdbcCon = initInformixTestData(props)

    InformixConnectorTest.testInformixdz(props, strBuffer)

    //Thread.sleep(20000)



    assertInformixSnapshot(strBuffer)


    assertInformixInsert(jdbcCon, props, strBuffer)

    assertInformixUpdate(jdbcCon, props, strBuffer)

    assertInformixDelete(jdbcCon, props, strBuffer)

    // Assert.assertTrue(true)
  }


  def assertInformixSnapshot(strBuffer: ByteBuf): Unit = {

    // val bs:Array[Byte] = new Array[Byte](1024)

    //strBuffer.readBytes(bs)

    val str = fetchStr(strBuffer, 20000, (new Date).getTime, 0, 100)

    if (str != null && str.length > 0) {

      println(str)
      Assert.assertEquals(matchTestResult(str), true, "InformixSnapshot test failed")

    }

    def matchTestResult(str: String): Boolean = {
      val pattern = "^record\\[key\\:.*\\,value\\:Struct\\{after.*(fname\\=laoflch_test).*\\,source.*\\,op\\=r\\,ts_ms.*\\]\\;$".r

      val out = pattern.matches(str)

      println("InformixSnapshot match result:" + out)
      out

    }

  }

  def assertInformixInsert(jdbcCon: Connection, props: Properties, strBuffer: ByteBuf): Unit = {


    if (jdbcCon == null) {
      return false
    }


    try {
      //val conn= DriverManager.getConnection(url,dbUser,dbPass)

      val sqlStr = "insert into customer (fname) values ('laoflch_test_insert') "

      if (jdbcCon.createStatement().executeUpdate(sqlStr) > 0) {
        println("insert success");
      } else {
        println("insert failed")
        return false
      }
    } catch {
      case e: SQLException => e.printStackTrace(); return false
    }

    val str = fetchStr(strBuffer, 20000, (new Date).getTime, 0, 100)

    if (str != null && str.length > 0) {
      println(str)
      Assert.assertEquals(matchTestResult(str), true, "InformixSnapshot test failed")
    }

    def matchTestResult(str: String): Boolean = {
      val pattern = "^record\\[key\\:.*\\,value\\:Struct\\{after.*(fname\\=laoflch_test_insert).*\\,source.*\\,op\\=c\\,ts_ms.*\\]\\;$".r

      val out = pattern.matches(str)

      println("InformixInsert match result:" + out)
      out

    }


  }


  def assertInformixUpdate(jdbcCon: Connection, props: Properties, strBuffer: ByteBuf): Unit = {

    if (jdbcCon == null) {
      return false
    }

    try {
      //val conn= DriverManager.getConnection(url,dbUser,dbPass)
      val sqlStr = "update customer set fname='laoflch_test_update' where fname='laoflch_test_insert'"

      if (jdbcCon.createStatement().executeUpdate(sqlStr) > 0) {
        println("insert success");
      } else {
        println("insert failed")
        return false
      }

      // println(rs)
      //println(empCount)
    } catch {
      case e: SQLException => e.printStackTrace(); return false
    }

    val str = fetchStr(strBuffer, 20000, (new Date).getTime, 0, 100)

    if (str != null && str.length > 0) {
      println(str)
      Assert.assertEquals(matchTestResult(str), true, "InformixSnapshot test failed")
    }

    def matchTestResult(str: String): Boolean = {
      val pattern = "^record\\[key\\:.*\\,value\\:Struct\\{before.*(fname\\=laoflch_test_insert).*after.*(fname\\=laoflch_test_update).*\\,source.*\\,op\\=u\\,ts_ms.*\\]\\;$".r
      val out = pattern.matches(str)

      println("InformixInsert update match result:" + out)
      out
    }
  }

  def assertInformixDelete(jdbcCon: Connection, props: Properties, strBuffer: ByteBuf): Unit = {

    if (jdbcCon == null) {
      return false
    }

    try {
      //val conn= DriverManager.getConnection(url,dbUser,dbPass)
      val sqlStr = "delete from  customer where fname='laoflch_test_update'"

      if (jdbcCon.createStatement().executeUpdate(sqlStr) > 0) {
        println("delete success");
      } else {
        println("delete failed")
        return false
      }

      // println(rs)
      //println(empCount)
    } catch {
      case e: SQLException => e.printStackTrace(); return false
    }

    val str = fetchStr(strBuffer, 20000, (new Date).getTime, 0, 100)

    if (str != null && str.length > 0) {
      println(str)
      Assert.assertEquals(matchTestResult(str), true, "InformixSnapshot test failed")
    }

    def matchTestResult(str: String): Boolean = {
      val pattern = "^record\\[key\\:.*\\,value\\:Struct\\{before.*(fname\\=laoflch_test_update).*\\,source.*\\,op\\=d\\,ts_ms.*\\]\\;$".r
      val out = pattern.matches(str)

      println("InformixInsert delete match result:" + out)

      out
    }
  }

  def fetchStr(strBuffer: ByteBuf, timeOut: Long, lastTime: Long, lastIndex: Int, timeInterval: Long): String = {
    //val bs:Array[Byte] = new Array[Byte](120)

    //var lastTime = -1l
    //for(){

    val currentIndex = strBuffer.readerIndex()
    val currentTime = (new Date).getTime
    Thread.sleep(timeInterval)
    if (currentIndex > lastIndex) {

      return fetchStr(strBuffer, timeOut, currentTime, currentIndex, timeInterval)

    } else {
      if ((currentTime - lastTime) < timeOut) {

        return fetchStr(strBuffer, timeOut, lastTime, currentIndex, timeInterval)
      } //else{


      //}
    }
    val str = strBuffer.toString(0, strBuffer.readableBytes(), Charset.forName("UTF-8"))
    strBuffer.clear()

    // println(str)
    //Base64.getEncoder.encodeToString(bs)
    str

  }

  def initInformixTestData(props: Properties): Connection = {

    //val hostIp = "192.168.0.213"
    val hostIp = props.getProperty("database.hostname")
    //val dbPort = "9998"
    val dbPort = props.getProperty("database.port")
    //val instanceName = "/syscdcv1"
    val instanceName = ""
    val server = "IFM_NET"
    //val dbUser = "informix"
    //val dbPass = "informix"

    val dbUser = props.getProperty("database.user")
    val dbPass = props.getProperty("database.password")
    val dbDB = props.getProperty("database.dbname")
    val url = "jdbc:informix-sqli://" + hostIp + ":" + dbPort + instanceName + ":user=" + dbUser + ";password=" + dbPass
    println(url)

    try {
      Class.forName("com.informix.jdbc.IfxDriver")
    } catch {
      case e: ClassNotFoundException => e.printStackTrace()
      case e: IllegalAccessException => e.printStackTrace()
      case e: InstantiationException => e.printStackTrace()
    }
    try {
      //val conn= DriverManager.getConnection(url,dbUser,dbPass)
      val conn = DriverManager.getConnection(url)

      conn.createStatement().executeUpdate("database " + dbDB)

      conn.createStatement().execute("CREATE TABLE IF NOT EXISTS customer(customer_num integer, fname varchar(100))")

      val delStr = "delete from customer "
      conn.createStatement().executeUpdate(delStr)
      println("before test delete success");

      val sqlStr = "insert into customer (fname) values ('laoflch_test') "

      if (conn.createStatement().executeUpdate(sqlStr) > 0) {
        println("insert success");

        return conn
      } else {
        println("insert failed")

        return null
      }

      // println(rs)
      //println(empCount)
    } catch {
      case e: SQLException => e.printStackTrace(); return null
    }

    //println("success");
  }
}
