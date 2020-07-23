package laoflch.debezium.connector.informix.integrtest

import java.sql.{DriverManager, SQLException}
import java.util.Properties

import io.debezium.embedded
import io.debezium.engine.DebeziumEngine
import org.apache.kafka.connect.source.SourceRecord
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

import com.informix.jdbcx.IfxDataSource
import com.informix.stream.api.IfmxStreamRecord
import com.informix.stream.cdc.IfxCDCEngine
import com.informix.stream.cdc.records.IfxCDCOperationRecord
import io.debezium.engine.DebeziumEngine.ChangeConsumer
import io.debezium.config.Configuration
import io.debezium.connector.mysql.MySqlConnector
import io.debezium.embedded.Connect
//import org.bson.assertions.Assertions

import scala.jdk.CollectionConverters
import org.testng.Assert
import org.testng.annotations.Test
import org.scalatest.Assertions

import scala.collection.mutable.ListBuffer
import org.testng.Assert._
import org.testng.annotations.Test



object InformixConnectorTest {

  def main(args: Array[String]): Unit ={
     //testInfromixNative()
    testInformixdz()
    //testInformixJdbc()

  }


  def testMysql(): Unit ={
  val props:Properties = new Properties()
  props.setProperty("name", "engine")
  props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore")
  props.setProperty("connector.class","io.debezium.connector.mysql.MySqlConnector")
  props.setProperty("offset.storage.file.filename", "/tmp/offsets.dat")
  props.setProperty("offset.flush.interval.ms", "60000")
  /* begin connector properties */
  props.setProperty("database.hostname", "192.168.0.213")
  props.setProperty("database.port", "3306")
  props.setProperty("database.user", "laoflch")
  props.setProperty("database.password", "arsenal")
  props.setProperty("database.server.id", "89")
  props.setProperty("database.server.name", "my-app-connector")
  props.setProperty("database.history", "io.debezium.relational.history.FileDatabaseHistory")
  props.setProperty("database.history.file.filename", "/tmp/dbhistory.dat")

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

  val engine = DebeziumEngine.create(classOf[Connect]).using(props).notifying((records: java.util.List[SourceRecord],committer) => {
    //println("1234")
    var r=null
    for ( r <- CollectionConverters.ListHasAsScala(records).asScala ) {
      println( String.format("record[key:%s,value:%s:string:%s]",r.key(),r.value(),r.toString))
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

  def testInformixdz(): Unit ={
    val props:Properties = new Properties()
    props.setProperty("name", "engine")
    props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore")
    props.setProperty("connector.class","laoflch.debezium.connector.informix.InformixConnector")
    props.setProperty("offset.storage.file.filename", "/tmp/offsets.dat")
    props.setProperty("offset.flush.interval.ms", "60000")
    /* begin connector properties */
    props.setProperty("database.server.name", "informix-test")
    //props.setProperty("database.hostname","192.168.0.213")
    props.setProperty("database.hostname","172.17.0.2")
    props.setProperty("database.port", "9998")
    props.setProperty("database.user", "informix")
    props.setProperty("database.password", "informix")
    props.setProperty("database.dbname","test")
    //props.setProperty("database.server.id", "89")
    //props.setProperty("database.server.name", "my-app-connector")
    props.setProperty("database.history", "io.debezium.relational.history.FileDatabaseHistory")
    props.setProperty("database.history.file.filename", "/tmp/dbhistory.dat")

    props.setProperty("message.key.columns","test.informix.customer:customer_num")
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

    val engine = DebeziumEngine.create(classOf[Connect]).using(props).notifying((records: java.util.List[SourceRecord],committer) => {
      //println("1234")
      var r=null
      for ( r <- CollectionConverters.ListHasAsScala(records).asScala ) {
        println( String.format("record[key:%s,value:%s:string:%s]",r.key(),r.value(),r.toString))
      }

      //println(record)
    }).build

    //Executors.newSingleThreadExecutor.execute(engine)
    // Run the engine asynchronously ...
    val executor = Executors.newSingleThreadExecutor
    executor.execute(engine)

    // executor.shutdown()
    println("testInformixdz")


  }

  def testInfromixNative(): Unit ={
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

  def testInformixJdbc(): Unit ={

    val hostIp = "192.168.0.213"
    val dbPort = "9998"
    //val instanceName = "/syscdcv1"
    val instanceName = ""
    val server = "IFM_NET"
    val url = "jdbc:informix-sqli://"+hostIp+":"+dbPort+instanceName+":user=informix;password=informix"
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
    }catch {
      case e:SQLException=>e.printStackTrace()
    }


    println("success");


  }

  class InformixConnectorTest extends Assertions {

    @Test
    def testInformixdz(): Unit = {
      println("test Informix by debezium")

      InformixConnectorTest.testInformixdz()
      Assert.assertTrue(true)
    }
  }



}
