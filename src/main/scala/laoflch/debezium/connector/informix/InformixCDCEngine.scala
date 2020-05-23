package laoflch.debezium.connector.informix



import java.sql.SQLException

import com.informix.jdbcx.IfxDataSource
import com.informix.stream.api.IfmxStreamRecord
import com.informix.stream.cdc.IfxCDCEngine
import com.informix.stream.cdc.records.IfxCDCRecord
import com.informix.stream.impl.IfxStreamException
import io.debezium.relational.TableId
import laoflch.debezium.connector.informix.InformixCDCEngine.CDCTabeEntry
import org.apache.kafka.connect.errors.ConnectException

import scala.collection.mutable
import scala.jdk.CollectionConverters






object InformixCDCEngine {

  private val URL_PATTERN: String = "jdbc:informix-sqli://%s:%s/syscdcv1:user=%s;password=%s"

  case class CDCTabeEntry(tableId:TableId,tableCols:Seq[String])

  def genURLStr(host:String,port:String,dataBase:String,user:String,password:String): String ={
    URL_PATTERN.format(host,port.toString,user,password)
  }

  /*def initDataSource(url :String) : IfxDataSource = {
     new IfxDataSource(url)
  }

  def initEngine(dataSource:IfxDataSource): Unit={

    val builder=new IfxCDCEngine.Builder(dataSource)
    builder.build.init

  }*/

  def watchTableAndCols(watchTables: Map[String,CDCTabeEntry],builder:IfxCDCEngine.Builder): Unit={
    watchTables.foreach(tuple=>builder.watchTable(tuple._1,tuple._2.tableCols:_*))
    //foreach  says argument is tuple -> unit, so We can easily do below
    //https://stackoverflow.com/questions/8610776/scala-map-foreach for more info
  }



  def setTimeout(timeOut: Int,builder:IfxCDCEngine.Builder): Unit ={
    builder.timeout(timeOut)
  }

  def buildCDCEngine(url:String,tableAndCols: Map[String,InformixCDCEngine.CDCTabeEntry], lsn :Long,timeOut:Int): (IfxCDCEngine,List[IfxCDCEngine.IfmxWatchedTable]) = {

    //val ds = new IfxDataSource(url)
    val builder = new IfxCDCEngine.Builder(new IfxDataSource(url))

    watchTableAndCols(tableAndCols,builder)

    builder.sequenceId(lsn)

    setTimeout(timeOut,builder)



    (builder.build,CollectionConverters.ListHasAsScala(builder.getWatchedTables).asScala.toList)

  }

  def build(host: String
            ,port: String
            ,user: String
            ,dataBase: String
            ,password: String): InformixCDCEngine ={




    new InformixCDCEngine(host,port,user,dataBase,password)
  }




}

class InformixCDCEngine(host: String
                        ,port: String
                        ,user: String
                        ,dataBase: String
                        ,password: String) {

  var cdcEngine: IfxCDCEngine = null

  var watchTableAndCols: Map[String, CDCTabeEntry] = null

  var lsn: Long = 0x00

  var timeOut: Int = 0x05

  var hasInit: Boolean = false

  var tables: List[IfxCDCEngine.IfmxWatchedTable] =null

  def init(): Unit ={

    if(watchTableAndCols!=null){

      val url = InformixCDCEngine.genURLStr(host,port,dataBase,user,password)

      val ent = InformixCDCEngine.buildCDCEngine(url,this.watchTableAndCols,this.lsn,timeOut)

      this.cdcEngine=ent._1
      this.tables=ent._2

      try {
        this.cdcEngine.init()
        hasInit=true
      }catch{
        case e: SQLException =>
          e.printStackTrace()
        case e: IfxStreamException =>
          e.printStackTrace()

      }



    }

  }

  def record(func:(IfmxStreamRecord)=>Boolean): Unit ={
    func(cdcEngine.getRecord)

  }

  def stream(func:(IfmxStreamRecord)=>Boolean): Unit ={

    while(func(cdcEngine.getRecord)){
      //Thread.sleep(1000)
    }

  }

  def converLabel2Table(): Map[Int,TableId] ={

   // val tableLabel=Map[Int,TableId]()

    this.tables.map[(Int,TableId)](x=>{
      val id=x.getDatabaseName+":"+x.getNamespace+":"+x.getTableName
      x.getLabel->this.watchTableAndCols(id).tableId
    }).toMap


    //tableLabel

  }

  def setWatchTableAndCols(wtac:Map[String,CDCTabeEntry]): Unit =this.watchTableAndCols=wtac



}
