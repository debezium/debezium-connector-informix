package laoflch.debezium.connector.informix

import com.informix.jdbcx.IfxDataSource
import com.informix.stream.api.IfmxStreamRecord
import com.informix.stream.cdc.IfxCDCEngine
import com.informix.stream.impl.IfxStreamException
import io.debezium.relational.TableId
import laoflch.debezium.connector.informix.InformixCDCEngine.{CDCTabeEntry, watchAllTableAndCols}

import java.sql.SQLException
import scala.jdk.CollectionConverters
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

object InformixCDCEngine {

  private val URL_PATTERN: String = "jdbc:informix-sqli://%s:%s/syscdcv1:user=%s;password=%s;"
  // private val URL_PATTERN: String = "jdbc:informix-sqli://%s:%s/syscdcv1:user=%s;password=%s;PROTOCOLTRACE=2;PROTOCOLTRACEFILE=/tmp/dbz_proto_trace.out;TRACE=3;TRACEFILE=/tmp/dbz_trace.out"

  private val LOGGER: Logger = LoggerFactory.getLogger(classOf[InformixCDCEngine])

  def genURLStr(host: String, port: String, dataBase: String, user: String, password: String): String = {
    URL_PATTERN.format(host, port.toString, user, password)
  }

  /*def initDataSource(url :String) : IfxDataSource = {
     new IfxDataSource(url)
  }

  def initEngine(dataSource:IfxDataSource): Unit={

    val builder=new IfxCDCEngine.Builder(dataSource)
    builder.build.init

  }*/

  def watchAllTableAndCols(watchTables: Map[String, CDCTabeEntry], builder: IfxCDCEngine.Builder): Unit = {
    watchTables.foreach(tuple => builder.watchTable(tuple._1, tuple._2.tableCols: _*))
    //foreach  says argument is tuple -> unit, so We can easily do below
    //https://stackoverflow.com/questions/8610776/scala-map-foreach for more info
  }

  def build(host: String, port: String, user: String, dataBase: String, password: String): InformixCDCEngine = {
    new InformixCDCEngine(host, port, user, dataBase, password)
  }

  case class CDCTabeEntry(tableId: TableId, tableCols: Seq[String])
}

class InformixCDCEngine(host: String, port: String, user: String, dataBase: String, password: String) {

  private val LOGGER: Logger = LoggerFactory.getLogger(classOf[InformixCDCEngine])

  var cdcEngine: IfxCDCEngine = null
  var lsn: Long = 0x00
  var timeOut: Int = 0x05
  var hasInit: Boolean = false
  var labelId_tableId_map: Map[Int, TableId] = null

  def init(schema: InformixDatabaseSchema): Unit = {

    val url = InformixCDCEngine.genURLStr(host, port, dataBase, user, password)
    val url_masked = InformixCDCEngine.genURLStr(host, port, dataBase, user, "****")
    this.cdcEngine = this.buildCDCEngine(url, this.lsn, timeOut, schema)

    LOGGER.info("Connecting to Informix URL: {}", url_masked)

    try {
      this.cdcEngine.init()
      hasInit = true
    } catch {
      case e: SQLException =>
        e.printStackTrace()
      case e: IfxStreamException =>
        e.printStackTrace()
    }
  }

  def buildCDCEngine(url: String, lsn: Long, timeOut: Int,
                     schema: InformixDatabaseSchema): IfxCDCEngine = {

    val builder = new IfxCDCEngine.Builder(new IfxDataSource(url))

    LOGGER.info("CDCEngine set LSN = {}", lsn)

    // TODO: Make an parameter 'buffer size' for better performance. Default value is 10240
    builder.buffer(819200)

    /*
     * Watch all tables and build a Map for looking up "label_id"
     */
    val tableIds = CollectionConverters.SetHasAsScala(schema.tableIds()).asScala.toList
    tableIds.foreach(tid => {
      val cols = CollectionConverters.ListHasAsScala(schema.tableFor(tid).columns()).asScala.map(col => col.name()).toList
      val tname = tid.catalog() + ":" + tid.schema() + "." + tid.table()
      builder.watchTable(tname, cols: _*)
    })

    if (lsn > 0) {
      builder.sequenceId(lsn)
    }
    builder.timeout(timeOut)

    for (tbl <- CollectionConverters.ListHasAsScala(builder.getWatchedTables).asScala.toList) {
      LOGGER.info("Watched Tables :: {} ==> {}", tbl, tbl.getColumns)
    }
    this.labelId_tableId_map = tableIds.zipWithIndex.map { case (tid, idx) =>
      // TODO: label_id start from 1?
      (idx + 1, tid)
    }.toMap

    builder.build()
  }

  def record(func: (IfmxStreamRecord) => Boolean): Unit = {
    func(cdcEngine.getRecord)
  }

  def stream(func: (IfmxStreamRecord) => Boolean): Unit = {
    while (func(cdcEngine.getRecord)) {
      //Thread.sleep(1000)
    }
  }

  def setStartLsn(startLsn: Long): Long = {
    lsn = startLsn
    lsn
  }

  def convertLabel2TableId(): Map[Int, TableId] = {

    // LOGGER.info("converLabel2TableId(), tables={}", this.tables)
    LOGGER.info("convertLabel2TableId, LabelId => TableId :: {}", this.labelId_tableId_map)

    /*
    this.tables.map[(Int, TableId)](x => {
      val id = x.getDatabaseName + ":" + x.getNamespace + ":" + x.getTableName
      x.getLabel -> this.tableColsMap(id).tableId
    }).toMap

    */

    this.labelId_tableId_map
  }

  // def setTableColsMap(tableColsMap: Map[String, CDCTabeEntry]): Unit = this.tableColsMap = tableColsMap

}
