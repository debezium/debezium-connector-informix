package laoflch.debezium.connector.informix

import io.debezium.config.Configuration
import io.debezium.jdbc.{JdbcConfiguration, JdbcConnection}
import io.debezium.relational.{Table, TableId}
import org.slf4j.LoggerFactory

import java.sql.{ResultSet, SQLException}
import java.time.Instant
import java.util

object InformixConnection {

  private val GET_DATABASE_NAME = "SELECT CURRENT_SERVER FROM SYSIBM.SYSDUMMY1" // DB2

  //private val JDBC_URL = "jdbc:informix-sqli://{}:{}/{}:user={};password={}}"

  private val LOGGER = LoggerFactory.getLogger(classOf[InformixConnector])


  // No new Tabels 1=0

  private val URL_PATTERN = "jdbc:informix-sqli://${" +
    JdbcConfiguration.HOSTNAME + "}:${" +
    JdbcConfiguration.PORT + "}/${" +
    JdbcConfiguration.DATABASE +
    "}:user=${" + JdbcConfiguration.USER +
    "};password=${" + JdbcConfiguration.PASSWORD + "}"

  private val FACTORY = JdbcConnection.patternBasedFactory(URL_PATTERN,
    classOf[com.informix.jdbc.IfxDriver].getName,
    classOf[InformixConnection].getClassLoader)


  //private val CDC_SOURCE =  new IfxDataSource("jdbc:informix-sqli://localhost:9998/syscdcv1:user=informix;password=informix")
  private var lsnToInstantCache = null

  /**
   * actual name of the database, which could differ in casing from the database name given in the connector config.
   */

  private trait ResultSetExtractor[T] {
    @throws[SQLException]
    def apply(rs: ResultSet): T
  }

  class CdcEnabledTable private(val tableId: String, val captureName: String, val fromLsn: Lsn) {
    def getTableId: String = tableId

    def getCaptureName: String = captureName

    def getFromLsn: Lsn = fromLsn
  }
}

class InformixConnection(config: Configuration) extends JdbcConnection(config, InformixConnection.FACTORY) {

  // private var lsnToInstantCache = new BoundedConcurrentHashMap[Lsn, Instant](100)
  // private val realDatabaseName = retrieveRealDatabaseName()
  private val realDatabaseName = config.getString(JdbcConfiguration.DATABASE)

  private val cdcEngine: InformixCDCEngine = InformixCDCEngine.build(
    config.getString(JdbcConfiguration.HOSTNAME),
    config.getString(JdbcConfiguration.PORT),
    config.getString(JdbcConfiguration.USER),
    config.getString(JdbcConfiguration.DATABASE),
    config.getString(JdbcConfiguration.PASSWORD)
  )

  private var lsn: Long = 0x00
  private var transactionID: Int = 0x00

  /**
   * @return the current largest log sequence number
   */
  @throws[SQLException]
  def getLsn: Long = lsn

  /**
   * init the tables and columns for cdc capture.
   *
   * @param map - the requested table changes
   * @throws SQLException
   */
  def initCDCTableAndCols(map: Map[String, InformixCDCEngine.CDCTabeEntry]): Unit = {
    // TODO: Remove this function
    // this.cdcEngine.tableColsMap = map
  }

  /**
   * init the lsn for the first time connect
   *
   * @throws SQLException
   */
  @throws[SQLException]
  def initLsn(): Unit = {
    this.cdcEngine.record((record) => {
      this.lsn = record.getSequenceId
      true
    })
  }

  def getCDCEngine(): InformixCDCEngine = this.cdcEngine

  /**
   * Provides all changes recorded by the DB2 CDC capture process for a given table.
   *
   * @param tableId  - the requested table changes
   * @param fromLsn  - closed lower bound of interval of changes to be provided
   * @param toLsn    - closed upper bound of interval  of changes to be provided
   * @param consumer - the change processor
   * @throws SQLException
   */
  @throws[SQLException]
  def getChangesForTable(tableId: TableId, fromLsn: Lsn, toLsn: Lsn, consumer: JdbcConnection.ResultSetConsumer): Unit = null

  /**
   * Provides all changes recorder by the DB2 CDC capture process for a set of tables.
   *
   * @param changeTables    - the requested tables to obtain changes for
   * @param intervalFromLsn - closed lower bound of interval of changes to be provided
   * @param intervalToLsn   - closed upper bound of interval  of changes to be provided
   * @param consumer        - the change processor
   * @throws SQLException
   */
  @throws[SQLException]
  @throws[InterruptedException]
  def getChangesForTables(changeTables: Array[ChangeTable],
                          intervalFromLsn: Lsn,
                          intervalToLsn: Lsn,
                          consumer: JdbcConnection.BlockingMultiResultSetConsumer): Unit = null

  /**
   * Obtain the next available position in the database log.
   *
   * @param lsn - LSN of the current position
   * @return LSN of the next position in the database
   * @throws SQLException
   */
  @throws[SQLException]
  def incrementLsn(lsn: Lsn): Lsn = lsn.increment

  /**
   * Map a commit LSN to a point in time when the commit happened.
   *
   * @param lsn - LSN of the commit
   * @return time when the commit was recorded into the database log
   * @throws SQLException
   */
  @throws[SQLException]
  def timestampOfLsn(lsn: Lsn): Instant = null

  /**
   * Creates an exclusive lock for a given table.
   *
   * @param tableId to be locked
   * @throws SQLException
   */
  @throws[SQLException]
  def lockTable(tableId: TableId): Unit = null

  @throws[SQLException]
  def listOfChangeTables: util.Set[ChangeTable] = null

  @throws[SQLException]
  def listOfNewChangeTables(fromLsn: Lsn, toLsn: Lsn): util.Set[ChangeTable] = null

  @throws[SQLException]
  def getTableSchemaFromTable(changeTable: ChangeTable): Table = null

  @throws[SQLException]
  def getTableSchemaFromChangeTable(changeTable: ChangeTable): Table = null

  @throws[SQLException]
  def rollback(): Unit = {
    if (isConnected) connection.rollback()
  }

  def getNameOfChangeTable(captureName: String): String = captureName + "_CT"

  def getRealDatabaseName: String = realDatabaseName

  private def cdcNameForTable(tableId: TableId) = tableId.schema + '_' + tableId.table

  @throws[SQLException]
  private def singleResultMapper[T](extractor: InformixConnection.ResultSetExtractor[T], error: String) = null

  // private def retrieveRealDatabaseName = null
}