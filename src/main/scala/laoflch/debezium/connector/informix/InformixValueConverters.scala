package laoflch.debezium.connector.informix.integrtest

import java.sql.Types
import java.time.ZoneOffset

import io.debezium.relational.ValueConverter
import org.apache.kafka.connect.data.Field
import org.apache.kafka.connect.data.SchemaBuilder
import io.debezium.jdbc.{JdbcValueConverters, ResultReceiver, TemporalPrecisionMode}
import io.debezium.relational.Column
import io.debezium.util.NumberConversions
import io.debezium.util.NumberConversions.SHORT_FALSE

object InformixValueConverters extends JdbcValueConverters{
  /*protected def convertSmallInt(column: Column, fieldDefn: Field, data: Any): Any = convertValue(column, fieldDefn, data, SHORT_FALSE, (r: ResultReceiver) => {
    def foo(r: ResultReceiver) = if (data.isInstanceOf[Short]) r.deliver(data)
    else if (data.isInstanceOf[Number]) {
      val value = data.asInstanceOf[Number]
      r.deliver(Short.valueOf(value.shortValue))
    }
    else if (data.isInstanceOf[Boolean]) r.deliver(NumberConversions.getShort(data.asInstanceOf[Boolean]))
    else if (data.isInstanceOf[String]) r.deliver(Short.valueOf(data.asInstanceOf[String]))

    foo(r)
  })*/
}
/**
 * Conversion of Informix specific datatypes.
 *
 * @author Jiri Pechanec, Peter Urbanetz
 *
 */
class InformixValueConverters(decimalMode: JdbcValueConverters.DecimalMode, temporalPrecisionMode: TemporalPrecisionMode) extends JdbcValueConverters(decimalMode, temporalPrecisionMode, ZoneOffset.UTC, null, null) {


  override def schemaBuilder(column: Column): SchemaBuilder = column.jdbcType match { // Numeric integers
    case Types.TINYINT =>
      // values are an 8-bit unsigned integer value between 0 and 255, we thus need to store it in short int
      SchemaBuilder.int16
    case _ =>
      super.schemaBuilder(column)
  }

  override def converter(column: Column, fieldDefn: Field): ValueConverter = column.jdbcType match {
    case Types.TINYINT =>
      (data: Any) => convertSmallInt(column, fieldDefn, data)
    case _ =>
      super.converter(column, fieldDefn)
  }

  /**
   * Time precision in DB2 is defined in scale, the default one is 7
   */
  override protected def getTimePrecision(column: Column): Int = column.scale.get

  override protected def convertTimestampWithZone(column: Column, fieldDefn: Field, data: Any): Any = { // dummy return
    super.convertTimestampWithZone(column, fieldDefn, data)
  }
}
