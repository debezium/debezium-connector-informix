package laoflch.debezium.connector.informix;

import java.sql.Types;
import java.time.ZoneOffset;

import io.debezium.time.MicroTimestamp;
import io.debezium.time.NanoTimestamp;
import io.debezium.time.Timestamp;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;

/**
 * Conversion of Informix specific datatypes.
 *
 * @author Xiaolin Zhang, laoflch Luo
 *
 */
public class InformixValueConverters extends JdbcValueConverters {

    public InformixValueConverters() {
    }

    /**
     * Create a new instance that always uses UTC for the default time zone when
     * converting values without timezone information to values that require
     * timezones.
     * <p>
     *
     * @param decimalMode
     *            how {@code DECIMAL} and {@code NUMERIC} values should be
     *            treated; may be null if
     *            {@link io.debezium.jdbc.JdbcValueConverters.DecimalMode#PRECISE}
     *            is to be used
     * @param temporalPrecisionMode
     *            date/time value will be represented either as Connect datatypes or Debezium specific datatypes
     */
    public InformixValueConverters(DecimalMode decimalMode, TemporalPrecisionMode temporalPrecisionMode) {
        super(decimalMode, temporalPrecisionMode, ZoneOffset.UTC, null, null);
    }

    @Override
    public SchemaBuilder schemaBuilder(Column column) {
        switch (column.jdbcType()) {
            // Numeric integers
            case Types.TINYINT:
                // values are an 8-bit unsigned integer value between 0 and 255, we thus need to store it in short int
                return SchemaBuilder.int16();
            case Types.TIMESTAMP:
                if (!this.adaptiveTimePrecisionMode && !this.adaptiveTimeMicrosecondsPrecisionMode) {
                    if (this.getTimePrecision(column) <= 3) {
                        return io.debezium.time.Timestamp.builder();
                    } else if (this.getTimePrecision(column) <= 6) {
                        return MicroTimestamp.builder();
                    } else {
                        return NanoTimestamp.builder();
                    }
                } else {
                    return Timestamp.builder();
                }
            default:
                return super.schemaBuilder(column);
        }
    }

    @Override
    public ValueConverter converter(Column column, Field fieldDefn) {
        switch (column.jdbcType()) {
            // Numeric integers
            case Types.TINYINT:
                // values are an 8-bit unsigned integer value between 0 and 255, we thus need to store it in short int
                return (data) -> convertSmallInt(column, fieldDefn, data);
            case Types.DECIMAL:
                return (data) -> convertDecimal(column, fieldDefn, data);
            case Types.TIMESTAMP:
                return (data) -> {
                    if (!adaptiveTimePrecisionMode && !adaptiveTimeMicrosecondsPrecisionMode) {
                        if (getTimePrecision(column) <= 3) {
                            return convertTimestampToEpochMillis(column, fieldDefn, data);
                        } else if (getTimePrecision(column) <= 6) {
                            return convertTimestampToEpochMicros(column, fieldDefn, data);
                        } else {
                            return convertTimestampToEpochNanos(column, fieldDefn, data);
                        }
                    } else {
                        return convertTimestampToEpochMillisAsDate(column, fieldDefn, data);
                    }
                };
            default:
                return super.converter(column, fieldDefn);
        }
    }

    @Override
    protected int getTimePrecision(Column column) {
        return column.scale().get();
    }

    protected Object convertTimestampWithZone(Column column, Field fieldDefn, Object data) {
        // dummy return
        return super.convertTimestampWithZone(column, fieldDefn, data);
    }

}
