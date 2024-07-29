/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import java.io.BufferedReader;
import java.math.BigDecimal;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.Types;
import java.time.ZoneOffset;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;

import com.informix.jdbc.IfxCblob;

import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;

/**
 * Conversion of Informix specific datatypes.
 *
 * @author Xiaolin Zhang, Laoflch Luo, Lars M Johansson
 *
 */
public class InformixValueConverters extends JdbcValueConverters {

    private static final int FLOATING_POINT_DECIMAL_SCALE = 255;

    /**
     * Create a new instance that always uses UTC for the default time zone when
     * converting values without timezone information to values that require
     * timezones.
     * <p>
     *
     * @param decimalMode           how {@code DECIMAL} and {@code NUMERIC} values should be treated;
     *                              may be null if {@link DecimalMode#PRECISE} is to be used
     * @param temporalPrecisionMode date/time value will be represented either as Connect datatypes or Debezium specific datatypes
     * @param binaryHandlingMode    ?
     */
    public InformixValueConverters(DecimalMode decimalMode, TemporalPrecisionMode temporalPrecisionMode, BinaryHandlingMode binaryHandlingMode) {
        super(decimalMode, temporalPrecisionMode, ZoneOffset.UTC, null, null, binaryHandlingMode);
    }

    @Override
    public SchemaBuilder schemaBuilder(Column column) {
        logger.debug("Building schema for column {} of type {} named {} with constraints ({},{})",
                column.name(),
                column.jdbcType(),
                column.typeName(),
                column.length(),
                column.scale());

        switch (column.jdbcType()) {
            case Types.NUMERIC:
            case Types.DECIMAL:
                return getNumericSchema(column);
            default:
                SchemaBuilder builder = super.schemaBuilder(column);
                logger.debug("JdbcValueConverters returned '{}' for column '{}'", builder != null ? builder.getClass().getName() : null, column.name());
                return builder;
        }
    }

    private SchemaBuilder getNumericSchema(Column column) {
        if (column.scale().isPresent()) {

            if (column.scale().get() == FLOATING_POINT_DECIMAL_SCALE && decimalMode == DecimalMode.PRECISE) {
                return VariableScaleDecimal.builder();
            }

            return super.schemaBuilder(column);
        }

        if (decimalMode == DecimalMode.PRECISE) {
            return VariableScaleDecimal.builder();
        }

        if (column.length() == 0) {
            // Defined as DECIMAL without specifying a length and scale, treat as DECIMAL(16)
            return SpecialValueDecimal.builder(decimalMode, 16, -1);
        }

        return SpecialValueDecimal.builder(decimalMode, column.length(), -1);
    }

    @Override
    public ValueConverter converter(Column column, Field fieldDefn) {
        switch (column.jdbcType()) {
            case Types.NUMERIC:
            case Types.DECIMAL:
                return getNumericConverter(column, fieldDefn);
            default:
                return super.converter(column, fieldDefn);
        }
    }

    private ValueConverter getNumericConverter(Column column, Field fieldDefn) {
        if (column.scale().isPresent()) {

            if (column.scale().get() == FLOATING_POINT_DECIMAL_SCALE && decimalMode == DecimalMode.PRECISE) {
                return data -> convertVariableScale(column, fieldDefn, data);
            }

            return data -> convertNumeric(column, fieldDefn, data);
        }

        return data -> convertVariableScale(column, fieldDefn, data);
    }

    private Object convertVariableScale(Column column, Field fieldDefn, Object data) {
        data = convertNumeric(column, fieldDefn, data); // provides default value

        if (data == null) {
            return null;
        }
        if (decimalMode == DecimalMode.PRECISE) {
            if (data instanceof SpecialValueDecimal) {
                return VariableScaleDecimal.fromLogical(fieldDefn.schema(), (SpecialValueDecimal) data);
            }
            else if (data instanceof BigDecimal) {
                return VariableScaleDecimal.fromLogical(fieldDefn.schema(), (BigDecimal) data);
            }
        }
        else {
            return data;
        }
        return handleUnknownData(column, fieldDefn, data);
    }

    @Override
    protected Object convertString(Column column, Field fieldDefn, Object data) {
        if (data instanceof Clob) {
            return convertValue(column, fieldDefn, data, "", (receiver) -> {
                try {
                    receiver.deliver(new BufferedReader(((IfxCblob) data).getCharacterStream()).lines().collect(Collectors.joining(System.lineSeparator())));
                }
                catch (SQLException e) {
                    throw new RuntimeException("Error processing data from " + column.jdbcType() + " and column " + column +
                            ": class=" + data.getClass(), e);
                }
            });
        }
        return super.convertString(column, fieldDefn, data);
    }

    @Override
    protected int getTimePrecision(Column column) {
        return column.length() < 20 ? 0 : column.length() - 20;
    }

}
