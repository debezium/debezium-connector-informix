/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import java.time.ZoneOffset;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
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
        switch (column.jdbcType()) {
            default:
                return super.schemaBuilder(column);
        }
    }

    @Override
    public ValueConverter converter(Column column, Field fieldDefn) {
        switch (column.jdbcType()) {
            default:
                return super.converter(column, fieldDefn);
        }
    }

    /**
     * Time precision in Informix is defined in scale, the default one is 3
     */
    @Override
    protected int getTimePrecision(Column column) {
        return column.scale().orElse(3);
    }

}
