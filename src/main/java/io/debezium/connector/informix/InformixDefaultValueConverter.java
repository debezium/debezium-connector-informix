/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.DefaultValueConverter;
import io.debezium.relational.ValueConverter;
import io.debezium.util.Strings;

/**
 * Converter for table column's default values.
 *
 * @author Lars M Johansson, Chris Cranford
 */
public class InformixDefaultValueConverter implements DefaultValueConverter {

    private static final Logger LOGGER = LoggerFactory.getLogger(InformixDefaultValueConverter.class);

    private final InformixValueConverters valueConverters;
    private final Map<Integer, DefaultValueMapper> defaultValueMappers;

    public InformixDefaultValueConverter(InformixValueConverters valueConverters, InformixConnection jdbcConnection) {
        this.valueConverters = valueConverters;
        this.defaultValueMappers = Collections.unmodifiableMap(createDefaultValueMappers(jdbcConnection));
    }

    @Override
    public Optional<Object> parseDefaultValue(Column column, String defaultValue) {
        LOGGER.info("Parsing default value for column '{}' with expression '{}'", column.name(), defaultValue);
        final int dataType = column.jdbcType();
        final DefaultValueMapper mapper = defaultValueMappers.get(dataType);
        if (mapper == null) {
            LOGGER.warn("Mapper for type '{}' not found.", dataType);
            return Optional.empty();
        }

        try {
            Object rawDefaultValue = mapper.parse(column, defaultValue != null ? defaultValue.trim() : defaultValue);
            Object convertedDefaultValue = convertDefaultValue(rawDefaultValue, column);
            if (convertedDefaultValue instanceof Struct) {
                // Workaround for KAFKA-12694
                LOGGER.warn("Struct can't be used as default value for column '{}', will use null instead.", column.name());
                return Optional.empty();
            }
            return Optional.ofNullable(convertedDefaultValue);
        }
        catch (Exception e) {
            LOGGER.warn("Cannot parse column default value '{}' to type '{}'.  Expression evaluation is not supported.", defaultValue, dataType, e);
            LOGGER.debug("Parsing failed due to error", e);
            return Optional.empty();
        }
    }

    private Object convertDefaultValue(Object defaultValue, Column column) {
        // if converters is not null and the default value is not null, we need to convert default value
        if (valueConverters != null && defaultValue != null) {
            final SchemaBuilder schemaBuilder = valueConverters.schemaBuilder(column);
            if (schemaBuilder != null) {
                final Schema schema = schemaBuilder.build();
                // In order to get the valueConverter for this column, we have to create a field;
                // The index value -1 in the field will never be used when converting default value;
                // So we can set any number here;
                final Field field = new Field(column.name(), -1, schema);
                final ValueConverter valueConverter = valueConverters.converter(column, field);

                return valueConverter.convert(defaultValue);
            }
        }
        return defaultValue;
    }

    private static Map<Integer, DefaultValueMapper> createDefaultValueMappers(InformixConnection connection) {
        // Data types that are supported should be registered in the map.
        final Map<Integer, DefaultValueMapper> result = new HashMap<>();

        // Numeric types
        result.put(Types.BOOLEAN, nullableDefaultValueMapper(booleanDefaultValueMapper()));
        result.put(Types.BIGINT, nullableDefaultValueMapper());
        result.put(Types.NUMERIC, nullableDefaultValueMapper(numericDefaultValueMapper()));
        result.put(Types.INTEGER, nullableDefaultValueMapper());
        result.put(Types.SMALLINT, nullableDefaultValueMapper());

        // Other numerical values
        result.put(Types.DECIMAL, nullableDefaultValueMapper(numericDefaultValueMapper()));
        result.put(Types.DOUBLE, nullableDefaultValueMapper((c, v) -> Double.parseDouble(v)));
        result.put(Types.FLOAT, nullableDefaultValueMapper((c, v) -> Double.parseDouble(v)));
        result.put(Types.REAL, nullableDefaultValueMapper((c, v) -> Float.parseFloat(v)));

        // Date and time
        result.put(Types.DATE, nullableDefaultValueMapper(castTemporalFunctionCall(connection, Types.DATE)));
        result.put(Types.TIME, nullableDefaultValueMapper(castTemporalFunctionCall(connection, Types.TIME)));
        result.put(Types.TIMESTAMP, nullableDefaultValueMapper(castTemporalFunctionCall(connection, Types.TIMESTAMP)));

        // Character strings
        result.put(Types.CHAR, nullableDefaultValueMapper(enforceCharFieldPadding()));
        result.put(Types.VARCHAR, nullableDefaultValueMapper(enforceStringUnquote()));
        result.put(Types.LONGVARCHAR, nullableDefaultValueMapper(enforceStringUnquote()));

        // Unicode character strings
        result.put(Types.NCHAR, nullableDefaultValueMapper(enforceCharFieldPadding()));
        result.put(Types.NVARCHAR, nullableDefaultValueMapper(enforceStringUnquote()));
        result.put(Types.LONGNVARCHAR, nullableDefaultValueMapper(enforceStringUnquote()));

        return result;
    }

    private static DefaultValueMapper nullableDefaultValueMapper() {
        return nullableDefaultValueMapper(null);
    }

    private static DefaultValueMapper nullableDefaultValueMapper(DefaultValueMapper mapper) {
        return (column, value) -> {
            if ("NULL".equalsIgnoreCase(value)) {
                return null;
            }
            if (mapper != null) {
                return mapper.parse(column, value);
            }
            return value;
        };
    }

    public static DefaultValueMapper booleanDefaultValueMapper() {
        return (column, value) -> {
            if ("t".equals(value.trim())) {
                return true;
            }
            else if ("f".equals(value.trim())) {
                return false;
            }
            return Boolean.parseBoolean(value.trim());
        };
    }

    public static DefaultValueMapper numericDefaultValueMapper() {
        return (column, value) -> {
            BigDecimal decimal = new BigDecimal(value);
            return column.scale().<Number> map(decimal::setScale).orElseGet(decimal::toBigIntegerExact);
        };
    }

    private static DefaultValueMapper castTemporalFunctionCall(InformixConnection connection, int jdbcType) {
        return (column, value) -> {
            if ("TODAY".equalsIgnoreCase(value.trim())) {
                // If the column is optional, the default value is ignored
                return column.isOptional() ? null : Date.valueOf("1970-01-01");
            }
            else if ("CURRENT".equalsIgnoreCase(value.trim()) || "SYSDATE".equalsIgnoreCase(value.trim())) {
                String[] typeExpr = column.typeExpression().trim().split("[\\s()]");
                StringBuilder dateTimeStr = new StringBuilder("1970-01-01 00:00:00");
                if ("DATETIME".equalsIgnoreCase(typeExpr[0])) {
                    int scale;
                    switch (typeExpr.length) {
                        case 5:
                            scale = Integer.parseInt(typeExpr[4]);
                            break;
                        case 4:
                            scale = "FRACTION".equalsIgnoreCase(typeExpr[3]) ? 3 : 0;
                            break;
                        case 2:
                            scale = Integer.parseInt(typeExpr[1]);
                            break;
                        case 1:
                        default:
                            scale = 3;
                    }
                    if (scale > 0) {
                        dateTimeStr.append('.').append("0".repeat(scale));
                    }
                }
                String defaultVal = dateTimeStr.toString();
                // If the column is optional, the default value is ignored
                return column.isOptional() ? null : Timestamp.valueOf(defaultVal);
            }
            else {
                switch (jdbcType) {
                    case Types.DATE:
                        return JdbcConnection.querySingleValue(
                                connection.connection(),
                                "SELECT DATE('" + value + "') FROM sysmaster:sysdual",
                                st -> {
                                },
                                rs -> rs.getDate(1));
                    case Types.TIME:
                    case Types.TIMESTAMP:
                        return JdbcConnection.querySingleValue(
                                connection.connection(),
                                "SELECT DATETIME(" + value + ") " + column.typeExpression().substring(9).toUpperCase() + " FROM sysmaster:sysdual",
                                st -> {
                                },
                                rs -> rs.getTimestamp(1));
                    default:
                        throw new DebeziumException("Unexpected JDBC type '" + jdbcType + "' for default value resolution: " + value);
                }
            }
        };
    }

    private static DefaultValueMapper enforceCharFieldPadding() {
        return (column, value) -> value != null ? Strings.pad(unquote(value), column.length(), ' ') : null;
    }

    private static DefaultValueMapper enforceStringUnquote() {
        return (column, value) -> value != null ? unquote(value) : null;
    }

    private static String unquote(String value) {
        if (value.startsWith("('") && value.endsWith("')")) {
            return value.substring(2, value.length() - 2);
        }
        if (value.startsWith("'") && value.endsWith("'")) {
            return value.substring(1, value.length() - 1);
        }
        return value;
    }
}
