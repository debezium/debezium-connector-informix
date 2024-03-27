/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import java.util.Optional;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Width;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.ConfigDefinition;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.document.Document;
import io.debezium.heartbeat.DatabaseHeartbeatImpl;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.ColumnFilterMode;
import io.debezium.relational.HistorizedRelationalDatabaseConnectorConfig;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.spi.schema.DataCollectionId;

/**
 * The list of configuration options for Informix connector
 *
 * @author Laoflch Luo, Lars M Johansson
 */
public class InformixConnectorConfig extends HistorizedRelationalDatabaseConnectorConfig {

    protected static final String CDC_DATABASE = "syscdcv1";

    protected static final int DEFAULT_PORT = 9088;

    protected static final int DEFAULT_CDC_BUFFERSIZE = 0x10000;

    protected static final int DEFAULT_CDC_TIMEOUT = 5;

    /**
     * The set of predefined SnapshotMode options or aliases.
     */
    public enum SnapshotMode implements EnumeratedValue {

        /**
         * Performs a snapshot of data and schema upon each connector start.
         */
        ALWAYS("always"),

        /**
         * Perform a snapshot of data and schema upon initial startup of a connector.
         */
        INITIAL("initial"),

        /**
         * Perform a snapshot of data and schema upon initial startup of a connector but does not transition to streaming.
         */
        INITIAL_ONLY("initial_only"),

        /**
         * Perform a snapshot of the schema but no data upon initial startup of a connector.
         * @deprecated to be removed in Debezium 3.0, replaced by {{@link #NO_DATA}}
         */
        SCHEMA_ONLY("schema_only"),

        /**
         * Perform a snapshot of the schema but no data upon initial startup of a connector.
         */
        NO_DATA("no_data"),

        /**
         * Perform a snapshot of only the database schemas (without data) and then begin reading the redo log at the current redo log position.
         * This can be used for recovery only if the connector has existing offsets and the schema.history.internal.kafka.topic does not exist (deleted).
         * This recovery option should be used with care as it assumes there have been no schema changes since the connector last stopped,
         * otherwise some events during the gap may be processed with an incorrect schema and corrupted.
         */
        RECOVERY("recovery"),

        /**
         * Perform a snapshot when it is needed.
         */
        WHEN_NEEDED("when_needed"),

        /**
         * Allows over snapshots by setting connectors properties prefixed with 'snapshot.mode.configuration.based'.
         */
        CONFIGURATION_BASED("configuration_based"),

        /**
         * Inject a custom snapshotter, which allows for more control over snapshots.
         */
        CUSTOM("custom");

        private final String value;

        SnapshotMode(String value) {
            this.value = value;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @return the matching option, or null if no match is found
         */
        public static SnapshotMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();

            for (SnapshotMode option : SnapshotMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }

            return null;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @param defaultValue the default value; may be null
         * @return the matching option, or null if no match is found and the non-null default is invalid
         */
        public static SnapshotMode parse(String value, String defaultValue) {
            SnapshotMode mode = parse(value);

            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }

            return mode;
        }

        @Override
        public String getValue() {
            return value;
        }

    }

    /**
     * The set of predefined snapshot locking mode options.
     */
    public enum SnapshotLockingMode implements EnumeratedValue {

        EXCLUSIVE("exclusive"),

        SHARE("share"),

        CUSTOM("custom");

        private final String value;

        SnapshotLockingMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @return the matching option, or null if no match is found
         */
        public static SnapshotLockingMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (SnapshotLockingMode option : SnapshotLockingMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return null;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @param defaultValue the default value; may be null
         * @return the matching option, or null if no match is found and the non-null default is invalid
         */
        public static SnapshotLockingMode parse(String value, String defaultValue) {
            SnapshotLockingMode mode = parse(value);
            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }
            return mode;
        }
    }

    /**
     * The set of predefined snapshot isolation mode options.
     */
    public enum SnapshotIsolationMode implements EnumeratedValue {

        /**
         * This mode will block all reads and writes for the entire duration of the snapshot.
         * The connector will execute {@code SELECT * FROM .. WITH (TABLOCKX)}
         */
        EXCLUSIVE("exclusive"),

        /**
         * This mode uses REPEATABLE READ isolation level. This mode will avoid taking any table
         * locks during the snapshot process, except schema snapshot phase where exclusive table
         * locks are acquired for a short period.  Since phantom reads can occur, it does not fully
         * guarantee consistency.
         */
        REPEATABLE_READ("repeatable_read"),

        /**
         * This mode uses READ COMMITTED isolation level. This mode does not take any table locks during
         * the snapshot process. In addition, it does not take any long-lasting row-level locks, like
         * in repeatable read isolation level. Snapshot consistency is not guaranteed.
         */
        READ_COMMITTED("read_committed"),

        /**
         * This mode uses READ UNCOMMITTED isolation level. This mode takes neither table locks nor row-level locks
         * during the snapshot process.  This way other transactions are not affected by initial snapshot process.
         * However, snapshot consistency is not guaranteed.
         */
        READ_UNCOMMITTED("read_uncommitted");

        private final String value;

        SnapshotIsolationMode(String value) {
            this.value = value;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @return the matching option, or null if no match is found
         */
        public static SnapshotIsolationMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (SnapshotIsolationMode option : SnapshotIsolationMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return null;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @param defaultValue the default value; may be null
         * @return the matching option, or null if no match is found and the non-null default is invalid
         */
        public static SnapshotIsolationMode parse(String value, String defaultValue) {
            SnapshotIsolationMode mode = parse(value);
            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }
            return mode;
        }

        @Override
        public String getValue() {
            return value;
        }
    }

    public static final Field PORT = RelationalDatabaseConnectorConfig.PORT.withDefault(DEFAULT_PORT);

    public static final Field SNAPSHOT_MODE = Field.create("snapshot.mode")
            .withDisplayName("Snapshot mode")
            .withEnum(SnapshotMode.class, SnapshotMode.INITIAL)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_SNAPSHOT, 0))
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("The criteria for running a snapshot upon startup of the connector. "
                    + "Options include: "
                    + "'initial' (the default) to specify the connector should run a snapshot only when no offsets are available for the logical server name; "
                    + "'schema_only' to specify the connector should run a snapshot of the schema when no offsets are available for the logical server name. ");

    public static final Field SNAPSHOT_ISOLATION_MODE = Field.create("snapshot.isolation.mode")
            .withDisplayName("Snapshot isolation mode")
            .withEnum(SnapshotIsolationMode.class, SnapshotIsolationMode.REPEATABLE_READ)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_SNAPSHOT, 1))
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("Controls which transaction isolation level is used and how long the connector locks the monitored tables. "
                    + "The default is '" + SnapshotIsolationMode.REPEATABLE_READ.getValue()
                    + "', which means that repeatable read isolation level is used. In addition, exclusive locks are taken only during schema snapshot. "
                    + "Using a value of '" + SnapshotIsolationMode.EXCLUSIVE.getValue()
                    + "' ensures that the connector holds the exclusive lock (and thus prevents any reads and updates) for all monitored tables during the entire snapshot duration. "
                    + "In '" + SnapshotIsolationMode.READ_COMMITTED.getValue()
                    + "' mode no table locks or any *long-lasting* row-level locks are acquired, but connector does not guarantee snapshot consistency."
                    + "In '" + SnapshotIsolationMode.READ_UNCOMMITTED.getValue()
                    + "' mode neither table nor row-level locks are acquired, but connector does not guarantee snapshot consistency.");

    public static final Field SNAPSHOT_LOCKING_MODE = Field.create("snapshot.locking.mode")
            .withDisplayName("Snapshot locking mode")
            .withEnum(SnapshotLockingMode.class, SnapshotLockingMode.EXCLUSIVE)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_SNAPSHOT, 2))
            .withDescription(
                    "Controls how the connector holds locks on tables while performing the schema snapshot when `snapshot.isolation.mode` is `REPEATABLE_READ` or `EXCLUSIVE`. The 'exclusive' "
                            + "which means the connector will hold a table lock for exclusive table access for just the initial portion of the snapshot "
                            + "while the database schemas and other metadata are being read. The remaining work in a snapshot involves selecting all rows from "
                            + "each table, and this is done using a flashback query that requires no locks. However, in some cases it may be desirable to avoid "
                            + "locks entirely which can be done by specifying 'none'. This mode is only safe to use if no schema changes are happening while the "
                            + "snapshot is taken.");

    public static final Field CDC_BUFFERSIZE = Field.create("cdc.buffersize")
            .withDisplayName("CDC Engine buffer size")
            .withType(ConfigDef.Type.INT)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_ADVANCED, 0))
            .withWidth(Width.MEDIUM).withImportance(Importance.MEDIUM)
            .withDescription("Size of the read buffer for receiving events from the server, in bytes.")
            .withValidation(Field::isNonNegativeInteger)
            .withDefault(DEFAULT_CDC_BUFFERSIZE);

    public static final Field CDC_TIMEOUT = Field.create("cdc.timeout")
            .withDisplayName("CDC Engine timeout")
            .withType(ConfigDef.Type.INT)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_ADVANCED, 0))
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .withDescription("Specifies a timeout to interrupt blocking to wait on an event, in seconds. "
                    + "The timeout allows the CDC API to periodically break to receive a timeout event. "
                    + "This in turns allows the CDC engine to close cleanly as well as indicate to the running program the connection is alive and active.")
            .withValidation(Field::isNonNegativeInteger)
            .withDefault(DEFAULT_CDC_TIMEOUT);

    public static final Field SOURCE_INFO_STRUCT_MAKER = CommonConnectorConfig.SOURCE_INFO_STRUCT_MAKER
            .withDefault(InformixSourceInfoStructMaker.class.getName());

    private static final ConfigDefinition CONFIG_DEFINITION = HistorizedRelationalDatabaseConnectorConfig.CONFIG_DEFINITION.edit()
            .name("Informix")
            .type(
                    HOSTNAME,
                    PORT,
                    USER,
                    PASSWORD,
                    DATABASE_NAME)
            .connector(
                    SNAPSHOT_MODE,
                    SNAPSHOT_ISOLATION_MODE,
                    INCREMENTAL_SNAPSHOT_CHUNK_SIZE,
                    CDC_BUFFERSIZE,
                    CDC_TIMEOUT)
            .events(SOURCE_INFO_STRUCT_MAKER)
            .excluding(
                    SCHEMA_INCLUDE_LIST,
                    SCHEMA_EXCLUDE_LIST,
                    INCLUDE_SCHEMA_COMMENTS,
                    INCREMENTAL_SNAPSHOT_ALLOW_SCHEMA_CHANGES,
                    SNAPSHOT_MAX_THREADS,
                    DatabaseHeartbeatImpl.HEARTBEAT_ACTION_QUERY)
            .create();

    protected static ConfigDef configDef() {
        return CONFIG_DEFINITION.configDef();
    }

    /**
     * The set of {@link Field}s defined as part of this configuration.
     */
    public static final Field.Set ALL_FIELDS = Field.setOf(CONFIG_DEFINITION.all());

    private final String databaseName;
    private final SnapshotMode snapshotMode;
    private final SnapshotIsolationMode snapshotIsolationMode;
    private final JdbcConfiguration cdcJdbcConfig;
    private final int cdcBuffersize;
    private final int cdcTimeout;

    private final SnapshotLockingMode snapshotLockingMode;

    public InformixConnectorConfig(Configuration config) {
        super(
                InformixConnector.class,
                config,
                new SystemTablesPredicate(),
                t -> t.catalog() + '.' + t.schema() + '.' + t.table(),
                false,
                ColumnFilterMode.SCHEMA,
                false);

        this.databaseName = config.getString(DATABASE_NAME);
        this.snapshotMode = SnapshotMode.parse(config.getString(SNAPSHOT_MODE), SNAPSHOT_MODE.defaultValueAsString());
        this.snapshotIsolationMode = SnapshotIsolationMode.parse(config.getString(SNAPSHOT_ISOLATION_MODE), SNAPSHOT_ISOLATION_MODE.defaultValueAsString());
        this.snapshotLockingMode = SnapshotLockingMode.parse(config.getString(SNAPSHOT_LOCKING_MODE), SNAPSHOT_LOCKING_MODE.defaultValueAsString());
        this.cdcJdbcConfig = JdbcConfiguration.adapt(getJdbcConfig().edit().with(JdbcConfiguration.DATABASE, CDC_DATABASE).build());
        this.cdcBuffersize = config.getInteger(CDC_BUFFERSIZE);
        this.cdcTimeout = config.getInteger(CDC_TIMEOUT);
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public SnapshotMode getSnapshotMode() {
        return snapshotMode;
    }

    @Override
    public Optional<? extends EnumeratedValue> getSnapshotLockingMode() {
        return Optional.of(this.snapshotLockingMode);
    }

    public SnapshotIsolationMode getSnapshotIsolationMode() {
        return this.snapshotIsolationMode;
    }

    public JdbcConfiguration getCdcJdbcConfig() {
        return cdcJdbcConfig;
    }

    public int getCdcBuffersize() {
        return cdcBuffersize;
    }

    public int getCdcTimeout() {
        return cdcTimeout;
    }

    @Override
    protected SourceInfoStructMaker<? extends AbstractSourceInfo> getSourceInfoStructMaker(Version version) {
        return getSourceInfoStructMaker(SOURCE_INFO_STRUCT_MAKER, Module.name(), Module.version(), this);
    }

    @Override
    protected HistoryRecordComparator getHistoryRecordComparator() {
        return new HistoryRecordComparator() {
            @Override
            protected boolean isPositionAtOrBefore(Document recorded, Document desired) {
                return Lsn.of(recorded.getString(SourceInfo.CHANGE_LSN_KEY))
                        .compareTo(Lsn.of(desired.getString(SourceInfo.CHANGE_LSN_KEY))) < 1;
            }
        };
    }

    @Override
    public String getContextName() {
        return Module.contextName();
    }

    @Override
    public boolean isSignalDataCollection(DataCollectionId dataCollectionId) {
        String signalingDataCollection = getSignalingDataCollectionId();
        return signalingDataCollection != null && !signalingDataCollection.isEmpty() && dataCollectionId.identifier().endsWith(signalingDataCollection);
    }

    @Override
    public String getConnectorName() {
        return Module.name();
    }

    private static class SystemTablesPredicate implements TableFilter {

        @Override
        public boolean isIncluded(TableId t) {
            return !(t.table().toLowerCase().startsWith("sys"));
        }
    }
}
