/**
 * Copyright 2010-2014 Axel Fontaine
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.flywaydb.core.internal.metadatatable;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.flywaydb.core.EsusFlywayTables;
import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.api.MigrationType;
import org.flywaydb.core.api.MigrationVersion;
import org.flywaydb.core.internal.dbsupport.DbSupport;
import org.flywaydb.core.internal.dbsupport.JdbcTemplate;
import org.flywaydb.core.internal.dbsupport.Schema;
import org.flywaydb.core.internal.dbsupport.SqlScript;
import org.flywaydb.core.internal.dbsupport.Table;
import org.flywaydb.core.internal.util.PlaceholderReplacer;
import org.flywaydb.core.internal.util.StopWatch;
import org.flywaydb.core.internal.util.StringUtils;
import org.flywaydb.core.internal.util.TimeFormat;
import org.flywaydb.core.internal.util.jdbc.RowMapper;
import org.flywaydb.core.internal.util.logging.Log;
import org.flywaydb.core.internal.util.logging.LogFactory;
import org.flywaydb.core.internal.util.scanner.classpath.ClassPathResource;

/**
 * Supports reading and writing to the metadata table.
 */
public class MetaDataTableImpl implements MetaDataTable {
    private static final Log LOG = LogFactory.getLog(MetaDataTableImpl.class);

    /**
     * Database-specific functionality.
     */
    private final DbSupport dbSupport;

    /**
     * The metadata table used by flyway.
     */
    private final Table table;

    /**
     * JdbcTemplate with ddl manipulation access to the database.
     */
    private final JdbcTemplate jdbcTemplate;

    /**
     * The ClassLoader to use.
     */
    private ClassLoader classLoader;

    /**
     * Creates a new instance of the metadata table support.
     *
     * @param dbSupport   Database-specific functionality.
     * @param table       The metadata table used by flyway.
     * @param classLoader The ClassLoader for loading migrations on the classpath.
     */
    public MetaDataTableImpl(DbSupport dbSupport, Table table, ClassLoader classLoader) {
        this.jdbcTemplate = dbSupport.getJdbcTemplate();
        this.dbSupport = dbSupport;
        this.table = table;
        this.classLoader = classLoader;
    }

    /**
     * Creates the metatable if it doesn't exist, upgrades it if it does.
     */
    private void createIfNotExists() {
        if (table.exists()) {
            return;
        }

        LOG.info("Creating Metadata table: " + table);

        String resourceName = "org/flywaydb/core/internal/dbsupport/resource/" + dbSupport.getDbName() + "/createMetaDataTable.sql";
        String source = new ClassPathResource(resourceName, classLoader).loadAsString("UTF-8");

        Map<String, String> placeholders = new HashMap<String, String>();
        placeholders.put(new String("schema"), table.getSchema().getName());
        placeholders.put(new String("table"), table.getName());
        String sourceNoPlaceholders = new PlaceholderReplacer(placeholders, "${", "}").replacePlaceholders(source);

        SqlScript sqlScript = new SqlScript(sourceNoPlaceholders, dbSupport);
        sqlScript.execute(jdbcTemplate);

        LOG.debug("Metadata table " + table + " created.");
    }

    @Override
    public void lock() {
        createIfNotExists();
        table.lock();
    }

    @Override
    public void addAppliedMigration(AppliedMigration appliedMigration) {
        createIfNotExists();

        MigrationVersion version = appliedMigration.getVersion();
        try {
            int versionRank = calculateVersionRank(version);

            jdbcTemplate.update("UPDATE " + table
                    + " SET " + dbSupport.quote(new String[] { EsusFlywayTables.getVersionRank() }) + " = " + dbSupport.quote(new String[] { EsusFlywayTables.getVersionRank() })
                    + " + 1 WHERE " + dbSupport.quote(new String[] { EsusFlywayTables.getVersionRank() }) + " >= ?", new Object[] { Integer.valueOf(versionRank) });
            jdbcTemplate.update("INSERT INTO " + this.table + 
                    " (" + this.dbSupport.quote(new String[] { EsusFlywayTables.getVersionRank() }) + 
                    "," + this.dbSupport.quote(new String[] { EsusFlywayTables.getInstalledRank() }) + 
                    "," + this.dbSupport.quote(new String[] { EsusFlywayTables.getVersion() }) + 
                    "," + this.dbSupport.quote(new String[] { EsusFlywayTables.getDescription() }) + 
                    "," + this.dbSupport.quote(new String[] { EsusFlywayTables.getType() }) + 
                    "," + this.dbSupport.quote(new String[] { EsusFlywayTables.getScript() }) + 
                    "," + this.dbSupport.quote(new String[] { EsusFlywayTables.getChecksum() }) + 
                    "," + this.dbSupport.quote(new String[] { EsusFlywayTables.getInstalledBy() }) + 
                    "," + this.dbSupport.quote(new String[] { EsusFlywayTables.getExecutionTime() }) + 
                    "," + this.dbSupport.quote(new String[] { EsusFlywayTables.getSuccess() }) + 
                    ")" + 
                    " VALUES (?, ?, ?, ?, ?, ?, ?, " + this.dbSupport.getCurrentUserFunction() + ", ?, ?)",
                    versionRank, 
                    calculateInstalledRank(), 
                    version.toString(), 
                    appliedMigration.getDescription(), 
                    appliedMigration.getType().name(), 
                    appliedMigration.getScript(), 
                    appliedMigration.getChecksum(), 
                    Integer.valueOf(appliedMigration.getExecutionTime()), 
                    (appliedMigration.isSuccess()? 1 : 0)
            );
            LOG.debug("MetaData table " + table + " successfully updated to reflect changes");
        } catch (SQLException e) {
            throw new FlywayException("Unable to insert row for version '" + version + "' in metadata table " + table, e);
        }
    }

    /**
     * Calculates the installed rank for the new migration to be inserted.
     *
     * @return The installed rank.
     */
    private int calculateInstalledRank() throws SQLException {
        int currentMax = jdbcTemplate.queryForInt("SELECT MAX(" + dbSupport.quote(new String [] {EsusFlywayTables.getInstalledRank()}) + ")"
                + " FROM " + table);
        return currentMax + 1;
    }

    /**
     * Calculate the rank for this new version about to be inserted.
     *
     * @param version The version to calculated for.
     * @return The rank.
     */
    private int calculateVersionRank(MigrationVersion version) throws SQLException {
        List<String> versions = jdbcTemplate.queryForStringList("select " + dbSupport.quote(new String [] {EsusFlywayTables.getVersion()}) + " from " + table);

        List<MigrationVersion> migrationVersions = new ArrayList<MigrationVersion>();
        for (String versionStr : versions) {
            migrationVersions.add(MigrationVersion.fromVersion(versionStr));
        }

        Collections.sort(migrationVersions);

        for (int i = 0; i < migrationVersions.size(); i++) {
            if (version.compareTo(migrationVersions.get(i)) < 0) {
                return i + 1;
            }
        }

        return migrationVersions.size() + 1;
    }

    @Override
    public List<AppliedMigration> allAppliedMigrations() {
        return findAppliedMigrations();
    }

    /**
     * Retrieve the applied migrations from the metadata table.
     *
     * @param migrationTypes The specific migration types to look for. (Optional) None means find all migrations.
     * @return The applied migrations.
     */
    private List<AppliedMigration> findAppliedMigrations(MigrationType... migrationTypes) {
        if (!table.exists()) {
            return new ArrayList<AppliedMigration>();
        }

        createIfNotExists();
//        new String[] { EsusFlywayTables.get }
        String query = "SELECT " + dbSupport.quote(new String[] { EsusFlywayTables.getVersionRank() })
                + "," + dbSupport.quote(new String[] { EsusFlywayTables.getInstalledRank() })
                + "," + dbSupport.quote(new String[] { EsusFlywayTables.getVersion() })
                + "," + dbSupport.quote(new String[] { EsusFlywayTables.getDescription() })
                + "," + dbSupport.quote(new String[] { EsusFlywayTables.getType() })
                + "," + dbSupport.quote(new String[] { EsusFlywayTables.getScript() })
                + "," + dbSupport.quote(new String[] { EsusFlywayTables.getChecksum() })
                + "," + dbSupport.quote(new String[] { EsusFlywayTables.getInstalledOn() })
                + "," + dbSupport.quote(new String[] { EsusFlywayTables.getInstalledBy() })
                + "," + dbSupport.quote(new String[] { EsusFlywayTables.getExecutionTime() })
                + "," + dbSupport.quote(new String[] { EsusFlywayTables.getSuccess() })
                + " FROM " + table;

        if (migrationTypes.length > 0) {
            query += " WHERE " + dbSupport.quote(new String[] { EsusFlywayTables.getType() }) + " IN (";
            for (int i = 0; i < migrationTypes.length; i++) {
                if (i > 0) {
                    query += ",";
                }
                query += "'" + migrationTypes[i] + "'";
            }
            query += ")";
        } 

        query += " ORDER BY " + dbSupport.quote(new String[] { EsusFlywayTables.getVersionRank() });

        try {
            return jdbcTemplate.query(query, new RowMapper<AppliedMigration>() {
                public AppliedMigration mapRow(final ResultSet rs) throws SQLException {
                    Integer checksum = rs.getInt(EsusFlywayTables.getChecksum());
                    if (rs.wasNull()) {
                        checksum = null;
                    }

                    return new AppliedMigration(
                            rs.getInt(EsusFlywayTables.getVersionRank()),
                            rs.getInt(EsusFlywayTables.getInstalledRank()),
                            MigrationVersion.fromVersion(rs.getString(EsusFlywayTables.getVersion())),
                            rs.getString(EsusFlywayTables.getDescription()),
                            MigrationType.valueOf(rs.getString(EsusFlywayTables.getType())),
                            rs.getString(EsusFlywayTables.getScript()),
                            checksum,
                            rs.getTimestamp(EsusFlywayTables.getInstalledOn()),
                            rs.getString(EsusFlywayTables.getInstalledBy()),
                            rs.getInt(EsusFlywayTables.getExecutionTime()),
                            rs.getBoolean(EsusFlywayTables.getSuccess())
                    );
                }
            });
        } catch (SQLException e) {
            throw new FlywayException("Error while retrieving the list of applied migrations from metadata table "
                    + table, e);
        }
    }

    @Override
    public void addInitMarker(final MigrationVersion initVersion, final String initDescription) {
        addAppliedMigration(new AppliedMigration(initVersion, initDescription, MigrationType.INIT, initDescription, null,
                0, true));
    }

    @Override
    public void removeFailedMigrations() {
        if (!table.exists()) {
            LOG.info("Repair of metadata table " + table + " not necessary. No failed migration detected.");
            return;
        }

        createIfNotExists();

        try {
            int failedCount = jdbcTemplate.queryForInt("SELECT COUNT(*) FROM " + table
                    + " WHERE " + dbSupport.quote(new String [] {EsusFlywayTables.getSuccess()}) + "=" + dbSupport.getBooleanFalse());
            if (failedCount == 0) {
                LOG.info("Repair of metadata table " + table + " not necessary. No failed migration detected.");
                return;
            }
        } catch (SQLException e) {
            throw new FlywayException("Unable to check the metadata table " + table + " for failed migrations", e);
        }

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        try {
            jdbcTemplate.execute("DELETE FROM " + table
                    + " WHERE " + dbSupport.quote(new String [] {EsusFlywayTables.getSuccess()}) + " = " + dbSupport.getBooleanFalse());
        } catch (SQLException e) {
            throw new FlywayException("Unable to repair metadata table " + table, e);
        }

        stopWatch.stop();

        LOG.info("Metadata table " + table + " successfully repaired (execution time "
                + TimeFormat.format(stopWatch.getTotalTimeMillis()) + ").");
        LOG.info("Manual cleanup of the remaining effects the failed migration may still be required.");
    }

    @Override
    public void addSchemasMarker(final Schema[] schemas) {
        createIfNotExists();

        addAppliedMigration(new AppliedMigration(MigrationVersion.fromVersion("0"), "<< Flyway Schema Creation >>",
                MigrationType.SCHEMA, StringUtils.arrayToCommaDelimitedString(schemas), null, 0, true));
    }

    @Override
    public boolean hasSchemasMarker() {
        if (!table.exists()) {
            return false;
        }

        createIfNotExists();

        try {
            int count = jdbcTemplate.queryForInt(
                    "SELECT COUNT(*) FROM " + table + " WHERE " + dbSupport.quote(new String [] {EsusFlywayTables.getType()}) + "='SCHEMA'");
            return count > 0;
        } catch (SQLException e) {
            throw new FlywayException("Unable to check whether the metadata table " + table + " has a schema marker migration", e);
        }
    }

    @Override
    public boolean hasInitMarker() {
        if (!table.exists()) {
            return false;
        }

        createIfNotExists();

        try {
            int count = jdbcTemplate.queryForInt(
                    "SELECT COUNT(*) FROM " + table + " WHERE " + dbSupport.quote(new String [] {EsusFlywayTables.getType()}) + "='INIT'");
            return count > 0;
        } catch (SQLException e) {
            throw new FlywayException("Unable to check whether the metadata table " + table + " has an init marker migration", e);
        }
    }

    @Override
    public AppliedMigration getInitMarker() {
        List<AppliedMigration> appliedMigrations = findAppliedMigrations(MigrationType.INIT);
        return appliedMigrations.isEmpty() ? null : appliedMigrations.get(0);
    }

    @Override
    public boolean hasAppliedMigrations() {
        if (!table.exists()) {
            return false;
        }

        createIfNotExists();

        try {
            int count = jdbcTemplate.queryForInt(
                    "SELECT COUNT(*) FROM " + table + " WHERE " + dbSupport.quote(new String [] {EsusFlywayTables.getType()}) + " NOT IN ('SCHEMA', 'INIT')");
            return count > 0;
        } catch (SQLException e) {
            throw new FlywayException("Unable to check whether the metadata table " + table + " has applied migrations", e);
        }
    }

    @Override
    public void updateChecksum(MigrationVersion version, Integer checksum) {
        try {
            jdbcTemplate.update("UPDATE " + table + " SET " + dbSupport.quote(new String [] {EsusFlywayTables.getChecksum()}) + "=" + checksum
                    + " WHERE " + dbSupport.quote(new String [] {EsusFlywayTables.getVersion()}) + "='" + version + "'");
        } catch (SQLException e) {
            throw new FlywayException("Unable to update checksum in metadata table " + table
                    + " for version " + version + " to " + checksum, e);
        }
    }

    @Override
    public String toString() {
        return table.toString();
    }
}
