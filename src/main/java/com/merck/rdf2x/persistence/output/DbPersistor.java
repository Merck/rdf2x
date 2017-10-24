/*
 * Copyright 2017 Merck Sharp & Dohme Corp. a subsidiary of Merck & Co.,
 * Inc., Kenilworth, NJ, USA.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.merck.rdf2x.persistence.output;

import com.merck.rdf2x.persistence.config.DbConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SaveMode;
import scala.Tuple2;
import scala.Tuple4;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * DbPersistor persists dataframes to a database via JDBC
 */
@Slf4j
@RequiredArgsConstructor
public class DbPersistor implements Persistor {

    /**
     * config storing JDBC connection properties
     */
    protected final DbConfig config;

    /**
     * save mode defines what to do when file exists
     */
    protected final SaveMode saveMode;

    @Override
    public void writeDataFrame(String name, DataFrame df) {
        String fullTableName = getFullTableName(name);

        Properties properties = config.getProperties();
        log.info("Writing to database table {} using batched inserts", fullTableName);
        df.write().mode(saveMode).jdbc(config.getUrl(), fullTableName, properties);
    }

    /**
     * Get table name including schema prefix (or original table name if prefix is null)
     *
     * @param tableName table name to prepend with schema prefix
     * @return table name including schema prefix (or original table name if prefix is null)
     */
    protected String getFullTableName(String tableName) {
        String schemaName = config.getSchema();
        return schemaName == null ? tableName : schemaName + "." + tableName;
    }

    /**
     * Create index on specified columns (if applicable for given output)
     *
     * @param tableColumnPairs collection of table, column pairs to create index for
     */
    public void createIndexes(Collection<Tuple2<String, String>> tableColumnPairs) {
        List<String> statements = tableColumnPairs.stream()
                .map(tuple -> getCreateIndexStatement(tuple._1(), tuple._2()))
                .collect(Collectors.toList());
        try {
            executeStatements(statements);
        } catch (PersistException e) {
            log.error("Unable to create indexes");
        }
    }

    /**
     * Create primary keys on specified columns (if applicable for given output)
     *
     * @param tableColumnPairs collection of table, column pairs to create primary keys for
     */
    public void createPrimaryKeys(Collection<Tuple2<String, String>> tableColumnPairs) {
        List<String> statements = tableColumnPairs.stream()
                .map(tuple -> getCreatePrimaryKeyStatement(tuple._1(), tuple._2()))
                .collect(Collectors.toList());
        try {
            executeStatements(statements);
        } catch (PersistException e) {
            log.error("Unable to create primary keys");
        }
    }

    /**
     * Create foreign keys on specified columns (if applicable for given output)
     *
     * @param tableColumnPairs collection of (fromTableName, fromTableColumn, toTableName, toTableColumn) tuples
     */
    public void createForeignKeys(Collection<Tuple4<String, String, String, String>> tableColumnPairs) {
        List<String> statements = tableColumnPairs.stream()
                .map(tuple -> getCreateForeignKeyStatement(tuple._1(), tuple._2(), tuple._3(), tuple._4()))
                .collect(Collectors.toList());
        try {
            executeStatements(statements);
        } catch (PersistException e) {
            log.error("Unable to create foreign keys");
        }
    }

    private String getCreatePrimaryKeyStatement(String tableName, String column) {
        return String.format("ALTER TABLE %s ADD PRIMARY KEY (%s);", getFullTableName(tableName), column);
    }

    private String getCreateIndexStatement(String tableName, String column) {
        return String.format("CREATE INDEX ON %s (%s);", getFullTableName(tableName), column);
    }

    private String getCreateForeignKeyStatement(String fromTableName, String fromColumn, String toTableName, String toColumn) {
        return String.format(
                "ALTER TABLE %s ADD FOREIGN KEY (%s) REFERENCES %s(%s);",
                getFullTableName(fromTableName),
                fromColumn,
                getFullTableName(toTableName),
                toColumn);
    }

    /**
     * Execute a collection of SQL statements on a JDBC
     *
     * @param statements set of SQL statements to execute
     */
    public void executeStatements(Collection<String> statements) throws PersistException {
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = DriverManager.getConnection(config.getUrl(), config.getProperties());
            stmt = conn.createStatement();
            for (String sql : statements) {
                stmt.execute(sql);
            }

        } catch (SQLException e) {
            throw new PersistException("Unable to execute statements", e);
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    log.warn("Error closing statement", e);
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    log.warn("Error closing connection", e);
                }
            }
        }
    }
}
