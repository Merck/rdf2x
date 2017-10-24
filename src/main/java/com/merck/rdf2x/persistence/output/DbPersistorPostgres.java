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
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils;
import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;
import scala.Function0;
import scala.reflect.ClassTag$;

import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

@Slf4j
public class DbPersistorPostgres extends DbPersistor {

    private static final String COPY_DELIMITER = "|";
    private static final String COPY_NULL_STRING = "NULL";

    public DbPersistorPostgres(DbConfig config, SaveMode saveMode) {
        super(config, saveMode);
    }

    @Override
    public void writeDataFrame(String name, DataFrame df) {
        if (!config.isBulkLoad()) {
            super.writeDataFrame(name, df);
            return;
        }
        String fullTableName = getFullTableName(name);
        Properties properties = config.getProperties();

        // create table schema by persisting empty dataframe
        log.info("Creating schema of table {}", fullTableName);
        SQLContext sql = df.sqlContext();
        DataFrame emptyDf = sql.createDataFrame(sql.sparkContext().emptyRDD(ClassTag$.MODULE$.apply(Row.class)), df.schema());
        emptyDf.write().mode(saveMode).jdbc(config.getUrl(), fullTableName, properties);

        final Function0<Connection> connectionFactory = JdbcUtils.createConnectionFactory(config.getUrl(), properties);
        log.info("Writing to database table {} using PostgreSQL COPY", fullTableName);
        int batchSize = config.getBatchSize();
        df.toJavaRDD().foreachPartition(rows -> {
            Connection connection = connectionFactory.apply();
            copyRows(fullTableName, rows, connection, batchSize);
            try {
                connection.close();
            } catch (SQLException e) {
                log.debug("Unexpected exception when closing database connection: {}", e);
            }
        });

    }

    private static void copyRows(String fullTableName, Iterator<Row> rows, Connection connection, int batchSize) {
        try {
            CopyManager cm = new CopyManager((org.postgresql.core.BaseConnection) connection);
            CopyIn copyIn = cm.copyIn("COPY " + fullTableName + " FROM STDIN WITH (NULL '" + COPY_NULL_STRING + "', FORMAT CSV, DELIMITER E'" + COPY_DELIMITER + "', QUOTE E'\"')");
            int i = 0;
            StringBuilder buffer = new StringBuilder();
            // write rows to CopyManager in batches of batchSize
            while (rows.hasNext()) {
                StringBuilder line = rowToCsvLine(rows.next());
                buffer.append(line);
                i++;
                if (i >= batchSize) {
                    copyBatch(copyIn, buffer);
                    i = 0;
                }
            }
            if (buffer.length() > 0) {
                copyBatch(copyIn, buffer);
            }
            copyIn.endCopy();
        } catch (SQLException e) {
            throw new RuntimeException("Unable to perform PostgreSQL COPY", e);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("UTF-8 encoding not supported, cannot perform PostgreSQL COPY", e);
        }
    }

    private static void copyBatch(CopyIn copyIn, StringBuilder buffer) throws UnsupportedEncodingException, SQLException {
        byte[] bytes = buffer.toString().getBytes("UTF-8");
        buffer.setLength(0);
        log.debug("Writing batch of {} MB", String.format("%.2f", bytes.length / 1000000.0));
        copyIn.writeToCopy(bytes, 0, bytes.length);
    }

    private static StringBuilder rowToCsvLine(Row row) {
        StringBuilder line = new StringBuilder();
        for (int i = 0; i < row.length(); i++) {
            if (i != 0) {
                line.append("|");
            }
            if (row.get(i) == null) {
                line.append(COPY_NULL_STRING);
                continue;
            }
            line.append('"');
            line.append(valueToCopyString(row.get(i)));
            line.append('"');
        }
        line.append("\n");
        return line;
    }

    private static String valueToCopyString(Object o) {
        if (o instanceof String) {
            String s = (String) o;
            //s = s.replace("\\", "\\\\"); // turn \ into \\
            s = s.replace("\"", "\"\""); // turn " into ""
            // s = s.replace("\n", "\\n"); // turn newline into \n
            // s = s.replace("\r", "\\r"); // turn carriage return into \r
            return s;
        }
        return o.toString();
    }

    /**
     * Get reserved names that should not be used (case insensitive)
     *
     * @return reserved names that should not be used (case insensitive)
     */
    @Override
    public Collection<String> getReservedNames() {
        return new HashSet<>(Arrays.asList(
                "ADMIN", "ALIAS", "ALL", "ALLOCATE", "ANALYSE", "ANALYZE", "AND", "ANY",
                "ARE", "ARRAY", "AS", "ASC", "AUTHORIZATION", "BINARY", "BLOB", "BOTH", "BREADTH",
                "CALL", "CASCADED", "CASE", "CAST", "CATALOG", "CHECK", "CLOB", "COLLATE",
                "COLLATION", "COLUMN", "COMPLETION", "CONNECT", "CONNECTION", "CONSTRAINT",
                "CONSTRUCTOR", "CONTINUE", "CORRESPONDING", "CREATE", "CROSS", "CUBE", "CURRENT",
                "CURRENT_DATE", "CURRENT_PATH", "CURRENT_ROLE", "CURRENT_TIME", "CURRENT_TIMESTAMP",
                "CURRENT_USER", "DATE", "DEFAULT", "DEFERRABLE", "DEPTH", "DEREF", "DESC", "DESCRIBE",
                "DESCRIPTOR", "DESTROY", "DESTRUCTOR", "DETERMINISTIC", "DIAGNOSTICS", "DICTIONARY",
                "DISCONNECT", "DISTINCT", "DO", "DYNAMIC", "ELSE", "END", "END-EXEC", "EQUALS", "EVERY",
                "EXCEPT", "EXCEPTION", "EXEC", "FALSE", "FIRST", "FOR", "FOREIGN", "FOUND", "FREE",
                "FREEZE", "FROM", "FULL", "GENERAL", "GO", "GOTO", "GRANT", "GROUP", "GROUPING",
                "HAVING", "HOST", "IDENTITY", "IGNORE", "ILIKE", "IN", "INDICATOR", "INITIALIZE",
                "INITIALLY", "INNER", "INTERSECT", "INTO", "IS", "ISNULL", "ITERATE", "JOIN",
                "LARGE", "LAST", "LATERAL", "LEADING", "LEFT", "LESS", "LIKE", "LIMIT", "LOCALTIME",
                "LOCALTIMESTAMP", "LOCATOR", "MAP", "MODIFIES", "MODIFY", "MODULE", "NATURAL",
                "NCLOB", "NEW", "NOT", "NOTNULL", "NULL", "OBJECT", "OFF", "OFFSET", "OLD", "ON",
                "ONLY", "OPEN", "OPERATION", "OR", "ORDER", "ORDINALITY", "OUTER", "OUTPUT", "PAD",
                "PARAMETER", "PARAMETERS", "PLACING", "POSTFIX", "PREFIX", "PREORDER", "PRESERVE",
                "PRIMARY", "PUBLIC", "READS", "RECURSIVE", "REF", "REFERENCES", "REFERENCING",
                "RESULT", "RETURN", "RIGHT", "ROLE", "ROLLUP", "ROUTINE", "ROWS", "SAVEPOINT",
                "SCOPE", "SEARCH", "SECTION", "SELECT", "SESSION_USER", "SETS", "SIZE", "SOME",
                "SPACE", "SPECIFIC", "SPECIFICTYPE", "SQL", "SQLCODE", "SQLERROR", "SQLEXCEPTION",
                "SQLSTATE", "SQLWARNING", "STATE", "STATIC", "STRUCTURE", "SYSTEM_USER", "TABLE",
                "TERMINATE", "THAN", "THEN", "TIMEZONE_HOUR", "TIMEZONE_MINUTE", "TO", "TRAILING",
                "TRANSLATION", "TRUE", "UNDER", "UNION", "UNIQUE", "UNNEST", "USER", "USING", "VALUE",
                "VARIABLE", "VERBOSE", "WHEN", "WHENEVER", "WHERE", "ZONE"
        ));
    }
}
