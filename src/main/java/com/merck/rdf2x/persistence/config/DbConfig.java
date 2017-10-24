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

package com.merck.rdf2x.persistence.config;

import com.beust.jcommander.Parameter;
import lombok.Data;
import lombok.ToString;
import lombok.experimental.Accessors;

import javax.naming.ConfigurationException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * DbConfig stores database connection parameters
 */
@Data
@Accessors(chain = true)
@ToString(exclude = "password")
public class DbConfig {
    @Parameter(names = "--db.url", description = "Database JDBC string")
    private String url;

    @Parameter(names = "--db.user", description = "Database user")
    private String user;

    @Parameter(names = "--db.password", description = "Database password")
    private String password;

    @Parameter(names = "--db.schema", description = "Database schema name")
    private String schema = null;

    @Parameter(names = "--db.batchSize", description = "Insert batch size (number of batched insert statements or number of lines in a CSV batch)")
    private Integer batchSize = 5000;

    @Parameter(names = "--db.bulkLoad", arity = 1, description = "Use CSV bulk load if possible (PostgreSQL COPY)")
    private boolean bulkLoad = true;

    /**
     * Validate the config, throw {@link ConfigurationException} on error
     *
     * @throws ConfigurationException found error
     */
    public void validate() throws ConfigurationException {
        if (url == null) {
            throw new ConfigurationException("Specify JDBC url with --db.url");
        }
        if (user == null) {
            throw new ConfigurationException("Specify DB user with --db.user");
        }
        getDriverClassName();
    }

    /**
     * Prepare Properties object for JDBC Connector
     *
     * @return prepared Properties object for JDBC Connector
     */
    public Properties getProperties() {
        Properties dbProperties = new Properties();

        try {
            // specify driver class name, required by Spark to register it on all executors
            dbProperties.put("driver", getDriverClassName());
        } catch (ConfigurationException ignored) {
            // already checked during validation
        }
        dbProperties.put("tcpKeepAlive", "true");
        dbProperties.put("connectTimeout", "0");
        dbProperties.put("socketTimeout", "0");
        dbProperties.setProperty("user", user);
        dbProperties.setProperty("password", password);
        dbProperties.setProperty("batchsize", batchSize.toString());
        if (schema != null) {
            dbProperties.put("searchpath", schema);
            dbProperties.put("currentSchema", schema);
        }
        return dbProperties;
    }

    public String getDriverClassName() throws ConfigurationException {
        try {
            return DriverManager.getDriver(url).getClass().getName();
        } catch (SQLException e) {
            throw new ConfigurationException("Driver not found for JDBC url '" + url + "', probably due to missing dependencies");
        }
    }
}
