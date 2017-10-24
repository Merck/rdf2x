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

import com.merck.rdf2x.persistence.config.OutputConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.NotImplementedException;

import javax.naming.ConfigurationException;

/**
 * PersistorFactory creates {@link Persistor}s based on specified {@link OutputConfig}
 */
@Slf4j
public class PersistorFactory {

    public static Persistor createPersistor(OutputConfig config) {
        OutputConfig.OutputTarget target = config.getTarget();
        switch (target) {
            case DB:
                try {
                    String driverClassName = config.getDbConfig().getDriverClassName();
                    switch (driverClassName) {
                        case "org.postgresql.Driver":
                            return new DbPersistorPostgres(config.getDbConfig(), config.getSaveMode());
                        case "com.microsoft.sqlserver.jdbc.SQLServerDriver":
                            return new DbPersistorSQLServer(config.getDbConfig(), config.getSaveMode());
                    }
                } catch (ConfigurationException ignored) {
                }
                return new DbPersistor(config.getDbConfig(), config.getSaveMode());
            case CSV:
                return new CSVPersistor(config.getFileConfig(), config.getSaveMode());
            case JSON:
                return new JSONPersistor(config.getFileConfig(), config.getSaveMode());
            case ES:
                return new ElasticSearchPersistor(config.getEsConfig());
            case Preview:
                return new PreviewPersistor();
            case DataFrameMap:
                return new DataFrameMapPersistor(config.getResultMap());
            default:
                throw new NotImplementedException("Output not supported: " + config);
        }
    }
}
