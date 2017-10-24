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
import com.beust.jcommander.ParametersDelegate;
import com.merck.rdf2x.persistence.InstanceRelationWriter;
import lombok.Data;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SaveMode;

import javax.naming.ConfigurationException;
import java.util.Map;

/**
 * DataFrameWriterConfig stores parameters for the {@link InstanceRelationWriter}.
 */
@Data
@Accessors(chain = true)
public class OutputConfig {

    @Parameter(names = "--output.target", description = "Where to output the result (DB, CSV, JSON, ES, Preview).", required = true)
    private OutputTarget target;

    @Parameter(names = "--output.saveMode", description = "How to handle existing tables (Append, Overwrite, ErrorIfExists, Ignore).")
    private SaveMode saveMode = SaveMode.ErrorIfExists;

    @ParametersDelegate
    private DbConfig dbConfig = new DbConfig();

    @ParametersDelegate
    private FileConfig fileConfig = new FileConfig();

    @ParametersDelegate
    private ElasticSearchConfig esConfig = new ElasticSearchConfig();

    /**
     * Map of DataFrames to use with DataFrameMap output mode
     */
    private Map<String, DataFrame> resultMap;

    @Override
    public String toString() {
        if (target == null) {
            return "NO TARGET SPECIFIED";
        }
        switch (target) {
            case DB:
                return target + "=" + dbConfig.toString();
            case ES:
                return target + "=" + esConfig.toString();
            case CSV:
            case JSON:
                return target + "=" + fileConfig.toString();

        }
        return target.toString();
    }

    /**
     * Validate the config, throw {@link ConfigurationException} on error
     *
     * @throws ConfigurationException found error
     */
    public void validate() throws ConfigurationException {
        switch (target) {
            case DB:
                dbConfig.validate();
                break;
            case CSV:
            case JSON:
                fileConfig.validate();
                break;
            case ES:
                esConfig.validate();
                break;
        }
    }

    /**
     * create new output config to a DB
     *
     * @param dbConfig DB config
     * @return create new output config to a DB
     */
    public static OutputConfig toDB(DbConfig dbConfig) {
        return new OutputConfig().setTarget(OutputTarget.DB).setDbConfig(dbConfig);
    }

    /**
     * create new CSV output config to a folder
     *
     * @param outputFolder Folder to output the files to
     * @return new CSV output config to a folder
     */
    public static OutputConfig toCSV(String outputFolder) {
        return new OutputConfig().setTarget(OutputTarget.CSV).setFileConfig(
                new FileConfig().setOutputFolder(outputFolder)
        );
    }

    /**
     * create new JSON output config to a folder
     *
     * @param outputFolder Folder to output the files to
     * @return new JSON output config to a folder
     */
    public static OutputConfig toJSON(String outputFolder) {
        return new OutputConfig().setTarget(OutputTarget.JSON).setFileConfig(
                new FileConfig().setOutputFolder(outputFolder)
        );
    }

    /**
     * create new ElasticSearch output config to an ElasticSearch index
     *
     * @param outputIndex ElasticSearch index to save the output to
     * @return new ElasticSearch output config to an ElasticSearch index
     */
    public static OutputConfig toElasticSearch(String outputIndex) {
        return new OutputConfig().setTarget(OutputTarget.ES).setEsConfig(
                new ElasticSearchConfig().setIndex(outputIndex)
        );
    }

    /**
     * create new output config saving results to a map of DataFrames
     *
     * @param map map to save the DataFrames to
     * @return new output config saving results to a map of DataFrames
     */
    public static OutputConfig toMap(@NonNull Map<String, DataFrame> map) {
        return new OutputConfig().setTarget(OutputTarget.DataFrameMap).setResultMap(map);
    }

    /**
     * create new preview output
     *
     * @return new preview output
     */
    public static OutputConfig toPreview() {
        return new OutputConfig().setTarget(OutputTarget.Preview);
    }


    /**
     * OutputTarget stores the output target type
     */
    public enum OutputTarget {
        /**
         * Output the result to a database via JDBC
         */
        DB,
        /**
         * Output the result as CSV files to a folder on the disk
         */
        CSV,
        /**
         * Output the result as JSON files to a folder on the disk
         */
        JSON,
        /**
         * Output the result into ElasticSearch
         */
        ES,
        /**
         * Don't output anywhere, just print the top rows to console
         */
        Preview,
        /**
         * Save to a map of DataFrames (programmatic API only)
         */
        DataFrameMap
    }
}
