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

import com.merck.rdf2x.persistence.config.FileConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SaveMode;

import java.nio.file.Paths;

/**
 * JSONPersistor persists dataframes as JSON files
 */
@Slf4j
@RequiredArgsConstructor
public class JSONPersistor implements Persistor {

    private final FileConfig config;

    private final SaveMode saveMode;

    /**
     * Write a {@link DataFrame} to the specified output
     *
     * @param name name of output table
     * @param df   dataframe containing the data
     */
    @Override
    public void writeDataFrame(String name, DataFrame df) {
        String outputFolder = config.getOutputFolder();
        String outputPath = Paths.get(outputFolder, name).toString();
        log.info("Writing JSON files to folder {}", outputPath);
        df.write().mode(saveMode).json(outputPath);
    }
}
