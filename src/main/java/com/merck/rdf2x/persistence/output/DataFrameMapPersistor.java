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

import org.apache.spark.sql.DataFrame;

import java.util.HashMap;
import java.util.Map;

/**
 * DataFrameMapPersistor persists dataframes directly to a map
 */
public class DataFrameMapPersistor implements Persistor {

    /**
     * Map of DataFrames to use with DataFrameMap output mode
     */
    private final Map<String, DataFrame> resultMap;

    public DataFrameMapPersistor() {
        this(new HashMap<>());
    }

    public DataFrameMapPersistor(Map<String, DataFrame> resultMap) {
        this.resultMap = resultMap;
    }

    /**
     * Write a {@link DataFrame} to the specified output
     *
     * @param name name of output table
     * @param df   dataframe containing the data
     */
    @Override
    public void writeDataFrame(String name, DataFrame df) {
        resultMap.put(name, df);
    }

    public Map<String, DataFrame> getResultMap() {
        return resultMap;
    }
}
