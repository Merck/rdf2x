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
import scala.Tuple2;
import scala.Tuple4;

import java.util.Collection;
import java.util.Collections;

/**
 * OutputWriter writes {@link DataFrame}s in various output targets.
 */
public interface Persistor {

    /**
     * Write a {@link DataFrame} to the specified output
     *
     * @param name name of output table
     * @param df   dataframe containing the data
     */
    void writeDataFrame(String name, DataFrame df);

    /**
     * Create index on specified columns (if applicable for given output)
     *
     * @param tableColumnPairs collection of table, column pairs to create index for
     */
    default void createIndexes(Collection<Tuple2<String, String>> tableColumnPairs) {
    }

    /**
     * Create primary keys on specified columns (if applicable for given output)
     *
     * @param tableColumnPairs collection of table, column pairs to create primary keys for
     */
    default void createPrimaryKeys(Collection<Tuple2<String, String>> tableColumnPairs) {
    }

    /**
     * Create foreign keys on specified columns (if applicable for given output)
     *
     * @param tableColumnPairs collection of (fromTableName, fromTableColumn, toTableName, toTableColumn) tuples
     */
    default void createForeignKeys(Collection<Tuple4<String, String, String, String>> tableColumnPairs) {
    }

    /**
     * Get reserved names that should not be used (case insensitive)
     *
     * @return reserved names that should not be used (case insensitive)
     */
    default Collection<String> getReservedNames() {
        return Collections.emptySet();
    }
}
