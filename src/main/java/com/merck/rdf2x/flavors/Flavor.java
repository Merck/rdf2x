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

package com.merck.rdf2x.flavors;

import com.merck.rdf2x.jobs.convert.ConvertConfig;
import com.merck.rdf2x.jobs.convert.ConvertJob;
import com.merck.rdf2x.jobs.stats.StatsConfig;
import com.merck.rdf2x.jobs.stats.StatsJob;
import org.apache.jena.sparql.core.Quad;
import org.apache.spark.api.java.JavaRDD;

import java.io.Serializable;

/**
 * Modifiers are classes used to introduce specific settings and methods for different data sources.
 */
public interface Flavor extends Serializable {

    /**
     * Set default values for {@link StatsJob}
     *
     * @param config config to update
     */
    default void setDefaultValues(StatsConfig config) {

    }

    /**
     * Set default values for {@link ConvertJob}
     *
     * @param config config to update
     */
    default void setDefaultValues(ConvertConfig config) {

    }

    /**
     * Modify RDD of quads in any needed way (filtering, flatMapping, ...)
     *
     * @param quads RDD of quads to modify
     * @return modified RDD of quads, returns original RDD in default
     */
    default JavaRDD<Quad> modifyQuads(JavaRDD<Quad> quads) {
        return quads;
    }

    /**
     * Format table or column name from URI and label
     *
     * @param URI       URI to format into name
     * @param label     rdfs:label or null if not available
     * @param maxLength maximum length of formatted name
     * @return formatted name or null if custom formatting function is not available
     */
    default String formatName(String URI, String label, Integer maxLength) {
        return null;
    }
}
