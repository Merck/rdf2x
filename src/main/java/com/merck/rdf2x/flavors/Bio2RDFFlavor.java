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

import com.merck.rdf2x.processing.formatting.FormatUtil;

/**
 * Bio2RDFFlavor adds some changes that might be desirable when converting Bio2RDF datasets
 */
public class Bio2RDFFlavor implements Flavor {
    /**
     * Format table or column name from URI and label
     *
     * @param URI       URI to format into name
     * @param label     rdfs:label or null if not available
     * @param maxLength maximum length of formatted name
     * @return formatted name or null if custom formatting function is not available
     */
    @Override
    public String formatName(String URI, String label, Integer maxLength) {
        // URIs for resources will be formatted as prefix_resource
        // for example http://bio2rdf.org/drugbank_vocabulary:Resource -> drugbank_resource
        final String RESOURCE_SUFFIX = "_vocabulary:Resource";
        if (URI.endsWith(RESOURCE_SUFFIX)) {
            return FormatUtil.getCleanURISuffix(URI.replace(RESOURCE_SUFFIX, "_resource"), "/", maxLength);
        }
        return null;
    }
}
