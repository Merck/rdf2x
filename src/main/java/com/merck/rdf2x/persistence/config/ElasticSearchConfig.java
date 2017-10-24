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
import lombok.experimental.Accessors;

import javax.naming.ConfigurationException;
import java.util.HashMap;
import java.util.Map;

/**
 * EsConfig stores properties for saving to ElasticSearch
 */
@Data
@Accessors(chain = true)
public class ElasticSearchConfig {
    @Parameter(names = "--es.index", description = "ElasticSearch Index to save the output to", required = false)
    private String index;

    @Parameter(names = "--es.createIndex", arity = 1, description = "Whether to create index in case it does not exist, overrides es.index.auto.create property", required = false)
    private Boolean createIndex = true;

    /**
     * Validate the config, throw {@link ConfigurationException} on error
     *
     * @throws ConfigurationException found error
     */
    public void validate() throws ConfigurationException {
        if (index == null) {
            throw new ConfigurationException("Specify the ElasticSearch output index with --es.index");
        }
    }

    public Map<String, String> getProperties(String name) {
        Map<String, String> properties = new HashMap<>();

        properties.put("es.resource", index + "/" + name);
        if (createIndex != null) {
            properties.put("es.index.auto.create", createIndex.toString());
        }

        return properties;
    }
}
