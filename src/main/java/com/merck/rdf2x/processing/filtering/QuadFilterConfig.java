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
package com.merck.rdf2x.processing.filtering;

import com.beust.jcommander.Parameter;
import lombok.Data;
import lombok.experimental.Accessors;

import javax.naming.ConfigurationException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * QuadFilterConfig stores all information for the {@link QuadFilter}.
 */
@Data
@Accessors(chain = true)
public class QuadFilterConfig implements Serializable {

    @Parameter(names = "--filter.resource", description = "Accept resources of specified URI. More resource URIs can be specified by repeating this parameter.")
    private List<String> resources = new ArrayList<>();

    @Parameter(names = "--filter.resourceBlacklist", description = "Ignore resources of specified URI. More resource URIs can be specified by repeating this parameter.")
    private List<String> resourceBlacklist = new ArrayList<>();

    @Parameter(names = "--filter.relatedDepth", description = "Accept also resources related to the original set in relatedDepth directed steps. Uses an in-memory set of subject URIs, therefore can only be used for small results (e.g. less than 1 million resources selected).")
    private Integer relatedDepth = 0;

    @Parameter(names = "--filter.directed", arity = 1, description = "Whether to traverse only in the subject->object directions of relations when retrieving related resources.")
    private boolean directed = true;

    /**
     * Validate the config, throw {@link ConfigurationException} on error
     *
     * @throws ConfigurationException found error
     */
    public void validate() throws ConfigurationException {
        if (relatedDepth > 0 && resources.isEmpty()) {
            throw new ConfigurationException("RelatedDepth > 0 has no effect when no filter resources are specified.");
        }
    }

    public QuadFilterConfig addResources(String... resources) {
        this.resources.addAll(Arrays.asList(resources));
        return this;
    }

    public QuadFilterConfig addResourceBlacklist(String... resourceBlacklist) {
        this.resourceBlacklist.addAll(Arrays.asList(resourceBlacklist));
        return this;
    }
}
