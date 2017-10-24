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

import com.merck.rdf2x.rdf.QuadUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.jena.sparql.core.Quad;
import org.apache.spark.api.java.JavaRDD;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * QuadFilter filters a RDD of {@link Quad}s based on a specified config.
 */
@Slf4j
@RequiredArgsConstructor
public class QuadFilter {

    private final QuadFilterConfig config;

    /**
     * filter RDD of {@link Quad}s based on the specified config
     *
     * @param quads RDD of quads to filter
     * @return filtered RDD of quads
     */
    public JavaRDD<Quad> filter(JavaRDD<Quad> quads) {
        Set<String> subjectBlacklist = new HashSet<>(config.getResourceBlacklist());
        if (config.getResources().isEmpty()) {
            return QuadUtils.filterQuadsByForbiddenSubjects(quads, subjectBlacklist);
        }
        log.info("Filtering quads");
        Set<String> subjects = new HashSet<>(config.getResources());
        boolean directed = config.isDirected();
        for (int d = 0; d < config.getRelatedDepth(); d++) {
            log.info("Depth {}, collecting neighbors of {} resources", d, subjects.size());
            List<String> neighbors = QuadUtils.getNeighborResources(quads, subjects, directed).collect();
            subjects.addAll(neighbors);
            subjects.removeAll(subjectBlacklist);
        }
        log.info("Filtering on an in-memory set of {} subjects", subjects.size());
        quads = QuadUtils.filterQuadsByAllowedSubjects(quads, subjects);
        return quads;
    }
}
