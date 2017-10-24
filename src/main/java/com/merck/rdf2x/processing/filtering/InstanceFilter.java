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

import com.google.common.collect.Sets;
import com.merck.rdf2x.beans.IndexMap;
import com.merck.rdf2x.beans.Instance;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * InstanceFilter filters a RDD of {@link Instance}s based on specified config.
 */
@Slf4j
@RequiredArgsConstructor
public class InstanceFilter {

    private final InstanceFilterConfig config;

    /**
     * filter RDD of {@link Instance}s based on the specified config
     *
     * @param instances RDD of instances to filter
     * @param typeIndex index mapping type URIs to integers
     * @return filtered RDD of instances
     */
    public JavaRDD<Instance> filter(JavaRDD<Instance> instances, IndexMap<String> typeIndex) {
        if (config.getTypes().isEmpty()) {
            return instances;
        }
        // get indexes of accepted type URIs
        Set<Integer> acceptedTypes = config.getTypes().stream()
                .map(typeIndex::getIndex)
                .collect(Collectors.toSet());

        instances = instances.filter(instance -> !Collections.disjoint(instance.getTypes(), acceptedTypes));

        if (config.isIgnoreOtherTypes()) {
            // remove other than accepted types from each instance
            instances = instances.map(instance -> {
                Set<Integer> intersect = Sets.intersection(instance.getTypes(), acceptedTypes).immutableCopy();
                instance.getTypes().clear();
                instance.getTypes().addAll(intersect);
                return instance;
            });
        }

        return instances;
    }
}
