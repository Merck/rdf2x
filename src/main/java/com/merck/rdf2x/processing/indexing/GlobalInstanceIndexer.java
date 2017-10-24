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

package com.merck.rdf2x.processing.indexing;

import com.merck.rdf2x.beans.Instance;
import org.apache.spark.api.java.JavaRDD;

import java.io.Serializable;

/**
 * GlobalPredicateMapIndexer adds a globally unique ID to a RDD of {@link Instance}s.
 */
public class GlobalInstanceIndexer implements InstanceIndexer, Serializable {

    /**
     * Add a globally unique index to each {@link Instance} in a RDD, starting from 1.
     *
     * @param instances RDD of {@link Instance}s
     * @return the modified RDD of {@link Instance}s with IDs
     */
    public JavaRDD<Instance> addIDs(JavaRDD<Instance> instances) {
        return instances.zipWithUniqueId().map(instanceWithId -> {
            Instance instance = instanceWithId._1();
            instance.setId(instanceWithId._2() + 1);
            return instance;
        });
    }
}
