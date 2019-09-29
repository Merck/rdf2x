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

package com.merck.rdf2x.stats;

import org.apache.jena.sparql.core.Quad;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

/**
 * QuadCounter provides methods for counting {@link Quad}s
 */
public class QuadCounter {

    public static JavaPairRDD<String, Long> countBySubjectURI(JavaRDD<Quad> quads) {
        return quads
                .filter(quad -> quad.getSubject().isURI())
                .mapToPair(quad -> new Tuple2<>(quad.getSubject().getURI(), 1L))
                .reduceByKey((a, b) -> a + b);
    }

    public static JavaPairRDD<String, Long> countByPredicateURI(JavaRDD<Quad> quads) {
        return quads
                .filter(quad -> quad.getPredicate().isURI())
                .mapToPair(quad -> new Tuple2<>(quad.getPredicate().getURI(), 1L))
                .reduceByKey((a, b) -> a + b);
    }

    public static JavaPairRDD<String, Long> getObjectURI(JavaRDD<Quad> quads) {
        return quads
                .filter(quad -> quad.getObject().isURI())
                .mapToPair(quad -> new Tuple2<>(quad.getObject().getURI(), 1L))
                .reduceByKey((a, b) -> a + b);
    }

}
