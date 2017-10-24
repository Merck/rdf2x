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

package com.merck.rdf2x.rdf;

import org.apache.jena.sparql.core.Quad;
import org.apache.spark.api.java.JavaRDD;
import org.eclipse.rdf4j.model.vocabulary.RDF;

import java.util.List;
import java.util.Set;

/**
 * QuadUtils stores various methods for working with RDDs of {@link Quad}s.
 */
public class QuadUtils {

    /**
     * Get URIs of resources that belong to at least one of the specified types
     *
     * @param quads             RDD of quads to filter
     * @param types             set of requested type URIs
     * @param typePredicateURIs additional type predicates to use together with rdf:type
     * @return RDD of URIs of resources that belong to at least one of the specified types
     */
    public static JavaRDD<String> getSubjectsOfType(JavaRDD<Quad> quads, Set<String> types, List<String> typePredicateURIs) {
        return filterTypeQuads(quads, typePredicateURIs)
                .filter(quad -> types.contains(quad.getObject().getURI()) &&
                        quad.getSubject().isURI()
                ).map(quad -> quad.getSubject().getURI());
    }

    /**
     * Get resources related to specified resources, computed by querying an in-memory set of subjects
     *
     * @param quads       RDD of quads to filter
     * @param subjectURIs set of requested subject URIs to grow from
     * @param directed    whether to use both directions of relations
     * @return URIs of resources related to specified resources
     */
    public static JavaRDD<String> getNeighborResources(JavaRDD<Quad> quads, Set<String> subjectURIs, boolean directed) {
        JavaRDD<String> neighbors = filterQuadsByAllowedSubjects(quads, subjectURIs)
                .filter(quad -> quad.getObject().isURI())
                .map(quad -> quad.getObject().getURI());
        if (!directed) {
            neighbors = neighbors.union(filterQuadsByObjects(quads, subjectURIs)
                    .filter(quad -> quad.getSubject().isURI())
                    .map(quad -> quad.getSubject().getURI()));
        }
        return neighbors;
    }

    /**
     * Get quads with specified subjects only, computed by querying an in-memory set of subjects
     *
     * @param quads       RDD of quads to filter
     * @param subjectURIs set of requested subject URIs to keep
     * @return filtered RDD with only those quads whose subject is in subjectURIs
     */
    public static JavaRDD<Quad> filterQuadsByAllowedSubjects(JavaRDD<Quad> quads, Set<String> subjectURIs) {
        return quads.filter(quad -> quad.getSubject().isURI() &&
                subjectURIs.contains(quad.getSubject().getURI())
        );
    }

    /**
     * Get quads with specified subjects filtered out, computed by querying an in-memory set of subjects
     *
     * @param quads            RDD of quads to filter
     * @param subjectBlacklist set of requested subject URIs to be filtered out
     * @return filtered RDD with only those quads whose subject is NOT in subjectBlacklist
     */
    public static JavaRDD<Quad> filterQuadsByForbiddenSubjects(JavaRDD<Quad> quads, Set<String> subjectBlacklist) {
        if (subjectBlacklist.isEmpty()) {
            return quads;
        }
        return quads.filter(quad -> !quad.getSubject().isURI() ||
                !subjectBlacklist.contains(quad.getSubject().getURI())
        );
    }

    /**
     * Get quads with specified object URIs, computed by querying an in-memory set of subjects
     *
     * @param quads      RDD of quads to filter
     * @param objectURIs set of requested object URIs to filter on
     * @return filtered RDD with only those quads whose object is in objectURIs
     */
    public static JavaRDD<Quad> filterQuadsByObjects(JavaRDD<Quad> quads, Set<String> objectURIs) {
        return quads.filter(quad -> quad.getObject().isURI() &&
                objectURIs.contains(quad.getObject().getURI())
        );
    }

    /**
     * Get quads that specify type of a resource
     *
     * @param quads             RDD of quads to filter
     * @param typePredicateURIs additional type predicates to use together with rdf:type
     * @return RDD of quads that specify type of a resource
     */
    public static JavaRDD<Quad> filterTypeQuads(JavaRDD<Quad> quads, List<String> typePredicateURIs) {
        String typePredicateURI = RDF.TYPE.toString();

        return quads.filter(quad -> {
            if (!quad.getPredicate().isURI() || !quad.getObject().isURI()) {
                return false;
            }
            String uri = quad.getPredicate().getURI();
            return uri.equals(typePredicateURI) || typePredicateURIs.contains(uri);
        });
    }

}
