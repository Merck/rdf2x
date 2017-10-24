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

package com.merck.rdf2x.rdf.schema;

import com.merck.rdf2x.beans.IndexMap;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.jgrapht.experimental.dag.DirectedAcyclicGraph;
import org.jgrapht.graph.DefaultEdge;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

/**
 * RdfSchemaCollector collects a {@link RdfSchema} from a RDD of {@link Quad}s.
 */
@Slf4j
@RequiredArgsConstructor
public class RdfSchemaCollector implements Serializable {

    private static final String TYPE_URI = RDF.type.toString();
    private static final String SUBCLASS_URI = RDFS.subClassOf.toString();
    private static final String LABEL_URI = RDFS.label.toString();
    /**
     * Config storing schema collection properties
     */
    private final RdfSchemaCollectorConfig config;

    /**
     * Spark context to use
     */
    transient private final JavaSparkContext sc;

    /**
     * Collect a {@link RdfSchema} from a RDD of {@link Quad}s.
     *
     * @param quads RDD of {@link Quad}s to extract schema from
     * @return the extracted {@link RdfSchema}
     */
    public RdfSchema collectSchema(JavaRDD<Quad> quads) {
        // check if cached schema is available
        RdfSchema cachedSchema = readCachedSchema();
        if (cachedSchema != null) {
            // if the cached config is equal (only the cache path can be different)
            if (cachedSchema.getConfig().setCacheFile(config.getCacheFile()).equals(config)) {
                log.info("Using cached RDF schema.");
                return cachedSchema;
            } else {
                throw new RuntimeException("RDF schema collector config changed, remove the original cache or change the cache path.");
            }
        }
        log.info("Collecting type URI index");
        IndexMap<String> typeIndex = getTypeIndex(quads);
        log.info("Collected {} type URIs", typeIndex.size());

        log.info("Collecting predicate URI index");
        IndexMap<String> predicateIndex = getPredicateIndex(quads);
        log.info("Collected {} predicate URIs", predicateIndex.size());

        Map<String, String> labels;
        if (config.isCollectLabels()) {
            log.info("Collecting type and predicate labels");
            Set<String> URIs = new HashSet<>(typeIndex.getValues());
            URIs.addAll(predicateIndex.getValues());
            labels = getRdfsLabels(quads, URIs);
            log.info("Collected {} labels", labels.size());
        } else {
            labels = new HashMap<>();
        }

        // collect graph of subclass edges
        ClassGraph graph = collectGraph(quads, sc.broadcast(typeIndex));

        RdfSchema schema = new RdfSchema(config, graph, typeIndex, predicateIndex, labels);
        writeCachedSchema(schema);
        return schema;
    }

    private RdfSchema readCachedSchema() {
        String cachePath = config.getCacheFile();
        if (cachePath == null) {
            return null;
        }
        log.info("Looking for cached schema in {}.", cachePath);
        try {
            return (RdfSchema) sc.objectFile(cachePath, 1).first();
        } catch (Exception ignored) {
            log.info("RDF schema cache not found, collecting new one.");
        }
        return null;
    }

    private void writeCachedSchema(RdfSchema schema) {
        String cachePath = config.getCacheFile();
        if (cachePath == null) {
            return;
        }
        log.info("Writing schema to cache file {}", cachePath);
        sc.parallelize(Collections.singletonList(schema))
                .saveAsObjectFile(cachePath);
    }

    /**
     * Collect acyclic graph of subClass type information
     *
     * @param quads             RDD of {@link Quad}s to extract from
     * @param broadcastUriIndex broadcast uri index mapping URIs to integers
     * @return acyclic graph of subClass type information
     */
    private ClassGraph collectGraph(JavaRDD<Quad> quads, Broadcast<IndexMap<String>> broadcastUriIndex) {
        DirectedAcyclicGraph<Integer, DefaultEdge> graph = new DirectedAcyclicGraph<>(DefaultEdge.class);

        // add types as vertices
        broadcastUriIndex.getValue().getIndex().forEach(graph::addVertex);

        // do not look for subClass edges if not requested
        if (!config.isCollectSubclassGraph()) {
            return new ClassGraph(graph);
        }

        log.info("Collecting subclass graph");
        // get subclass information
        quads.filter(quad ->
                quad.getPredicate().isURI() &&
                        isSubClassPredicate(quad.getPredicate().getURI()) &&
                        quad.getSubject().isURI() &&
                        quad.getObject().isURI() &&
                        broadcastUriIndex.getValue().getValueSet().contains(quad.getSubject().getURI())
        )
                .map(quad -> new Tuple2<>(
                        broadcastUriIndex.getValue().getIndex(quad.getSubject().getURI()),
                        broadcastUriIndex.getValue().getIndex(quad.getObject().getURI())
                ))
                .collect()
                .forEach(fromToPair -> {
                    try {
                        // from superclass to subclass
                        graph.addDagEdge(fromToPair._2(), fromToPair._1());
                    } catch (DirectedAcyclicGraph.CycleFoundException | IllegalArgumentException e) {
                        log.error("SubClass cycle found in RDF schema.");
                        log.error("Edge skipped: {} -> {}", broadcastUriIndex.getValue().getValue(fromToPair._1()), broadcastUriIndex.getValue().getValue(fromToPair._2()));
                    }
                });

        log.info("Collected graph of {} subclass edges", graph.edgeSet().size());
        return new ClassGraph(graph);
    }

    private boolean isTypePredicate(String uri) {
        return config.getTypePredicates().contains(uri) || uri.equals(TYPE_URI);
    }

    private boolean isSubClassPredicate(String uri) {
        return config.getSubclassPredicates().contains(uri) || uri.equals(SUBCLASS_URI);
    }

    /**
     * Get index representing all type URIs as integers.
     *
     * @param quads quad RDD to be searched for type URIs
     * @return an {@link IndexMap} representing type URIs as integers
     */
    private IndexMap<String> getTypeIndex(JavaRDD<Quad> quads) {
        List<String> URIs = quads
                .filter(quad -> quad.getPredicate().isURI() &&
                        quad.getObject().isURI() &&
                        // collect URIs used as objects in type statements or subClass statements
                        (isTypePredicate(quad.getPredicate().getURI()) || isSubClassPredicate(quad.getPredicate().getURI())))
                .map(quad -> quad.getObject().getURI())
                .distinct().collect()
                .stream().collect(Collectors.toList()); // collect mutable list

        URIs.addAll(config.getAdditionalTypes());

        return new IndexMap<>(URIs);
    }

    /**
     * Get index representing all predicate URIs as integers.
     *
     * @param quads quad RDD to be searched for predicate URIs
     * @return an {@link IndexMap} representing predicate URIs as integers
     */
    private IndexMap<String> getPredicateIndex(JavaRDD<Quad> quads) {
        List<String> URIs = quads
                .filter(quad -> quad.getPredicate().isURI())
                .map(quad -> quad.getPredicate().getURI())
                .distinct().collect()
                .stream().collect(Collectors.toList()); // collect mutable list

        URIs.addAll(config.getAdditionalPredicates());

        return new IndexMap<>(URIs);
    }

    /**
     * Get map of rdfs:labels for specified URIs
     *
     * @param quads Quads to use for retrieving labels
     * @param uris  Set of URI Strings to find labels for
     * @return map of URI -&gt; rdfs:label
     */
    private Map<String, String> getRdfsLabels(JavaRDD<Quad> quads, Set<String> uris) {
        Broadcast<Set<String>> broadcastURIs = sc.broadcast(uris);
        Map<String, String> nonSerializableMap = quads.filter(quad ->
                        // filter out label predicates for specified subject URIs
                        quad.getPredicate().isURI() &&
                                quad.getPredicate().getURI().equals(LABEL_URI) &&
                                quad.getSubject().isURI() &&
                                (broadcastURIs.getValue().contains(quad.getSubject().getURI()))
                // map to pair of uri, label
        ).mapToPair(quad -> new Tuple2<>(
                quad.getSubject().getURI(),
                quad.getObject().getLiteralValue().toString()
        )).collectAsMap();

        return new HashMap<>(nonSerializableMap);
    }
}
