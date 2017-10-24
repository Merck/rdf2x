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
import com.merck.rdf2x.test.TestSparkContextProvider;
import com.merck.rdf2x.test.TestUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.jena.sparql.core.Quad;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

/**
 * Test of {@link RdfSchemaCollector}
 */
@Slf4j
public class RdfSchemaCollectorTest extends TestSparkContextProvider {

    @Test
    public void testCollectSchema() {
        JavaRDD<Quad> quads = TestUtils.getQuadsRDD(jsc(), "rdfSchemaCollectorTest.nt");

        RdfSchemaCollector rdfSchemaCollector = new RdfSchemaCollector(
                new RdfSchemaCollectorConfig()
                        .setTypePredicates(Arrays.asList("http://www.wikidata.org/prop/direct/P31"))
                        .setSubclassPredicates(Arrays.asList("http://www.wikidata.org/prop/direct/P279")),
                jsc()
        );

        RdfSchema rdfSchema = rdfSchemaCollector.collectSchema(
                quads
        );

        IndexMap<String> typeIndex = rdfSchema.getTypeIndex();
        IndexMap<String> predicateIndex = rdfSchema.getPredicateIndex();
        Map<String, String> uriLabels = rdfSchema.getUriLabels();
        ClassGraph graph = rdfSchema.getClassGraph();

        Set<String> types = new HashSet<>(Arrays.asList(
                "http://www.wikidata.org/entity/Q3624078",
                "http://wikiba.se/ontology-beta#Statement",
                "http://www.wikidata.org/entity/Q7275",
                "http://www.wikidata.org/entity/Q6256",
                "http://wikiba.se/ontology-beta#BestRank",
                "http://www.wikidata.org/entity/Q56061"
        ));
        assertEquals("Expected type index URIs collected", types, typeIndex.getValueSet());

        Set<String> predicates = new HashSet<>(Arrays.asList(
                "http://www.wikidata.org/prop/qualifier/value/P582",
                "http://www.wikidata.org/prop/statement/P474",
                "http://www.wikidata.org/prop/qualifier/P582",
                "http://www.wikidata.org/prop/statement/P131",
                "http://www.wikidata.org/prop/qualifier/P580",
                "http://wikiba.se/ontology-beta#rank",
                "http://www.w3.org/ns/prov#wasDerivedFrom",
                "http://www.wikidata.org/prop/direct/P279",
                "http://www.wikidata.org/prop/direct/P31",
                "http://www.wikidata.org/prop/statement/P856",
                "http://www.wikidata.org/prop/direct/P474",
                "http://www.wikidata.org/prop/qualifier/value/P580",
                "http://schema.org/name",
                "http://www.wikidata.org/prop/P856",
                "http://www.w3.org/2000/01/rdf-schema#label",
                "http://www.wikidata.org/prop/P474",
                "http://www.wikidata.org/prop/P131"

        ));
        assertEquals("Expected predicate index URIs collected", predicates, predicateIndex.getValueSet());

        Map<String, String> expectedLabels = new HashMap<>();
        expectedLabels.put("http://www.wikidata.org/entity/Q3624078", "sovereign state");
        expectedLabels.put("http://www.wikidata.org/prop/statement/P474", "Code d'appel du pays");
        expectedLabels.put("http://www.wikidata.org/prop/qualifier/P582", "end time");
        expectedLabels.put("http://www.wikidata.org/prop/statement/P131", "Situé dans l'entité administrative territoriale");
        expectedLabels.put("http://www.wikidata.org/entity/Q7275", "state");
        expectedLabels.put("http://www.wikidata.org/prop/qualifier/P580", "start time");
        expectedLabels.put("http://www.wikidata.org/entity/Q6256", "country");
        expectedLabels.put("http://www.wikidata.org/prop/statement/P856", "site officiel");
        expectedLabels.put("http://www.wikidata.org/prop/direct/P474", "Code d'appel du pays");
        expectedLabels.put("http://www.wikidata.org/entity/Q56061", "administrative territorial entity");

        assertEquals("Expected URIs labels collected", expectedLabels, uriLabels);

        assertEquals("Types are subclass graph vertices", new HashSet<>(typeIndex.getIndex()), graph.getClasses());

        Set<Tuple2<String, String>> expectedEdges = new HashSet<>();
        expectedEdges.add(new Tuple2<>("http://www.wikidata.org/entity/Q7275", "http://www.wikidata.org/entity/Q3624078"));
        expectedEdges.add(new Tuple2<>("http://www.wikidata.org/entity/Q56061", "http://www.wikidata.org/entity/Q6256"));
        expectedEdges.add(new Tuple2<>("http://www.wikidata.org/entity/Q56061", "http://www.wikidata.org/entity/Q7275"));

        Set<Tuple2<String, String>> realEdges = graph.edgeSet().stream().map(edge -> new Tuple2<>(
                typeIndex.getValue(graph.getEdgeSuperclass(edge)),
                typeIndex.getValue(graph.getEdgeSubclass(edge))
        )).collect(Collectors.toSet());

        assertEquals("Expected graph edges collected", expectedEdges, realEdges);

    }


}