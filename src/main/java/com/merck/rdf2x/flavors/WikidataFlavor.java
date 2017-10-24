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

import com.google.common.collect.Sets;
import com.merck.rdf2x.jobs.convert.ConvertConfig;
import com.merck.rdf2x.jobs.convert.ConvertJob;
import com.merck.rdf2x.processing.schema.RelationSchemaStrategy;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.sparql.core.Quad;
import org.apache.spark.api.java.JavaRDD;
import org.eclipse.rdf4j.model.vocabulary.RDFS;

import java.util.Arrays;
import java.util.Collections;

/**
 * WikidataFlavor provides default settings and methods to improve conversion of the Wikidata RDF dump
 */
public class WikidataFlavor implements Flavor {

    private static final String WIKIDATA_PREFIX = "http://www.wikidata.org/";
    private static final String ENTITY_PREFIX = WIKIDATA_PREFIX + "entity/";
    private static final String PROPERTY_ENTITY_PREFIX = ENTITY_PREFIX + "P";
    private static final String PROPERTY_DIRECT_PREFIX = "http://www.wikidata.org/prop/direct/P";
    private static final String PROPERTY_STATEMENT_PREFIX = "http://www.wikidata.org/prop/statement/P";
    private static final String PROPERTY_QUALIFIER_PREFIX = "http://www.wikidata.org/prop/qualifier/P";

    /**
     * Set default values for {@link ConvertJob}
     *
     * @param config config to update
     */
    @Override
    public void setDefaultValues(ConvertConfig config) {
        config.getRdfSchemaCollectorConfig()
                .setSubclassPredicates(Arrays.asList(
                        "http://www.wikidata.org/prop/direct/P279" // subclass of
                ))
                .setTypePredicates(Arrays.asList(
                        "http://www.wikidata.org/prop/direct/P31", // instance of
                        "http://www.wikidata.org/prop/direct/P279" // subclass of - consider subclasses to also be instances of the class (to include instances such as Piano, which does not have 'instance of' information)
                ));
        config.getSchemaFormatterConfig()
                .setUseLabels(true)
                .setMaxTableNameLength(50);

        config.getRelationConfig()
                .setSchemaStrategy(RelationSchemaStrategy.Predicates);
    }

    /**
     * Modify RDD of quads in any needed way (filtering, flatMapping, ...)
     *
     * @param quads RDD of quads to modify
     * @return modified RDD of quads, returns original RDD in default
     */
    @Override
    public JavaRDD<Quad> modifyQuads(JavaRDD<Quad> quads) {
        final String labelURI = RDFS.LABEL.toString();
        return quads.flatMap(quad -> {
            if (quad.getSubject().isURI()) {
                String subjectURI = quad.getSubject().getURI();
                // for each quad specifying property label, create label quads for each URI variant of this property
                // done because Wikidata only provides entity labels, for example http://www.wikidata.org/entity/P279 and not http://www.wikidata.org/prop/direct/P279
                if (subjectURI.contains(PROPERTY_ENTITY_PREFIX) && quad.getPredicate().getURI().equals(labelURI)) {
                    return Sets.newHashSet(
                            quad,
                            new Quad(quad.getGraph(),
                                    NodeFactory.createURI(subjectURI.replace(PROPERTY_ENTITY_PREFIX, PROPERTY_DIRECT_PREFIX)),
                                    quad.getPredicate(),
                                    quad.getObject()),
                            new Quad(quad.getGraph(),
                                    NodeFactory.createURI(subjectURI.replace(PROPERTY_ENTITY_PREFIX, PROPERTY_STATEMENT_PREFIX)),
                                    quad.getPredicate(),
                                    quad.getObject()),
                            new Quad(quad.getGraph(),
                                    NodeFactory.createURI(subjectURI.replace(PROPERTY_ENTITY_PREFIX, PROPERTY_QUALIFIER_PREFIX)),
                                    quad.getPredicate(),
                                    quad.getObject())
                    );
                }
            }

            return Collections.singleton(quad);
        });
    }


}
