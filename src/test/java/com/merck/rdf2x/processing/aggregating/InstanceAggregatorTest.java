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

package com.merck.rdf2x.processing.aggregating;

import com.google.common.collect.Sets;
import com.merck.rdf2x.beans.IndexMap;
import com.merck.rdf2x.beans.Instance;
import com.merck.rdf2x.beans.Predicate;
import com.merck.rdf2x.beans.RelationPredicate;
import com.merck.rdf2x.rdf.LiteralType;
import com.merck.rdf2x.rdf.schema.ClassGraph;
import com.merck.rdf2x.rdf.schema.RdfSchema;
import com.merck.rdf2x.rdf.schema.RdfSchemaCollectorConfig;
import com.merck.rdf2x.test.TestSparkContextProvider;
import com.merck.rdf2x.test.TestUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.util.*;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test of {@link InstanceAggregator}
 */
@Slf4j
public class InstanceAggregatorTest extends TestSparkContextProvider implements Serializable {

    private RdfSchema schema;
    private IndexMap<String> uriIndex;

    @Before
    public void setUp() {
        List<String> URIs = new ArrayList<>(Arrays.asList(
                "http://t.com/types#Flavor",
                "http://t.com/types#Vegetable",
                "http://t.com/types#Fruit",
                "http://t.com/types#Root",
                "http://t.com/rel#HasFlavor",
                "http://another.com/rel#Weight",
                "http://t.com/rel#Weight",
                "http://t.com/rel#IsTasty",
                "http://t.com/rel#Color"
        ));
        URIs.addAll(InstanceAggregator.getSpecialTypes());
        URIs.addAll(InstanceAggregator.getSpecialPredicates());
        uriIndex = new IndexMap<>(URIs);

        ClassGraph mockedClassGraph = mock(ClassGraph.class);

        Set<Integer> mockedAncestors = Sets.newHashSet(uriIndex.getIndex("http://t.com/types#Root"));
        when(mockedClassGraph.getSuperClasses(any(Integer.class)))
                .thenReturn(mockedAncestors);

        schema = new RdfSchema(
                new RdfSchemaCollectorConfig(),
                mockedClassGraph,
                uriIndex,
                uriIndex,
                null
        );
    }

    /**
     * Test if expected Instances are aggregated from input Quads
     */
    @Test
    public void testCreateInstancesWithoutSuperTypes() {
        InstanceAggregatorConfig config = new InstanceAggregatorConfig()
                .setDefaultLanguage("en")
                .setAddSuperTypes(false);
        InstanceAggregator collector = new InstanceAggregator(config, jsc().broadcast(schema));
        JavaRDD<Instance> result = collector.aggregateInstances(TestUtils.getQuadsRDD(jsc(), "aggregatorTest.nq")).cache();

        result = checkErrorInstance(result);

        assertRDDEquals("Aggregated instances without super types are equal to expected RDD.", getExpectedRDD(false), result);
    }

    /**
     * Test if expected Instances (with added super types) are aggregated from input Quads
     */
    @Test
    public void testCreateInstancesWithSuperTypes() {
        InstanceAggregatorConfig config = new InstanceAggregatorConfig()
                .setDefaultLanguage("en")
                .setAddSuperTypes(true);
        InstanceAggregator collector = new InstanceAggregator(config, jsc().broadcast(schema));
        JavaRDD<Instance> result = collector.aggregateInstances(TestUtils.getQuadsRDD(jsc(), "aggregatorTest.nq")).cache();

        result = checkErrorInstance(result);

        assertRDDEquals("Aggregated instances with super types are equal to expected RDD.", getExpectedRDD(true), result);
    }

    private JavaRDD<Instance> checkErrorInstance(JavaRDD<Instance> result) {
        int errorType = uriIndex.getIndex(InstanceAggregator.IRI_TYPE_ERROR);
        assertTrue("Parsing error instance is present", result.filter(instance -> instance.getTypes().contains(errorType)).count() == 1L);
        // filter out error instance
        return result.filter(instance -> !instance.getTypes().contains(errorType));
    }

    private JavaRDD<Instance> getExpectedRDD(boolean addSuperTypes) {
        List<Instance> rdd = new ArrayList<>();

        Instance instance;

        instance = new Instance();
        instance.addType(uriIndex.getIndex("http://t.com/types#Flavor"));
        instance.setUri("http://t.com/flavors#Sour");
        rdd.add(instance);

        instance = new Instance();
        instance.addType(uriIndex.getIndex("http://t.com/types#Vegetable"));
        instance.addType(uriIndex.getIndex("http://t.com/types#Fruit"));
        instance.setUri("http://t.com/fruit#Tomato");
        instance.addRelation(new RelationPredicate(uriIndex.getIndex("http://t.com/rel#HasFlavor"), "http://t.com/flavors#Sour"));
        instance.addRelation(new RelationPredicate(uriIndex.getIndex("http://t.com/rel#HasFlavor"), "http://t.com/flavors#Sweet"));
        instance.putLiteralValue(new Predicate(uriIndex.getIndex("http://another.com/rel#Weight"), LiteralType.INTEGER), 110);
        instance.putLiteralValue(new Predicate(uriIndex.getIndex("http://t.com/rel#Weight"), LiteralType.INTEGER), 100);
        instance.putLiteralValue(new Predicate(uriIndex.getIndex("http://t.com/rel#Color"), LiteralType.STRING), "red");
        instance.putLiteralValue(new Predicate(uriIndex.getIndex("http://t.com/rel#Color"), LiteralType.STRING, "fr"), "rouge");
        rdd.add(instance);

        instance = new Instance();
        instance.addType(uriIndex.getIndex("http://t.com/types#Fruit"));
        instance.setUri("http://t.com/fruit#Apple");
        instance.setRelation(new RelationPredicate(uriIndex.getIndex("http://t.com/rel#HasFlavor"), "http://t.com/flavors#Sweet"));
        instance.putLiteralValue(new Predicate(uriIndex.getIndex("http://t.com/rel#Weight"), LiteralType.STRING), "unknown");
        instance.putLiteralValue(new Predicate(uriIndex.getIndex("http://t.com/rel#Weight"), LiteralType.INTEGER), new HashSet<>(Arrays.asList(100, 150, 180)));
        rdd.add(instance);

        instance = new Instance();
        instance.addType(uriIndex.getIndex("http://t.com/types#Vegetable"));
        instance.setUri("http://t.com/fruit#Cucumber");
        instance.putLiteralValue(new Predicate(uriIndex.getIndex("http://t.com/rel#IsTasty"), LiteralType.BOOLEAN), new HashSet<>(Arrays.asList(false, true)));
        instance.addRelation(new RelationPredicate(uriIndex.getIndex("http://t.com/rel#HasFlavor"), "http://t.com/flavors#FirstThingWithNoType"));
        rdd.add(instance);

        instance = new Instance();
        instance.addType(uriIndex.getIndex("http://t.com/types#Flavor"));
        instance.setUri("http://t.com/flavors#Sweet");
        rdd.add(instance);

        if (addSuperTypes) {
            rdd.forEach(i -> i.addType(uriIndex.getIndex("http://t.com/types#Root")));
        }

        instance = new Instance();
        instance.addType(uriIndex.getIndex(InstanceAggregator.IRI_TYPE_DEFAULT));
        instance.setUri("http://t.com/flavors#FirstThingWithNoType");
        rdd.add(instance);

        instance = new Instance();
        instance.addType(uriIndex.getIndex(InstanceAggregator.IRI_TYPE_DEFAULT));
        instance.setUri("http://t.com/fruit#SecondThingWithNoType");
        instance.putLiteralValue(new Predicate(uriIndex.getIndex("http://t.com/rel#Color"), LiteralType.STRING), "red");
        rdd.add(instance);

        return jsc().parallelize(rdd);
    }


}