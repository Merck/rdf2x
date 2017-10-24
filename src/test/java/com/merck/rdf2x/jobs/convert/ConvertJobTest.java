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

package com.merck.rdf2x.jobs.convert;

import com.merck.rdf2x.flavors.DefaultFlavor;
import com.merck.rdf2x.persistence.MetadataWriter;
import com.merck.rdf2x.persistence.config.OutputConfig;
import com.merck.rdf2x.processing.aggregating.InstanceAggregator;
import com.merck.rdf2x.test.TestSparkContextProvider;
import com.merck.rdf2x.test.TestUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.storage.StorageLevel;
import org.junit.Test;

import javax.naming.ConfigurationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test of {@link ConvertJob}
 */
@Slf4j
public class ConvertJobTest extends TestSparkContextProvider {

    @Test
    public void testConvertJob() throws ConfigurationException {

        Map<String, DataFrame> results = new HashMap<>();

        ConvertConfig config = new ConvertConfig()
                .setCacheLevel(StorageLevel.MEMORY_ONLY())
                .setInputFile(TestUtils.getDatasetPath("convertJobTest.nq"))
                .setOutputConfig(OutputConfig.toMap(results));

        config.getParserConfig().setBatchSize(10);

        config.getEntitySchemaCollectorConfig().setForceTypeSuffix(true);

        JavaSparkContext sc = jsc();
        ConvertJob job = new ConvertJob(config, sc, new DefaultFlavor());
        job.run();

        log.info("Result tables: {}", results.keySet());

        assertTrue("Result contains META Entities table", results.containsKey(MetadataWriter.ENTITIES_TABLE_NAME));
        assertTrue("Result contains META Relations table", results.containsKey(MetadataWriter.RELATIONS_TABLE_NAME));
        assertTrue("Result contains META Columns table", results.containsKey(MetadataWriter.PROPERTIES_TABLE_NAME));
        assertTrue("Result contains META Predicates table", results.containsKey(MetadataWriter.PREDICATES_TABLE_NAME));

        assertTrue("Result contains table for 'fruit' type", results.containsKey("fruit"));
        assertTrue("Result contains table for 'vegetable' type", results.containsKey("vegetable"));
        assertTrue("Result contains table for 'flavor' type", results.containsKey("flavor"));

        assertTrue("Result contains table for 'fruit_fruit' relation", results.containsKey("fruit_fruit"));
        assertTrue("Result contains table for 'vegetable_fruit' relation", results.containsKey("vegetable_fruit"));
        assertTrue("Result contains table for 'fruit_flavor' relation", results.containsKey("fruit_flavor"));
        assertTrue("Result contains table for 'vegetable_flavor' relation", results.containsKey("vegetable_flavor"));

        Set<Row> metaEntities = new HashSet<>();
        metaEntities.add(RowFactory.create("fruit", "http://t.com/types#Fruit", null, 2));
        metaEntities.add(RowFactory.create("vegetable", "http://t.com/types#Vegetable", null, 2));
        metaEntities.add(RowFactory.create("flavor", "http://t.com/types#Flavor", null, 2));
        assertEquals("Expected META Entities table persisted",
                metaEntities,
                new HashSet<>(results.get(MetadataWriter.ENTITIES_TABLE_NAME).collectAsList()));

        Set<Row> metaRelations = new HashSet<>();
        metaRelations.add(RowFactory.create("fruit_fruit", "fruit", "fruit", null));
        metaRelations.add(RowFactory.create("vegetable_flavor", "vegetable", "flavor", null));
        metaRelations.add(RowFactory.create("fruit_flavor", "fruit", "flavor", null));
        metaRelations.add(RowFactory.create("vegetable_fruit", "vegetable", "fruit", null));
        assertEquals("Expected META Relations table persisted",
                metaRelations,
                new HashSet<>(results.get(MetadataWriter.RELATIONS_TABLE_NAME).collectAsList()));


        Set<Row> metaProperties = new HashSet<>();
        metaProperties.add(RowFactory.create("fruit", 0, "weight_2_integer", true, true, true, "INTEGER", null, 1.0, null));
        metaProperties.add(RowFactory.create("vegetable", 4, "weight_integer", true, true, false, "INTEGER", null, 0.5, null));
        metaProperties.add(RowFactory.create("vegetable", 5, "istasty_boolean", true, true, true, "BOOLEAN", null, 0.5, null));
        metaProperties.add(RowFactory.create("vegetable", 0, "weight_2_integer", true, true, false, "INTEGER", null, 0.5, null));
        metaProperties.add(RowFactory.create("fruit", 2, "planted_datetime", true, true, false, "DATETIME", null, 0.5, null));
        metaProperties.add(RowFactory.create("vegetable", 2, "planted_datetime", true, true, false, "DATETIME", null, 0.5, null));
        metaProperties.add(RowFactory.create("fruit", 4, "weight_integer", true, true, false, "INTEGER", null, 0.5, null));
        metaProperties.add(RowFactory.create("fruit", 0, "weight_2_string", true, true, false, "STRING", null, 0.5, null));
        assertEquals("Expected META Properties table persisted",
                metaProperties,
                new HashSet<>(results.get(MetadataWriter.PROPERTIES_TABLE_NAME).collectAsList()));

        Set<Row> metaPredicates = new HashSet<>();
        metaPredicates.add(RowFactory.create(0, "http://t.com/rel#Weight", null));
        metaPredicates.add(RowFactory.create(1, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", null));
        metaPredicates.add(RowFactory.create(2, "http://t.com/rel#Planted", null));
        metaPredicates.add(RowFactory.create(3, "http://t.com/rel#HasFlavor", null));
        metaPredicates.add(RowFactory.create(4, "http://another.com/rel#Weight", null));
        metaPredicates.add(RowFactory.create(5, "http://t.com/rel#IsTasty", null));
        metaPredicates.add(RowFactory.create(6, "http://t.com/rel#LooksLike", null));
        metaPredicates.add(RowFactory.create(7, InstanceAggregator.IRI_PROPERTY_ERROR_SUBJECT, null));
        metaPredicates.add(RowFactory.create(8, InstanceAggregator.IRI_PROPERTY_ERROR_QUAD, null));
        metaPredicates.add(RowFactory.create(9, InstanceAggregator.IRI_PROPERTY_ERROR_MESSAGE, null));
        assertEquals("Expected META Predicates table persisted",
                metaPredicates,
                new HashSet<>(results.get(MetadataWriter.PREDICATES_TABLE_NAME).collectAsList()));

        Set<Row> fruit = new HashSet<>();
        fruit.add(RowFactory.create(4, "http://t.com/fruit#Tomato", 100, "2016-01-04T00:00:00Z", null, 110));
        fruit.add(RowFactory.create(6, "http://t.com/fruit#Apple", 180, null, "TBD", null));
        assertEquals("Expected table for entity 'fruit' persisted",
                fruit,
                new HashSet<>(results.get("fruit").collectAsList()));

        Set<Row> vegetable = new HashSet<>();
        vegetable.add(RowFactory.create(4, "http://t.com/fruit#Tomato", 110, 100, null, "2016-01-04T00:00:00Z"));
        vegetable.add(RowFactory.create(8, "http://t.com/fruit#Cucumber", null, null, false, null));
        assertEquals("Expected table for entity 'vegetable' persisted",
                vegetable,
                new HashSet<>(results.get("vegetable").collectAsList()));

        Set<Row> flavor = new HashSet<>();
        flavor.add(RowFactory.create(2, "http://t.com/flavors#Sour"));
        flavor.add(RowFactory.create(1, "http://t.com/flavors#Sweet"));
        assertEquals("Expected table for entity 'flavor' persisted",
                flavor,
                new HashSet<>(results.get("flavor").collectAsList()));

        Set<Row> fruit_fruit = new HashSet<>();
        fruit_fruit.add(RowFactory.create(4, 6, 6));
        assertEquals("Expected table for relation 'fruit_fruit' persisted",
                fruit_fruit,
                new HashSet<>(results.get("fruit_fruit").collectAsList()));

        Set<Row> fruit_flavor = new HashSet<>();
        fruit_flavor.add(RowFactory.create(4, 2, 3));
        fruit_flavor.add(RowFactory.create(4, 1, 3));
        fruit_flavor.add(RowFactory.create(6, 1, 3));
        assertEquals("Expected table for relation 'fruit_flavor' persisted",
                fruit_flavor,
                new HashSet<>(results.get("fruit_flavor").collectAsList()));

        Set<Row> vegetable_flavor = new HashSet<>();
        vegetable_flavor.add(RowFactory.create(4, 2, 3));
        vegetable_flavor.add(RowFactory.create(4, 1, 3));
        assertEquals("Expected table for relation 'vegetable_flavor' persisted",
                vegetable_flavor,
                new HashSet<>(results.get("vegetable_flavor").collectAsList()));

        Set<Row> vegetable_fruit = new HashSet<>();
        vegetable_fruit.add(RowFactory.create(4, 6, 6));
        assertEquals("Expected table for relation 'vegetable_fruit' persisted",
                vegetable_fruit,
                new HashSet<>(results.get("vegetable_fruit").collectAsList()));

    }

}
