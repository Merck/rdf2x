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

package com.merck.rdf2x.processing.schema;

import com.google.common.collect.Sets;
import com.merck.rdf2x.beans.IndexMap;
import com.merck.rdf2x.beans.Instance;
import com.merck.rdf2x.beans.Predicate;
import com.merck.rdf2x.persistence.schema.EntityColumn;
import com.merck.rdf2x.persistence.schema.EntityProperty;
import com.merck.rdf2x.persistence.schema.EntitySchema;
import com.merck.rdf2x.persistence.schema.EntityTable;
import com.merck.rdf2x.processing.formatting.FormatUtil;
import com.merck.rdf2x.processing.formatting.SchemaFormatter;
import com.merck.rdf2x.rdf.LiteralType;
import com.merck.rdf2x.rdf.schema.ClassGraph;
import com.merck.rdf2x.rdf.schema.RdfSchema;
import com.merck.rdf2x.rdf.schema.RdfSchemaCollectorConfig;
import com.merck.rdf2x.test.TestSparkContextProvider;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test of {@link EntitySchemaCollector}
 */
@Slf4j
public class EntitySchemaCollectorTest extends TestSparkContextProvider {
    private IndexMap<String> uriIndex;
    private Map<String, String> typeNames;
    private Map<String, String> predicateNames;
    private RdfSchema schema;
    private SchemaFormatter mockFormatter;
    private Predicate name, name_en, age_int, age_float;
    private String column_name, column_name_en, column_age_int, column_age_float;
    private EntityProperty a_name, a_name_en, a_age_int, b_name, b_age_int, b_age_float;

    @Before
    public void setUp() {

        uriIndex = new IndexMap<>(Arrays.asList(
                "http://example.com/a/type",
                "http://example.com/b/type",
                "http://example.com/name",
                "http://example.com/age"
        ));

        typeNames = new HashMap<>();
        typeNames.put("http://example.com/a/type", "a_type");
        typeNames.put("http://example.com/b/type", "b_type");

        predicateNames = new HashMap<>();
        predicateNames.put("http://example.com/name", "name");
        predicateNames.put("http://example.com/age", "age");

        // predicates
        name = new Predicate(uriIndex.getIndex("http://example.com/name"), LiteralType.STRING);
        name_en = new Predicate(uriIndex.getIndex("http://example.com/name"), LiteralType.STRING, "en");
        age_int = new Predicate(uriIndex.getIndex("http://example.com/age"), LiteralType.INTEGER);
        age_float = new Predicate(uriIndex.getIndex("http://example.com/age"), LiteralType.FLOAT);

        // column names
        column_name = FormatUtil.addTypeSuffix("name", LiteralType.STRING);
        column_name_en = FormatUtil.addTypeSuffix(FormatUtil.addLanguageSuffix("name", "en"), LiteralType.STRING);
        column_age_int = FormatUtil.addTypeSuffix("age", LiteralType.INTEGER);
        column_age_float = FormatUtil.addTypeSuffix("age", LiteralType.FLOAT);

        // properties of table A
        a_name = new EntityProperty(name, true, 1.0);
        a_name_en = new EntityProperty(name_en, false, 1.0);
        a_age_int = new EntityProperty(age_int, false, 1.0);

        // properties of table B
        b_name = new EntityProperty(name, false, 1.0);
        b_age_int = new EntityProperty(age_int, true, 0.5);
        b_age_float = new EntityProperty(age_float, false, 0.5);

        schema = new RdfSchema(
                new RdfSchemaCollectorConfig(),
                new ClassGraph(),
                uriIndex,
                uriIndex,
                new HashMap<>()
        );

        mockFormatter = mock(SchemaFormatter.class);
        when(mockFormatter.getTypeNames(any(Collection.class), any(Map.class))).thenReturn(typeNames);
        when(mockFormatter.getPropertyNames(any(Collection.class), any(Map.class))).thenReturn(predicateNames);
    }

    /**
     * Test if expected schema is extracted from the test RDD of Instances
     */
    @Test
    public void testSchemaExtractionWithAllColumns() {
        EntitySchemaCollector collector = new EntitySchemaCollector(
                new EntitySchemaCollectorConfig()
                        .setSortColumnsAlphabetically(true)
                        .setMinColumnNonNullFraction(0.0)
                        .setRedundantEAV(false)
                        .setForceTypeSuffix(true),
                mockFormatter,
                schema
        );

        List<EntityTable> expectedTables = new ArrayList<>();
        expectedTables.add(new EntityTable(
                "a_type",
                "http://example.com/a/type",
                1L,
                Arrays.asList(
                        new EntityColumn(column_age_int, a_age_int),
                        new EntityColumn(column_name_en, a_name_en),
                        new EntityColumn(column_name, a_name)
                ),
                new HashSet<>(Arrays.asList(
                        a_name
                ))
        ));

        expectedTables.add(new EntityTable(
                "b_type",
                "http://example.com/b/type",
                2L,
                new ArrayList<>(Arrays.asList(
                        new EntityColumn(column_age_float, b_age_float),
                        new EntityColumn(column_age_int, b_age_int),
                        new EntityColumn(column_name, b_name)
                )),
                new HashSet<>(Arrays.asList(
                        b_age_int
                ))
        ));

        EntitySchema expected = new EntitySchema(expectedTables, typeNames, predicateNames);

        EntitySchema schema = collector.collectSchema(getTestRDD());
        assertEquals("Entity schema without EAV matches expected schema", expected, schema);
    }

    /**
     * Test if expected schema is extracted from the test RDD of Instances
     */
    @Test
    public void testSchemaExtractionWithAllColumnsAndAttributes() {
        EntitySchemaCollector collector = new EntitySchemaCollector(
                new EntitySchemaCollectorConfig()
                        .setSortColumnsAlphabetically(true)
                        .setMinColumnNonNullFraction(0.0)
                        .setRedundantEAV(true)
                        .setForceTypeSuffix(true),
                mockFormatter,
                schema
        );

        List<EntityTable> expectedTables = new ArrayList<>();
        expectedTables.add(new EntityTable(
                "a_type",
                "http://example.com/a/type",
                1L,
                Arrays.asList(
                        new EntityColumn(column_age_int, a_age_int),
                        new EntityColumn(column_name_en, a_name_en),
                        new EntityColumn(column_name, a_name)
                ),
                new HashSet<>(Arrays.asList(
                        a_name, a_name_en, a_age_int
                ))
        ));

        expectedTables.add(new EntityTable(
                "b_type",
                "http://example.com/b/type",
                2L,
                new ArrayList<>(Arrays.asList(
                        new EntityColumn(column_age_float, b_age_float),
                        new EntityColumn(column_age_int, b_age_int),
                        new EntityColumn(column_name, b_name)
                )),
                new HashSet<>(Arrays.asList(
                        b_name, b_age_int, b_age_float
                ))
        ));

        EntitySchema expected = new EntitySchema(expectedTables, typeNames, predicateNames);

        EntitySchema schema = collector.collectSchema(getTestRDD());
        assertEquals("Entity schema without EAV matches expected schema", expected, schema);
    }

    /**
     * Test if expected schema is extracted from the test RDD of Instances
     */
    @Test
    public void testSchemaExtractionWithMinNonNullFraction() {
        EntitySchemaCollector collector = new EntitySchemaCollector(
                new EntitySchemaCollectorConfig()
                        .setSortColumnsAlphabetically(true)
                        .setMinColumnNonNullFraction(0.9)
                        .setRedundantEAV(false)
                        .setForceTypeSuffix(true),
                mockFormatter,
                schema
        );

        List<EntityTable> expectedTables = new ArrayList<>();
        expectedTables.add(new EntityTable(
                "a_type",
                "http://example.com/a/type",
                1L,
                Arrays.asList(
                        new EntityColumn(column_age_int, a_age_int),
                        new EntityColumn(column_name_en, a_name_en),
                        new EntityColumn(column_name, a_name)
                ),
                new HashSet<>(Arrays.asList(
                        a_name // multivalued
                ))
        ));

        expectedTables.add(new EntityTable(
                "b_type",
                "http://example.com/b/type",
                2L,
                new ArrayList<>(Arrays.asList(
                        new EntityColumn(column_name, b_name)
                )),
                new HashSet<>(Arrays.asList(
                        b_age_int, b_age_float
                ))
        ));

        EntitySchema expected = new EntitySchema(expectedTables, typeNames, predicateNames);

        EntitySchema schema = collector.collectSchema(getTestRDD());
        assertEquals("Entity schema without EAV matches expected schema", expected, schema);
    }


    private JavaRDD<Instance> getTestRDD() {
        List<Instance> rdd = new ArrayList<>();
        Instance instance;

        instance = new Instance();
        instance.setType(uriIndex.getIndex("http://example.com/a/type"));
        instance.setId(1L);
        instance.putLiteralValue(name, Sets.newHashSet("first name", "second name"));
        instance.putLiteralValue(name_en, "english name");
        instance.putLiteralValue(age_int, 1);
        rdd.add(instance);

        instance = new Instance();
        instance.setType(uriIndex.getIndex("http://example.com/b/type"));
        instance.setId(2L);
        instance.putLiteralValue(name, "name 2");
        instance.putLiteralValue(age_int, Sets.newHashSet(2, 20));
        rdd.add(instance);

        instance = new Instance();
        instance.setType(uriIndex.getIndex("http://example.com/b/type"));
        instance.setId(3L);
        instance.putLiteralValue(name, "name 3");
        instance.putLiteralValue(age_float, 3.0F);
        rdd.add(instance);
        return jsc().parallelize(rdd);
    }

}
