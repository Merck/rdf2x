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

package com.merck.rdf2x.persistence;

import com.merck.rdf2x.beans.IndexMap;
import com.merck.rdf2x.beans.Predicate;
import com.merck.rdf2x.persistence.output.Persistor;
import com.merck.rdf2x.persistence.schema.*;
import com.merck.rdf2x.processing.schema.RelationSchemaStrategy;
import com.merck.rdf2x.rdf.LiteralType;
import com.merck.rdf2x.rdf.schema.RdfSchema;
import com.merck.rdf2x.rdf.schema.RdfSchemaCollectorConfig;
import com.merck.rdf2x.test.TestSparkContextProvider;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.*;

import static com.merck.rdf2x.persistence.MetadataWriter.*;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class MetadataWriterTest extends TestSparkContextProvider {

    private IndexMap<String> typeIndex, predicateIndex;
    private Map<String, String> typeNames;
    private Map<String, String> predicateNames;
    private Map<String, String> uriLabels;

    private DataFrame mockResult;
    private Persistor mockWriter;
    private RdfSchema schema;

    @Before
    public void setUp() {
        typeIndex = new IndexMap<>(Arrays.asList(
                "http://example.com/a",
                "http://example.com/b"));

        predicateIndex = new IndexMap<>(Arrays.asList(
                "http://example.com/knows",
                "http://example.com/likes",
                "http://example.com/name",
                "http://example.com/age"
        ));

        uriLabels = new HashMap<>();
        uriLabels.put("http://example.com/a", "a label");
        uriLabels.put("http://example.com/b", null);
        uriLabels.put("http://example.com/knows", "Knows label");
        uriLabels.put("http://example.com/likes", "Likes label");
        uriLabels.put("http://example.com/name", "Name label");
        uriLabels.put("http://example.com/age", null);

        typeNames = new HashMap<>();
        typeNames.put("http://example.com/a", "a");
        typeNames.put("http://example.com/b", "b");
        typeNames.put("http://example.com/c", "c");

        predicateNames = new HashMap<>();
        predicateNames.put("http://example.com/name", "name");
        predicateNames.put("http://example.com/age", "age");
        predicateNames.put("http://example.com/knows", "knows");
        predicateNames.put("http://example.com/likes", "likes");

        mockWriter = mock(Persistor.class);
        doAnswer(new Answer<Void>() {
            public Void answer(InvocationOnMock invocation) {
                Object[] args = invocation.getArguments();
                mockResult = (DataFrame) args[1];
                return null;
            }
        }).when(mockWriter).writeDataFrame(anyString(), any(DataFrame.class));

        schema = new RdfSchema(new RdfSchemaCollectorConfig(), null, typeIndex, predicateIndex, uriLabels);
    }

    @Test
    public void testWriteEntityMetadata() throws IOException {
        MetadataWriter writer = new MetadataWriter(jsc(), mockWriter, schema);
        writer.writeEntityMetadata(getTestEntitySchema());

        assertEquals("Expected entity schema was extracted", getExpectedSchemaOfMetaEntities(), mockResult.schema());
        assertRDDEquals("Expected meta entities were extracted", getExpectedRowsOfMetaEntities(), mockResult.toJavaRDD());
    }

    @Test
    public void testWritePropertyMetadata() throws IOException {
        MetadataWriter writer = new MetadataWriter(jsc(), mockWriter, schema);
        writer.writePropertyMetadata(getTestEntitySchema());

        assertEquals("Expected property schema was extracted", getExpectedSchemaOfMetaProperties(), mockResult.schema());
        assertRDDEquals("Expected meta properties were extracted", getExpectedRowsOfMetaProperties(), mockResult.toJavaRDD());
    }

    @Test
    public void testWriteRelationMetadata() throws IOException {
        MetadataWriter writer = new MetadataWriter(jsc(), mockWriter, schema);
        writer.writeRelationMetadata(getTestRelationSchema());

        assertEquals("Expected relation schema was extracted", getExpectedSchemaOfMetaRelations(), mockResult.schema());
        assertRDDEquals("Expected meta relation were extracted", getExpectedRowsOfMetaRelations(), mockResult.toJavaRDD());
    }

    @Test
    public void testWritePredicateMetadata() throws IOException {
        MetadataWriter writer = new MetadataWriter(jsc(), mockWriter, schema);
        writer.writePredicateMetadata();

        assertEquals("Expected predicate schema was extracted", getExpectedSchemaOfMetaPredicates(), mockResult.schema());
        assertRDDEquals("Expected meta predicates were extracted", getExpectedRowsOfMetaPredicates(), mockResult.toJavaRDD());
    }


    private JavaRDD<Row> getExpectedRowsOfMetaProperties() {
        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create("a", predicateIndex.getIndex("http://example.com/name"), "name", true, true, true, "STRING", null, 0.5, null));
        rows.add(RowFactory.create("b", predicateIndex.getIndex("http://example.com/age"), "age", true, true, false, "INTEGER", null, 0.5, null));
        rows.add(RowFactory.create("b", predicateIndex.getIndex("http://example.com/name"), "name", true, true, false, "STRING", "en", 0.5, null));
        return jsc().parallelize(rows);
    }

    private StructType getExpectedSchemaOfMetaProperties() {
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField(PROPERTIES_ENTITY_NAME, DataTypes.StringType, false));
        fields.add(DataTypes.createStructField(PROPERTIES_PREDICATE_ID, DataTypes.IntegerType, false));
        fields.add(DataTypes.createStructField(PROPERTIES_COLUMN_NAME, DataTypes.StringType, true));
        fields.add(DataTypes.createStructField(PROPERTIES_IS_COLUMN, DataTypes.BooleanType, false));
        fields.add(DataTypes.createStructField(PROPERTIES_IS_ATTRIBUTE, DataTypes.BooleanType, false));
        fields.add(DataTypes.createStructField(PROPERTIES_IS_MULTIVALUED, DataTypes.BooleanType, false));
        fields.add(DataTypes.createStructField(PROPERTIES_TYPE, DataTypes.StringType, false));
        fields.add(DataTypes.createStructField(PROPERTIES_LANGUAGE, DataTypes.StringType, true));
        fields.add(DataTypes.createStructField(PROPERTIES_NON_NULL_RATIO, DataTypes.DoubleType, true));
        fields.add(DataTypes.createStructField(PROPERTIES_STORED_IN_SUPERCLASS, DataTypes.StringType, true));
        return DataTypes.createStructType(fields);
    }


    private JavaRDD<Row> getExpectedRowsOfMetaEntities() {
        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create("a", "http://example.com/a", uriLabels.get("http://example.com/a"), 2L));
        rows.add(RowFactory.create("b", "http://example.com/b", uriLabels.get("http://example.com/b"), 1L));
        return jsc().parallelize(rows);
    }

    private StructType getExpectedSchemaOfMetaEntities() {
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField(ENTITIES_NAME, DataTypes.StringType, false));
        fields.add(DataTypes.createStructField(ENTITIES_URI, DataTypes.StringType, false));
        fields.add(DataTypes.createStructField(ENTITIES_LABEL, DataTypes.StringType, true));
        fields.add(DataTypes.createStructField(ENTITIES_NUM_ROWS, DataTypes.LongType, false));
        return DataTypes.createStructType(fields);
    }


    private JavaRDD<Row> getExpectedRowsOfMetaRelations() {
        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create("a_b", "a", "b", null));
        return jsc().parallelize(rows);
    }

    private StructType getExpectedSchemaOfMetaRelations() {
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField(RELATIONS_NAME, DataTypes.StringType, false));
        fields.add(DataTypes.createStructField(RELATIONS_FROM_NAME, DataTypes.StringType, true));
        fields.add(DataTypes.createStructField(RELATIONS_TO_NAME, DataTypes.StringType, true));
        fields.add(DataTypes.createStructField(RELATIONS_PREDICATE_ID, DataTypes.IntegerType, true));
        return DataTypes.createStructType(fields);
    }

    private JavaRDD<Row> getExpectedRowsOfMetaPredicates() {
        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create(predicateIndex.getIndex("http://example.com/knows"), "http://example.com/knows", "Knows label"));
        rows.add(RowFactory.create(predicateIndex.getIndex("http://example.com/likes"), "http://example.com/likes", "Likes label"));
        rows.add(RowFactory.create(predicateIndex.getIndex("http://example.com/name"), "http://example.com/name", "Name label"));
        rows.add(RowFactory.create(predicateIndex.getIndex("http://example.com/age"), "http://example.com/age", null));
        return jsc().parallelize(rows);
    }

    private StructType getExpectedSchemaOfMetaPredicates() {
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField(PREDICATE_ID, DataTypes.IntegerType, false));
        fields.add(DataTypes.createStructField(PREDICATE_URI, DataTypes.StringType, false));
        fields.add(DataTypes.createStructField(PREDICATE_LABEL, DataTypes.StringType, true));
        return DataTypes.createStructType(fields);
    }

    private EntitySchema getTestEntitySchema() {
        List<EntityTable> tables = new ArrayList<>();
        List<EntityColumn> columns;
        Set<EntityProperty> attributes;

        columns = new ArrayList<>();
        columns.add(new EntityColumn("name", new EntityProperty(new Predicate(predicateIndex.getIndex("http://example.com/name"), LiteralType.STRING), true, 0.5)));
        attributes = new HashSet<>();
        attributes.add(new EntityProperty(
                new Predicate(predicateIndex.getIndex("http://example.com/name"), LiteralType.STRING, null),
                true,
                0.5
        ));
        tables.add(new EntityTable("a", "http://example.com/a", 2L, columns, attributes));

        columns = new ArrayList<>();
        columns.add(new EntityColumn("name", new EntityProperty(new Predicate(predicateIndex.getIndex("http://example.com/name"), LiteralType.STRING, "en"), false, 0.5)));
        columns.add(new EntityColumn("age", new EntityProperty(new Predicate(predicateIndex.getIndex("http://example.com/age"), LiteralType.INTEGER, null), false, 0.5)));
        attributes = new HashSet<>();
        tables.add(new EntityTable("b", "http://example.com/b", 1L, columns, attributes));

        return new EntitySchema(tables, typeNames, predicateNames);
    }

    private RelationSchema getTestRelationSchema() {
        List<RelationTable> tables = new ArrayList<>();

        tables.add(new RelationTable("a_b", RelationSchemaStrategy.Types).setEntityFilter(
                new RelationEntityFilter(
                        typeIndex.getIndex("http://example.com/a"),
                        "a",
                        typeIndex.getIndex("http://example.com/b"),
                        "b")
                )
        );

        return new RelationSchema(tables);
    }
}
