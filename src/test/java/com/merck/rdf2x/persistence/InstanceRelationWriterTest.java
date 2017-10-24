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

import com.google.common.collect.Sets;
import com.merck.rdf2x.beans.IndexMap;
import com.merck.rdf2x.beans.Instance;
import com.merck.rdf2x.beans.Predicate;
import com.merck.rdf2x.beans.RelationPredicate;
import com.merck.rdf2x.persistence.output.DataFrameMapPersistor;
import com.merck.rdf2x.persistence.schema.*;
import com.merck.rdf2x.processing.schema.RelationSchemaStrategy;
import com.merck.rdf2x.rdf.LiteralType;
import com.merck.rdf2x.rdf.schema.ClassGraph;
import com.merck.rdf2x.rdf.schema.RdfSchema;
import com.merck.rdf2x.rdf.schema.RdfSchemaCollectorConfig;
import com.merck.rdf2x.test.TestSparkContextProvider;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static com.merck.rdf2x.persistence.InstanceRelationWriter.*;
import static org.junit.Assert.assertEquals;

public class InstanceRelationWriterTest extends TestSparkContextProvider {

    private RdfSchema rdfSchema;
    private IndexMap<String> uriIndex;
    private Map<String, String> typeNames;
    private InstanceRelationWriterConfig config;

    private Map<String, DataFrame> result;
    private DataFrameMapPersistor persistor;
    private SQLContext sql;

    @Before
    public void setUp() {
        sql = new SQLContext(jsc());
        uriIndex = new IndexMap<>(Arrays.asList(
                "http://example.com/a",
                "http://example.com/b",
                "http://example.com/c",
                "http://example.com/knows",
                "http://example.com/likes",
                "http://example.com/name",
                "http://example.com/age"
        ));

        rdfSchema = new RdfSchema(
                new RdfSchemaCollectorConfig(),
                new ClassGraph(),
                uriIndex,
                uriIndex,
                null
        );

        typeNames = new HashMap<>();
        typeNames.put("http://example.com/a", "a");
        typeNames.put("http://example.com/b", "b");
        typeNames.put("http://example.com/c", "c");

        config = new InstanceRelationWriterConfig();

        persistor = new DataFrameMapPersistor();
        result = persistor.getResultMap();
    }

    @Test
    public void testWriteEntityTables() throws IOException {
        InstanceRelationWriter writer = new InstanceRelationWriter(config, jsc(), persistor, rdfSchema);
        writer.writeEntityTables(getTestEntitySchema(), getTestInstances());

        assertEquals("Expected schema of A was extracted", getExpectedSchemaOfA(), result.get("a").schema());
        assertRDDEquals("Expected rows of A were extracted", getExpectedRowsOfA(), result.get("a").toJavaRDD());

        assertEquals("Expected schema of B was extracted", getExpectedSchemaOfB(), result.get("b").schema());
        assertRDDEquals("Expected rows of B were extracted", getExpectedRowsOfB(), result.get("b").toJavaRDD());
    }

    @Test
    public void testWriteEAVTables() throws IOException {
        InstanceRelationWriter writer = new InstanceRelationWriter(config, jsc(), persistor, rdfSchema);
        writer.writeEntityAttributeValueTable(getTestEntitySchema(), getTestInstances());

        DataFrame result = this.result.values().iterator().next();
        assertEquals("Expected schema of EAV table was extracted", getExpectedSchemaOfEAV(), result.schema());
        assertRDDEquals("Expected rows of EAV table were extracted", getExpectedRowsOfEAV(), result.toJavaRDD());
    }

    @Test
    public void testWriteRelationTablesWithoutPredicateIndex() throws IOException {
        InstanceRelationWriter writer = new InstanceRelationWriter(config
                .setStorePredicate(false), jsc(), persistor, rdfSchema);
        writer.writeRelationTables(getTestRelationSchema(), getTestRelations());

        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create(1L, 3L));
        rows.add(RowFactory.create(2L, 3L));

        DataFrame result = this.result.values().iterator().next();
        assertEquals("Expected schema of A_B was extracted", getExpectedSchemaOfAB(false, false), result.schema());
        assertRDDEquals("Expected rows of A_B were extracted", jsc().parallelize(rows), result.toJavaRDD());
    }

    @Test
    public void testWriteRelationTablesWithPredicateIndex() throws IOException {
        InstanceRelationWriter writer = new InstanceRelationWriter(config
                .setStorePredicate(true), jsc(), persistor, rdfSchema);
        writer.writeRelationTables(getTestRelationSchema(), getTestRelations());

        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create(1L, 3L, uriIndex.getIndex("http://example.com/knows")));
        rows.add(RowFactory.create(2L, 3L, uriIndex.getIndex("http://example.com/likes")));

        DataFrame result = this.result.values().iterator().next();
        assertEquals("Expected schema of A_B was extracted", getExpectedSchemaOfAB(true, false), result.schema());
        assertRDDEquals("Expected rows of A_B were extracted", jsc().parallelize(rows), result.toJavaRDD());
    }

    @Test
    public void testWriteSingleRelationTable() throws IOException {
        InstanceRelationWriter writer = new InstanceRelationWriter(
                config, jsc(), persistor, rdfSchema);
        writer.writeRelationTables(getTestSingleRelationSchema(), getTestRelations());

        DataFrame result = this.result.values().iterator().next();
        assertEquals("Expected schema of single relation table was extracted", getExpectedSchemaOfSingleRelationTable(), result.schema());
        assertRDDEquals("Expected rows of single relation table were extracted", getExpectedRowsOfSingleRelationTable(), result.toJavaRDD());
    }

    private JavaRDD<Row> getExpectedRowsOfA() {
        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create(1L, "http://example.com/a/1", "First A 1"));
        rows.add(RowFactory.create(2L, "http://example.com/a/2", "Second A"));
        return jsc().parallelize(rows);
    }

    private StructType getExpectedSchemaOfA() {
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField(ID_COLUMN_NAME, DataTypes.LongType, false));
        fields.add(DataTypes.createStructField(URI_COLUMN_NAME, DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        return DataTypes.createStructType(fields);
    }

    private JavaRDD<Row> getExpectedRowsOfB() {
        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create(3L, "http://example.com/b/1", "First B", 100));
        return jsc().parallelize(rows);
    }

    private StructType getExpectedSchemaOfB() {
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField(ID_COLUMN_NAME, DataTypes.LongType, false));
        fields.add(DataTypes.createStructField(URI_COLUMN_NAME, DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        return DataTypes.createStructType(fields);
    }

    private JavaRDD<Row> getExpectedRowsOfEAV() {
        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create(1L, uriIndex.getIndex("http://example.com/name"), "STRING", null, "First A 1"));
        rows.add(RowFactory.create(1L, uriIndex.getIndex("http://example.com/name"), "STRING", null, "First A 2"));
        rows.add(RowFactory.create(2L, uriIndex.getIndex("http://example.com/name"), "STRING", null, "Second A"));
        rows.add(RowFactory.create(3L, uriIndex.getIndex("http://example.com/age"), "INTEGER", null, "100"));
        rows.add(RowFactory.create(3L, uriIndex.getIndex("http://example.com/name"), "STRING", "en", "First B"));
        return jsc().parallelize(rows);
    }

    private StructType getExpectedSchemaOfEAV() {
        List<StructField> fields = new ArrayList<>();

        fields.add(DataTypes.createStructField(ID_COLUMN_NAME, DataTypes.LongType, false));
        fields.add(DataTypes.createStructField(PREDICATE_COLUMN_NAME, DataTypes.IntegerType, false));
        fields.add(DataTypes.createStructField(EAV_DATATYPE_COLUMN_NAME, DataTypes.StringType, true));
        fields.add(DataTypes.createStructField(EAV_LANGUAGE_COLUMN_NAME, DataTypes.StringType, true));
        fields.add(DataTypes.createStructField(EAV_VALUE_COLUMN_NAME, DataTypes.StringType, false));

        return DataTypes.createStructType(fields);
    }

    private JavaRDD<Row> getExpectedRowsOfSingleRelationTable() {
        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create(1L, 3L, uriIndex.getIndex("http://example.com/knows")));
        rows.add(RowFactory.create(2L, 3L, uriIndex.getIndex("http://example.com/likes")));

        return jsc().parallelize(rows);
    }

    private StructType getExpectedSchemaOfSingleRelationTable() {
        List<StructField> fields = new ArrayList<>();

        fields.add(DataTypes.createStructField(ID_COLUMN_NAME + ID_FROM_SUFFIX, DataTypes.LongType, false));
        fields.add(DataTypes.createStructField(ID_COLUMN_NAME + ID_TO_SUFFIX, DataTypes.LongType, false));
        fields.add(DataTypes.createStructField(PREDICATE_COLUMN_NAME, DataTypes.IntegerType, false));

        return DataTypes.createStructType(fields);
    }

    private StructType getExpectedSchemaOfAB(boolean isPredicateStored, boolean isPredicateStoredAsURI) {
        List<StructField> fields = new ArrayList<>();

        fields.add(DataTypes.createStructField("a" + ID_SUFFIX_JOINER + ID_COLUMN_NAME, DataTypes.LongType, false));
        fields.add(DataTypes.createStructField("b" + ID_SUFFIX_JOINER + ID_COLUMN_NAME, DataTypes.LongType, false));
        if (isPredicateStored) {
            fields.add(DataTypes.createStructField(PREDICATE_COLUMN_NAME, isPredicateStoredAsURI ? DataTypes.StringType : DataTypes.IntegerType, false));
        }

        return DataTypes.createStructType(fields);
    }


    private JavaRDD<Instance> getTestInstances() {
        List<Instance> instances = new ArrayList<>();
        Instance instance;

        instance = new Instance();
        instance.setId(1L);
        instance.setType(uriIndex.getIndex("http://example.com/a"));
        instance.setUri("http://example.com/a/1");
        instance.putLiteralValue(new Predicate(uriIndex.getIndex("http://example.com/name"), LiteralType.STRING, null), new TreeSet<>(Arrays.asList("First A 1", "First A 2")));
        instance.addRelation(new RelationPredicate(uriIndex.getIndex("http://example.com/knows"), "http://example.com/b/1"));
        instances.add(instance);

        instance = new Instance();
        instance.setId(2L);
        instance.setType(uriIndex.getIndex("http://example.com/a"));
        instance.setUri("http://example.com/a/2");
        instance.putLiteralValue(new Predicate(uriIndex.getIndex("http://example.com/name"), LiteralType.STRING, null), "Second A");
        instance.addRelation(new RelationPredicate(uriIndex.getIndex("http://example.com/likes"), "http://example.com/b/1"));
        instances.add(instance);

        instance = new Instance();
        instance.setId(3L);
        instance.setType(uriIndex.getIndex("http://example.com/b"));
        instance.setUri("http://example.com/b/1");
        instance.putLiteralValue(new Predicate(uriIndex.getIndex("http://example.com/name"), LiteralType.STRING, "en"), "First B");
        instance.putLiteralValue(new Predicate(uriIndex.getIndex("http://example.com/age"), LiteralType.INTEGER, null), 100);
        instances.add(instance);

        return jsc().parallelize(instances);
    }

    private EntitySchema getTestEntitySchema() {
        List<EntityTable> tables = new ArrayList<>();
        List<EntityColumn> columns;
        Set<EntityProperty> attributes;

        EntityProperty a_name = new EntityProperty(
                new Predicate(uriIndex.getIndex("http://example.com/name"), LiteralType.STRING, null),
                false,
                1.0
        );
        columns = new ArrayList<>();
        columns.add(new EntityColumn("name", a_name));

        attributes = Sets.newHashSet(a_name);

        tables.add(new EntityTable("a", "http://example.com/a", 2L, columns, attributes));


        EntityProperty b_name = new EntityProperty(
                new Predicate(uriIndex.getIndex("http://example.com/name"), LiteralType.STRING, "en"),
                true,
                1.0
        );
        EntityProperty b_age = new EntityProperty(
                new Predicate(uriIndex.getIndex("http://example.com/age"), LiteralType.INTEGER, null),
                true,
                1.0
        );

        columns = new ArrayList<>();
        columns.add(new EntityColumn("name", b_name));
        columns.add(new EntityColumn("age", b_age));

        attributes = new HashSet<>();
        attributes.add(b_name);
        attributes.add(b_age);

        tables.add(new EntityTable("b", "http://example.com/b", 1L, columns, attributes));

        return new EntitySchema(tables, typeNames, null);
    }

    private DataFrame getTestRelations() {
        List<Row> rows = new ArrayList<>();

        rows.add(RowFactory.create(
                uriIndex.getIndex("http://example.com/knows"),
                uriIndex.getIndex("http://example.com/a"),
                1L,
                uriIndex.getIndex("http://example.com/b"),
                3L
        ));

        rows.add(RowFactory.create(
                uriIndex.getIndex("http://example.com/likes"),
                uriIndex.getIndex("http://example.com/a"),
                2L,
                uriIndex.getIndex("http://example.com/b"),
                3L
        ));

        return sql.createDataFrame(rows, new StructType()
                .add("predicateIndex", DataTypes.IntegerType, false)
                .add("fromTypeIndex", DataTypes.IntegerType, false)
                .add("fromID", DataTypes.LongType, false)
                .add("toTypeIndex", DataTypes.IntegerType, false)
                .add("toID", DataTypes.LongType, false)
        );
    }

    private RelationSchema getTestSingleRelationSchema() {
        return new RelationSchema("single_relations");
    }

    private RelationSchema getTestRelationSchema() {
        List<RelationTable> tables = new ArrayList<>();

        tables.add(new RelationTable("a_b", RelationSchemaStrategy.Types).setEntityFilter(
                new RelationEntityFilter(
                        uriIndex.getIndex("http://example.com/a"),
                        "a",
                        uriIndex.getIndex("http://example.com/b"),
                        "b")
                )
        );

        return new RelationSchema(tables);
    }
}
