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

import com.merck.rdf2x.beans.IndexMap;
import com.merck.rdf2x.persistence.schema.EntitySchema;
import com.merck.rdf2x.persistence.schema.RelationEntityFilter;
import com.merck.rdf2x.persistence.schema.RelationSchema;
import com.merck.rdf2x.persistence.schema.RelationTable;
import com.merck.rdf2x.processing.formatting.SchemaFormatter;
import com.merck.rdf2x.rdf.schema.ClassGraph;
import com.merck.rdf2x.rdf.schema.RdfSchema;
import com.merck.rdf2x.rdf.schema.RdfSchemaCollectorConfig;
import com.merck.rdf2x.test.TestSparkContextProvider;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static com.merck.rdf2x.processing.schema.RelationSchemaCollector.SINGLE_RELATION_TABLE_NAME;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("ConstantConditions")
@Slf4j
public class RelationSchemaCollectorTest extends TestSparkContextProvider {
    private IndexMap<String> uriIndex;
    private EntitySchema mockEntitySchema;
    private RdfSchema schema;
    private SchemaFormatter mockFormatter;
    private Map<String, String> relationNames;

    @Before
    public void setUp() {

        uriIndex = new IndexMap<>(Arrays.asList(
                "http://example.com/a",
                "http://example.com/b",
                "http://example.com/c",
                "http://example.com/relation"
        ));

        Map<String, String> typeNames = new HashMap<>();
        typeNames.put("http://example.com/a", "a");
        typeNames.put("http://example.com/b", "b");
        typeNames.put("http://example.com/c", "c");

        relationNames = new HashMap<>();
        relationNames.put("http://example.com/relation", "relation");

        mockEntitySchema = mock(EntitySchema.class);
        when(mockEntitySchema.getTableNames()).thenReturn(typeNames);

        schema = new RdfSchema(
                new RdfSchemaCollectorConfig(),
                new ClassGraph(),
                uriIndex,
                uriIndex,
                new HashMap<>()
        );

        mockFormatter = mock(SchemaFormatter.class);
        when(mockFormatter.getRelationNames(any(Collection.class), any(Map.class), any(Set.class))).thenReturn(relationNames);
    }

    /**
     * Test if expected directed schema is extracted from the test EntitySchema and RDD of RelationRows
     */
    @Test
    public void testMultipleRelationTablesSchema() {
        RelationSchemaCollector extractor = new RelationSchemaCollector(
                new RelationConfig(),
                mockFormatter,
                schema
        );

        List<RelationTable> tables = new ArrayList<>();
        tables.add(new RelationTable("a_a", RelationSchemaStrategy.Types).setEntityFilter(new RelationEntityFilter(uriIndex.getIndex("http://example.com/a"), "a", uriIndex.getIndex("http://example.com/a"), "a")));
        tables.add(new RelationTable("a_b", RelationSchemaStrategy.Types).setEntityFilter(new RelationEntityFilter(uriIndex.getIndex("http://example.com/a"), "a", uriIndex.getIndex("http://example.com/b"), "b")));
        tables.add(new RelationTable("b_a", RelationSchemaStrategy.Types).setEntityFilter(new RelationEntityFilter(uriIndex.getIndex("http://example.com/b"), "b", uriIndex.getIndex("http://example.com/a"), "a")));
        tables.add(new RelationTable("c_a", RelationSchemaStrategy.Types).setEntityFilter(new RelationEntityFilter(uriIndex.getIndex("http://example.com/c"), "c", uriIndex.getIndex("http://example.com/a"), "a")));
        RelationSchema expectedSchema = new RelationSchema(tables);

        RelationSchema schema = extractor.collectSchema(
                getTestRDD(),
                mockEntitySchema,
                uriIndex);

        assertEquals("Relation schema matches", expectedSchema, schema);
    }

    /**
     * Test if expected directed schema is extracted from the test EntitySchema and RDD of RelationRows
     */
    @Test
    public void testSingleRelationTableSchema() {
        RelationSchemaCollector extractor = new RelationSchemaCollector(
                new RelationConfig()
                        .setSchemaStrategy(RelationSchemaStrategy.SingleTable),
                mockFormatter,
                schema
        );

        RelationSchema expectedSchema = new RelationSchema(SINGLE_RELATION_TABLE_NAME);

        RelationSchema schema = extractor.collectSchema(
                getTestRDD(),
                mockEntitySchema,
                uriIndex);

        assertEquals("Single relation schema matches", expectedSchema, schema);
    }

    private DataFrame getTestRDD() {
        SQLContext sql = new SQLContext(jsc());
        List<Row> rdd = new ArrayList<>();

        // cycle one -> two -> three -> one
        rdd.add(RowFactory.create(0, uriIndex.getIndex("http://example.com/a"), 1L, uriIndex.getIndex("http://example.com/a"), 2L));
        rdd.add(RowFactory.create(0, uriIndex.getIndex("http://example.com/a"), 2L, uriIndex.getIndex("http://example.com/a"), 3L));
        rdd.add(RowFactory.create(0, uriIndex.getIndex("http://example.com/a"), 3L, uriIndex.getIndex("http://example.com/a"), 1L));

        // one -> four, four -> one
        rdd.add(RowFactory.create(0, uriIndex.getIndex("http://example.com/a"), 1L, uriIndex.getIndex("http://example.com/b"), 4L));
        rdd.add(RowFactory.create(0, uriIndex.getIndex("http://example.com/b"), 4L, uriIndex.getIndex("http://example.com/a"), 1L));

        // five -> one
        rdd.add(RowFactory.create(0, uriIndex.getIndex("http://example.com/c"), 5L, uriIndex.getIndex("http://example.com/a"), 1L));

        return sql.createDataFrame(rdd, new StructType()
                .add("predicateIndex", DataTypes.IntegerType, false)
                .add("fromTypeIndex", DataTypes.IntegerType, false)
                .add("fromID", DataTypes.LongType, false)
                .add("toTypeIndex", DataTypes.IntegerType, false)
                .add("toID", DataTypes.LongType, false)
        );
    }

}
