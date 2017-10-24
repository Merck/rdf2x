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

package com.merck.rdf2x.processing.relations;

import com.merck.rdf2x.beans.Instance;
import com.merck.rdf2x.beans.RelationPredicate;
import com.merck.rdf2x.processing.schema.RelationConfig;
import com.merck.rdf2x.rdf.schema.ClassGraph;
import com.merck.rdf2x.test.TestSparkContextProvider;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Test of {@link RelationExtractor}
 */
@Slf4j
public class RelationExtractorTest extends TestSparkContextProvider {
    /**
     * Test if expected directed relations are collected from a RDD of Instances
     */
    @Test
    public void testCollectRelations() {
        SQLContext sql = new SQLContext(jsc());

        RelationExtractor collector = new RelationExtractor(
                new RelationConfig(),
                jsc(),
                new ClassGraph()
        );

        List<Row> rdd = new ArrayList<>();

        // cycle one -> two -> three -> one
        rdd.add(RowFactory.create(0, 1, 1L, 1, 2L));
        rdd.add(RowFactory.create(0, 1, 2L, 1, 3L));
        rdd.add(RowFactory.create(0, 1, 3L, 1, 1L));

        // one -> four, four -> one
        rdd.add(RowFactory.create(0, 2, 4L, 1, 1L));
        rdd.add(RowFactory.create(0, 1, 1L, 2, 4L));

        // five -> one
        rdd.add(RowFactory.create(0, 3, 5L, 1, 1L));

        DataFrame expected = sql.createDataFrame(rdd, new StructType()
                .add("predicateIndex", DataTypes.IntegerType, false)
                .add("fromTypeIndex", DataTypes.IntegerType, false)
                .add("fromID", DataTypes.LongType, false)
                .add("toTypeIndex", DataTypes.IntegerType, false)
                .add("toID", DataTypes.LongType, false)
        );

        // (predicateIndex, fromTypeIndex, instanceID, toTypeIndex, relatedID)
        DataFrame result = collector.extractRelations(getTestRDD());

        assertEquals("Expected relation row schema is collected", expected.schema(), result.schema());
        assertRDDEquals("Expected relation rows are collected", expected.javaRDD(), result.javaRDD());
    }

    private JavaRDD<Instance> getTestRDD() {
        List<Instance> rdd = new ArrayList<>();

        Instance one = new Instance();
        one.setType(1);
        one.setUri("http://example.com/a/one");
        one.setId(1L);
        rdd.add(one);

        Instance two = new Instance();
        two.setType(1);
        two.setUri("http://example.com/a/two");
        two.setId(2L);
        rdd.add(two);

        Instance three = new Instance();
        three.setType(1);
        three.setUri("http://example.com/a/three");
        three.setId(3L);
        rdd.add(three);

        Instance four = new Instance();
        four.setType(2);
        four.setUri("http://example.com/b/four");
        four.setId(4L);
        rdd.add(four);

        Instance five = new Instance();
        five.setType(3);
        five.setUri("http://example.com/c/five");
        five.setId(5L);
        rdd.add(five);

        // cycle one -> two -> three -> one
        one.addRelation(new RelationPredicate(0, two.getUri()));
        two.addRelation(new RelationPredicate(0, three.getUri()));
        three.addRelation(new RelationPredicate(0, one.getUri()));

        // one -> four, four -> one
        one.addRelation(new RelationPredicate(0, four.getUri()));
        four.addRelation(new RelationPredicate(0, one.getUri()));

        // five -> one
        five.addRelation(new RelationPredicate(0, one.getUri()));

        return jsc().parallelize(rdd);
    }

}
