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

package com.merck.rdf2x.processing.filtering;

import com.merck.rdf2x.beans.IndexMap;
import com.merck.rdf2x.beans.Instance;
import com.merck.rdf2x.test.TestSparkContextProvider;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Test of {@link InstanceFilter}
 */
@Slf4j
public class InstanceFilterTest extends TestSparkContextProvider implements Serializable {

    private IndexMap<String> typeIndex;
    private JavaRDD<Instance> testRDD;

    @Before
    public void setUp() {
        typeIndex = new IndexMap<>(Arrays.asList(
                "http://example.com/a/type",
                "http://example.com/b/type",
                "http://example.com/c/type"
        ));
        testRDD = getTestRDD();
    }

    @Test
    public void testEmptyTypeFilter() {
        InstanceFilterConfig config = new InstanceFilterConfig();
        InstanceFilter filter = new InstanceFilter(config);
        assertRDDToStringEquals("Empty filter returns original RDD", testRDD, filter.filter(testRDD, typeIndex));
    }

    @Test
    public void testFilterWithIgnoreTypes() {
        InstanceFilterConfig config = new InstanceFilterConfig()
                .setIgnoreOtherTypes(true)
                .setTypes(Arrays.asList("http://example.com/a/type", "http://example.com/b/type"));
        InstanceFilter filter = new InstanceFilter(config);
        JavaRDD<Instance> result = filter.filter(testRDD, typeIndex);

        List<Instance> rdd = new ArrayList<>();

        Instance one = new Instance();
        one.addType(typeIndex.getIndex("http://example.com/a/type"));
        one.setUri("http://example.com/one");
        rdd.add(one);

        Instance two = new Instance();
        two.addType(typeIndex.getIndex("http://example.com/b/type"));
        two.setUri("http://example.com/two");
        rdd.add(two);

        JavaRDD<Instance> expected = jsc().parallelize(rdd);

        assertRDDEquals("Expected instances with restricted types were filtered out", expected, result);
    }

    @Test
    public void testFilterWithoutIgnoreTypes() {
        InstanceFilterConfig config = new InstanceFilterConfig()
                .setIgnoreOtherTypes(false)
                .setTypes(Arrays.asList("http://example.com/a/type", "http://example.com/b/type"));
        InstanceFilter filter = new InstanceFilter(config);
        JavaRDD<Instance> result = filter.filter(testRDD, typeIndex);

        List<Instance> rdd = new ArrayList<>();

        Instance one = new Instance();
        one.addType(typeIndex.getIndex("http://example.com/a/type"));
        one.setUri("http://example.com/one");
        rdd.add(one);

        Instance two = new Instance();
        two.addType(typeIndex.getIndex("http://example.com/b/type"));
        two.addType(typeIndex.getIndex("http://example.com/c/type"));
        two.setUri("http://example.com/two");
        rdd.add(two);

        JavaRDD<Instance> expected = jsc().parallelize(rdd);

        assertRDDEquals("Expected instances without restricted types were filtered out", expected, result);
    }

    private JavaRDD<Instance> getExpectedRDD() {
        List<Instance> rdd = new ArrayList<>();

        Instance one = new Instance();
        one.addType(typeIndex.getIndex("http://example.com/a/type"));
        one.setUri("http://example.com/one");
        rdd.add(one);

        Instance two = new Instance();
        two.addType(typeIndex.getIndex("http://example.com/b/type"));
        two.addType(typeIndex.getIndex("http://example.com/c/type"));
        two.setUri("http://example.com/two");
        rdd.add(two);

        Instance three = new Instance();
        three.addType(typeIndex.getIndex("http://example.com/c/type"));
        three.setUri("http://example.com/three");
        rdd.add(three);

        return jsc().parallelize(rdd);
    }

    private JavaRDD<Instance> getTestRDD() {
        List<Instance> rdd = new ArrayList<>();

        Instance one = new Instance();
        one.addType(typeIndex.getIndex("http://example.com/a/type"));
        one.setUri("http://example.com/one");
        rdd.add(one);

        Instance two = new Instance();
        two.addType(typeIndex.getIndex("http://example.com/b/type"));
        two.addType(typeIndex.getIndex("http://example.com/c/type"));
        two.setUri("http://example.com/two");
        rdd.add(two);

        Instance three = new Instance();
        three.addType(typeIndex.getIndex("http://example.com/c/type"));
        three.setUri("http://example.com/three");
        rdd.add(three);

        return jsc().parallelize(rdd);
    }
}