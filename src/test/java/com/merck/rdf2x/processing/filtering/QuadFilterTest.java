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

import com.merck.rdf2x.test.TestSparkContextProvider;
import com.merck.rdf2x.test.TestUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.jena.sparql.core.Quad;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Before;
import org.junit.Test;

import javax.naming.ConfigurationException;
import java.io.Serializable;
import java.util.Arrays;

/**
 * Test of {@link QuadFilter}
 */
@Slf4j
public class QuadFilterTest extends TestSparkContextProvider implements Serializable {

    private JavaRDD<Quad> testRDD;

    @Before
    public void setUp() {
        testRDD = TestUtils.getQuadsRDD(jsc(), "filtering/input.nq");
    }

    @Test
    public void testEmptyFilter() {
        QuadFilterConfig config = new QuadFilterConfig();
        QuadFilter filter = new QuadFilter(config);
        assertRDDToStringEquals("Empty filter returns original RDD", testRDD, filter.filter(testRDD));
    }

    @Test(expected = ConfigurationException.class)
    public void testRequiredResourcesWithNonZeroDepth() throws ConfigurationException {
        QuadFilterConfig config = new QuadFilterConfig()
                .setRelatedDepth(1);
        config.validate();
    }

    @Test
    public void testDirectedResourceFilter() {
        for (int depth = 0; depth <= 3; depth++) {
            QuadFilterConfig config = new QuadFilterConfig()
                    .setRelatedDepth(depth)
                    .setResources(Arrays.asList("http://t.com/a/1", "http://t.com/a/3"));
            QuadFilter filter = new QuadFilter(config);
            JavaRDD<Quad> expected = TestUtils.getQuadsRDD(jsc(), "filtering/resource" + depth + ".nq");
            JavaRDD<Quad> result = filter.filter(testRDD);
            assertRDDToStringEquals("Directed filter on resources with depth " + depth, expected, result);
        }
    }

    @Test
    public void testUndirectedResourceFilter() {
        for (int depth = 0; depth <= 3; depth++) {
            QuadFilterConfig config = new QuadFilterConfig()
                    .setRelatedDepth(depth)
                    .setDirected(false)
                    .setResources(Arrays.asList("http://t.com/b"));
            QuadFilter filter = new QuadFilter(config);
            JavaRDD<Quad> expected = TestUtils.getQuadsRDD(jsc(), "filtering/type" + depth + ".nq");
            JavaRDD<Quad> result = filter.filter(testRDD);
            assertRDDToStringEquals("Undirected filter on type with depth " + depth, expected, result);
        }
    }


}