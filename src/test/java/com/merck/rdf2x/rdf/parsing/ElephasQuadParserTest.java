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

package com.merck.rdf2x.rdf.parsing;

import com.merck.rdf2x.test.TestSparkContextProvider;
import com.merck.rdf2x.test.TestUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.jena.sparql.core.Quad;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * Test of {@link ElephasQuadParser}
 */
@Slf4j
public class ElephasQuadParserTest extends TestSparkContextProvider {


    /**
     * Test if expected quads are parsed from N-Quads and Turtle format
     */
    @Test
    public void testParseQuads() {
        QuadParser parser = new ElephasQuadParser(
                new QuadParserConfig()
                        .setBatchSize(2),
                jsc()
        );
        String[] datasetPaths = new String[]{"parserTest.nq", "parserTest.ttl"};
        JavaRDD<String> expected = getExpectedRDD();

        for (String datasetPath : datasetPaths) {
            JavaRDD<String> parsed = parser
                    .parseQuads(TestUtils.getDatasetPath(datasetPath))
                    .map(ElephasQuadParserTest::quadToRawString);
            assertRDDEquals("Parsed file " + datasetPath + " equals expected RDD", expected, parsed);
        }
    }

    private JavaRDD<String> getExpectedRDD() {
        List<String> rdd = new LinkedList<>();
        rdd.add("[http://t.com/flavors#Sweet, http://www.w3.org/1999/02/22-rdf-syntax-ns#type, http://t.com/types#Flavor]");
        rdd.add("[http://t.com/flavors#Sour, http://www.w3.org/1999/02/22-rdf-syntax-ns#type, http://t.com/types#Flavor]");
        rdd.add("[http://t.com/fruit#Apple, http://t.com/rel#Flavor, http://t.com/flavors#Sweet]");
        rdd.add("[http://t.com/fruit#Apple, http://www.w3.org/1999/02/22-rdf-syntax-ns#type, http://t.com/types#Fruit]");
        rdd.add("[http://t.com/fruit#Apple, http://t.com/rel#Weight, \"100\"^^http://www.w3.org/2001/XMLSchema#int]");
        rdd.add("[http://t.com/fruit#Apple, http://t.com/rel#Weight, \"150\"^^http://www.w3.org/2001/XMLSchema#int]");
        rdd.add("[http://t.com/fruit#Apple, http://t.com/rel#Weight, \"TBD\"]");
        rdd.add("[http://t.com/fruit#Tomato, http://t.com/rel#Flavor, http://t.com/flavors#Sweet]");
        rdd.add("[http://t.com/fruit#Tomato, http://t.com/rel#Flavor, http://t.com/flavors#Sour]");
        rdd.add("[http://t.com/fruit#Tomato, http://t.com/rel#Weight, \"100\"^^http://www.w3.org/2001/XMLSchema#int]");
        rdd.add("[http://t.com/fruit#Tomato, http://another.com/rel#Weight, \"110\"^^http://www.w3.org/2001/XMLSchema#int]");
        rdd.add("[http://t.com/fruit#Tomato, http://www.w3.org/1999/02/22-rdf-syntax-ns#type, http://t.com/types#Fruit]");
        rdd.add("[http://t.com/fruit#Tomato, http://www.w3.org/1999/02/22-rdf-syntax-ns#type, http://t.com/types#Vegetable]");
        rdd.add("[http://t.com/fruit#Cucumber, http://www.w3.org/1999/02/22-rdf-syntax-ns#type, http://t.com/types#Vegetable]");

        return jsc().parallelize(rdd);
    }

    private static String quadToRawString(Quad quad) {
        return Arrays.asList(new String[]{
                quad.getSubject().getURI(),
                quad.getPredicate().getURI(),
                quad.getObject().toString()
        }).toString();
    }
}