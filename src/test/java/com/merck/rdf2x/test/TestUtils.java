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

package com.merck.rdf2x.test;

import com.merck.rdf2x.rdf.parsing.ElephasQuadParser;
import com.merck.rdf2x.rdf.parsing.QuadParser;
import com.merck.rdf2x.rdf.parsing.QuadParserConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.jena.sparql.core.Quad;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.stream.Collectors;

import static org.junit.Assert.assertTrue;

/**
 * TestUtils stores various methods used for testing
 */
@Slf4j
public class TestUtils {
    /**
     * Assert all names in a collection are unique
     *
     * @param message assert message
     * @param names   collection of names to test against
     */
    public static void assertUniqueNames(String message, Collection<String> names) {
        boolean isUnique = new HashSet<>(names).size() == names.size();
        if (!isUnique) {
            log.error("Not unique names:");
            log.error(names.stream().filter(name -> Collections.frequency(names, name) > 1).collect(Collectors.toList()).toString());
        }
        assertTrue(message, isUnique);
    }

    /**
     * Get path of a dataset in the test resources folder
     */
    public static String getDatasetPath(String datasetPath) {
        URL url = TestUtils.class.getClassLoader().getResource("test/datasets/" + datasetPath);
        return url.getPath();
    }

    /**
     * Parse RDF file from resources folder
     * @param sc spark context to use for parsing
     * @param fileName name of the file to parse
     * @return RDD of quads from the requested file
     */
    public static JavaRDD<Quad> getQuadsRDD(JavaSparkContext sc, String fileName) {
        QuadParser parser = new ElephasQuadParser(
                new QuadParserConfig()
                        .setBatchSize(2),
                sc
        );
        String path = TestUtils.getDatasetPath(fileName);
        return parser.parseQuads(path);
    }
}
