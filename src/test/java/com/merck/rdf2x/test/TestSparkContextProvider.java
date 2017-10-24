package com.merck.rdf2x.test;

import com.holdenkarau.spark.testing.JavaRDDComparisons;
import com.holdenkarau.spark.testing.SharedJavaSparkContext;
import com.merck.rdf2x.spark.SparkContextProvider;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import scala.Option;
import scala.Tuple3;

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
@Slf4j
public class TestSparkContextProvider extends SharedJavaSparkContext {

    @Override
    public SparkConf conf() {
        SparkConf conf = super.conf();
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.registerKryoClasses(SparkContextProvider.getSerializableClasses());
        return conf;
    }

    @Override
    public void runBefore() {
        super.runBefore();
        jsc().setLogLevel("WARN");
    }

    public <T> void assertRDDEquals(String message, JavaRDD<T> expected, JavaRDD<T> result) {
        Option<Tuple3<T, Integer, Integer>> diff = JavaRDDComparisons.compareRDD(expected, result);
        if (diff.isDefined()) {
            log.error("EXPECTED");
            expected.foreach(row -> log.error(row.toString()));
            log.error("RESULT");
            result.foreach(row -> log.error(row.toString()));
            log.error("FIRST DIFF");
            Tuple3<T, Integer, Integer> diffTriple = diff.get();
            log.error(diffTriple.toString());
            if (diffTriple._2() == 0) {
                log.error("(row not expected but present in result {} times)", diffTriple._3());
            }
            if (diffTriple._3() == 0) {
                log.error("(row expected {} times but not present)", diffTriple._2());
            }
            throw new AssertionError(message);
        }
    }

    public <T> void assertRDDEquals(JavaRDD<T> expected, JavaRDD<T> result) {
        assertRDDEquals("Datasets are equal.", expected, result);
    }

    public <T> void assertRDDToStringEquals(String message, JavaRDD<T> expected, JavaRDD<T> result) {
        JavaRDD<String> expectedToString = expected.map(Object::toString);
        JavaRDD<String> resultToString = result.map(Object::toString);
        assertRDDEquals(message, expectedToString, resultToString);
    }

    public <T> void assertRDDToStringEquals(JavaRDD<T> expected, JavaRDD<T> result) {
        assertRDDToStringEquals("Datasets are equal.", expected, result);
    }
}
