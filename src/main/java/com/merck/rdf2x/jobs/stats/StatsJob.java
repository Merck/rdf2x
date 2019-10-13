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

package com.merck.rdf2x.jobs.stats;

import com.merck.rdf2x.rdf.parsing.ElephasQuadParser;
import com.merck.rdf2x.rdf.parsing.QuadParser;
import com.merck.rdf2x.stats.QuadCounter;
import lombok.extern.slf4j.Slf4j;
import org.apache.jena.sparql.core.Quad;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import javax.naming.ConfigurationException;
import java.util.List;

import static com.merck.rdf2x.jobs.stats.StatsConfig.Stat.*;

/**
 * StatsJob computes various stats on RDF datasets.
 * <p>
 * The instructions are passed in a {@link StatsConfig} object.
 */
@Slf4j
public class StatsJob implements Runnable {
    /**
     * job config with all instructions
     */
    private final StatsConfig config;
    /**
     * Spark context to be used
     */
    private final JavaSparkContext sc;

    /**
     * Subject URI count
     */
    private Long subjectURICount;

    /**
     * Predicate URI count
     */
    private Long predicateURICount;

    /**
     * Object URI count
     */
    private Long objectURICount;

    public StatsJob(StatsConfig config, JavaSparkContext sc) throws ConfigurationException {
        this.config = config;
        this.sc = sc;
    }


    /**
     * Run the job
     */
    @Override
    public void run() {

        // create all required processors
        QuadParser parser = new ElephasQuadParser(
                config.getParserConfig(),
                sc
        );

        String inputFile = config.getInputFile();
        log.info("Preparing input file: {}", inputFile);
        JavaRDD<Quad> quads = parser.parseQuads(inputFile);
        log.info("Done preparing RDD of quads with {} partitions", quads.getNumPartitions());

        List<StatsConfig.Stat> stats = config.getStats();

        if (stats.contains(SUBJECT_URI_COUNT)) {
            log.info("----------------------------");
            log.info("Subject URI Stats:");
            JavaPairRDD<Long, String> counts = QuadCounter.countBySubjectURI(quads).mapToPair(Tuple2::swap);
            counts.sortByKey(false).take(100).forEach(uriCount -> {
                log.info(uriCount.toString());
            });
            subjectURICount = counts.count();
            stats.remove(SUBJECT_URI_COUNT);
        }

        if(stats.contains(PREDICATE_URI_COUNT)) {
            log.info("----------------------------");
            log.info("Predicate URI stats:");
            JavaPairRDD<Long, String> counts = QuadCounter.countByPredicateURI(quads).mapToPair(Tuple2::swap);
            counts.sortByKey(false).take(100).forEach(uriCount -> {
                log.info(uriCount.toString());
            });
            predicateURICount = counts.count();
            stats.remove(PREDICATE_URI_COUNT);
        }

        if(stats.contains(OBJECT_URI_COUNT)) {
            log.info("----------------------------");
            log.info("Object URI stats:");
            JavaPairRDD<Long, String> counts = QuadCounter.getObjectURI(quads).mapToPair(Tuple2::swap);
            counts.sortByKey(false).take(100).forEach( uriCount -> {
                log.info(uriCount.toString());
            });
            objectURICount = counts.count();
            stats.remove(OBJECT_URI_COUNT);
        }

        printStats();

    }

    /**
     * Prints stats for distinct counts of {@link Quad} properties i.e. subject, predicate and object
     */
    private void printStats() {
        log.info("---------------------------------");
        if(subjectURICount != null) {
            log.info("Total Distinct Subject URIs: {}", subjectURICount);
        }
        if(predicateURICount != null) {
            log.info("Total Distinct Predicate URIs: {}", predicateURICount);
        }
        if(objectURICount != null) {
            log.info("Total Distinct Object URIs: {}", objectURICount);
        }
        log.info("---------------------------------");
    }

}
