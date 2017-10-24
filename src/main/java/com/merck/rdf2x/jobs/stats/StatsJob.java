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

import static com.merck.rdf2x.jobs.stats.StatsConfig.Stat.SUBJECT_URI_COUNT;

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
            log.info("----------------------------");
            log.info("Total Distinct Subject URIs: {}", counts.count());
            log.info("----------------------------");
            stats.remove(SUBJECT_URI_COUNT);
        }

    }

}
