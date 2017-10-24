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

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.jena.hadoop.rdf.io.RdfIOConstants;
import org.apache.jena.hadoop.rdf.io.input.TriplesOrQuadsInputFormat;
import org.apache.jena.hadoop.rdf.io.input.nquads.NQuadsInputFormat;
import org.apache.jena.hadoop.rdf.types.QuadWritable;
import org.apache.jena.sparql.core.Quad;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

/**
 * ElephasQuadParser parses a variety of RDF formats into a RDD of {@link Quad}. It is based on Jena Elephas Hadoop parser.
 * <p>
 * It operates in two modes: Line based (NQuads) and Whole file based (Turtle, JSON-LD, ...).
 */
@Slf4j
@RequiredArgsConstructor
public class ElephasQuadParser implements QuadParser {
    /**
     * parser config
     */
    private final QuadParserConfig config;
    /**
     * Spark context to be used
     */
    transient private final JavaSparkContext sc;

    @Override
    public JavaRDD<Quad> parseQuads(String path) {

        Configuration conf = new Configuration();

        Integer batchSize = config.getBatchSize();
        conf.set(NLineInputFormat.LINES_PER_MAP, batchSize.toString());

        if (config.getErrorHandling() == ParseErrorHandling.Throw) {
            conf.set(RdfIOConstants.INPUT_IGNORE_BAD_TUPLES, "false");
        } else {
            conf.set(RdfIOConstants.INPUT_IGNORE_BAD_TUPLES, "true");
        }

        Boolean isLineBased = config.getLineBasedFormat();
        if (isLineBased == null) {
            isLineBased = guessIsLineBasedFormat(path);
        }
        JavaRDD<Quad> quads;
        Integer partitions = config.getRepartition();
        if (isLineBased) {
            log.info("Parsing RDF in parallel with batch size: {}", batchSize);
            quads = sc.newAPIHadoopFile(path,
                    NQuadsInputFormat.class,
                    LongWritable.class, // position
                    QuadWritable.class, // value
                    conf).values().map(QuadWritable::get);
        } else {
            // let Jena guess the format, load whole files
            log.info("Input format is not line based, parsing RDF by Master node only.");
            quads = sc.newAPIHadoopFile(path,
                    TriplesOrQuadsInputFormat.class,
                    LongWritable.class, // position
                    QuadWritable.class, // value
                    conf).values().map(QuadWritable::get);

            if (partitions == null) {
                log.warn("Reading non-line based formats by master node only, consider setting --parsing.repartition to redistribute work to other nodes.");
            }
        }
        if (partitions != null) {
            log.info("Distributing workload, repartitioning into {} partitions", partitions);
            quads = quads.repartition(partitions);
        }


        final List<String> acceptedLanguages = config.getAcceptedLanguages();
        // if only some languages are accepted
        if (!acceptedLanguages.isEmpty()) {
            // filter out literals of unsupported languages
            quads = quads.filter(quad ->
                    !quad.getObject().isLiteral() ||
                            quad.getObject().getLiteralLanguage() == null ||
                            quad.getObject().getLiteralLanguage().isEmpty() ||
                            acceptedLanguages.contains(quad.getObject().getLiteralLanguage())
            );
        }

        return quads;
    }

    private boolean guessIsLineBasedFormat(String path) {
        if (path.endsWith(".nq") || path.endsWith(".nq.gz") || path.endsWith(".nt") || path.endsWith(".nt.gz")) {
            return true;
        } else {
            log.warn("Unable to guess input file format, parsing by master node only.");
            return false;
        }
    }


}