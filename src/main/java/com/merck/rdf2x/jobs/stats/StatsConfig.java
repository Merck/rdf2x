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

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import com.merck.rdf2x.processing.aggregating.InstanceAggregatorConfig;
import com.merck.rdf2x.rdf.parsing.QuadParserConfig;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.util.List;

/**
 * StatsConfig stores all parameters used for a {@link StatsJob}.
 * It is annotated by jCommander Parameters used to load values from the command line.
 * <p>
 */
@Data
@Accessors(chain = true)
@NoArgsConstructor
@AllArgsConstructor
@Parameters(commandDescription = "Compute various stats on RDF datasets")
public class StatsConfig {

    @Parameter(names = "--input.file", description = "Path to input file or folder", required = true)
    private String inputFile;

    @Parameter(names = "--stat", description = "Stat to compute (multiple stats can be added using '--stat A --stat B')", required = true)
    private List<Stat> stats;

    @Parameter(names = "--help", description = "Show usage page", help = true)
    private boolean help = false;

    @ParametersDelegate()
    private QuadParserConfig parserConfig = new QuadParserConfig();

    @ParametersDelegate()
    private InstanceAggregatorConfig aggregatorConfig = new InstanceAggregatorConfig();

    public enum Stat {
        /**
         * Count number of occurrences of each subject URI
         */
        SUBJECT_URI_COUNT,

        /**
         * Count number of occurrences of each predicate URI
         */
        PREDICATE_URI_COUNT,

        /**
         * Count number of occurrences of each object URI
         */
        OBJECT_URI_COUNT
    }
}
