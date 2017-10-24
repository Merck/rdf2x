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

import com.beust.jcommander.Parameter;
import lombok.Data;
import lombok.experimental.Accessors;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * SparkConfig stores all information needed to connect to Spark.
 */
@Data
@Accessors(chain = true)
public class QuadParserConfig implements Serializable {

    @Parameter(names = "--input.lineBasedFormat", arity = 1, description = "Whether the input files can be read line by line (e.g. true for NTriples or NQuads, false for Turtle). In default, will try to guess based on file extension. Line based formats can be parsed by multiple nodes at the same time, other formats will be read by master node and repartitioned after parsing.")
    private Boolean lineBasedFormat = null;

    @Parameter(names = "--input.repartition", description = "Repartition after parsing into this number of partitions.")
    private Integer repartition = null;

    @Parameter(names = "--input.batchSize", description = "Batch size for parsing line-based formats (number of quads per partition)")
    private Integer batchSize = 500000;

    @Parameter(names = "--input.errorHandling", description = "How to handle RDF parsing errors (Ignore, Throw).")
    private ParseErrorHandling errorHandling = ParseErrorHandling.Ignore;

    @Parameter(names = "--input.acceptedLanguage", description = "Accepted language. Literals in other languages are ignored. You can specify more languages by repeating this parameter.")
    private List<String> acceptedLanguages = new ArrayList<>();
}
