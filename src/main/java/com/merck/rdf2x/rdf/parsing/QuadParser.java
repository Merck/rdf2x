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

import org.apache.jena.sparql.core.Quad;
import org.apache.spark.api.java.JavaRDD;

/**
 * QuadParser defines an interface for parsing RDF files into a RDD of Quads.
 */
public interface QuadParser {

    /**
     * Parse RDF file into a RDD of Jena {@link Quad}s.
     *
     * @param path Path to RDF file or folder
     * @return RDD of Jena Quads
     */
    JavaRDD<Quad> parseQuads(String path);

}
