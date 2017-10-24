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

package com.merck.rdf2x.processing.formatting;

import com.beust.jcommander.Parameter;
import com.merck.rdf2x.flavors.Flavor;
import lombok.Data;
import lombok.experimental.Accessors;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * SchemaFormatterConfig stores all instructions for the SchemaFormatter.
 */

@Data
@Accessors(chain = true)
public class SchemaFormatterConfig implements Serializable {
    @Parameter(names = "--formatting.maxTableNameLength", description = "Maximum length of entity table names")
    private Integer maxTableNameLength = 25;

    @Parameter(names = "--formatting.maxColumnNameLength", description = "Maximum length of column names")
    private Integer maxColumnNameLength = 50;

    @Parameter(names = "--formatting.uriSuffixPattern", description = "When collecting name from URI, use the segment after the last occurrence of this regex")
    private String uriSuffixPattern = "[/:#=]";

    @Parameter(names = "--formatting.useLabels", arity = 1, description = "Try to use rdfs:label for formatting names. Will use URIs if label is not present.")
    private boolean useLabels = false;

    private Set<String> reservedNames = new HashSet<>();
    /**
     * Flavor containing custom conversion methods
     */
    private Flavor flavor;
}
