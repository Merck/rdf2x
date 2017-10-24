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
package com.merck.rdf2x.rdf.schema;

import com.beust.jcommander.Parameter;
import lombok.Data;
import lombok.experimental.Accessors;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * RdfSchemaCollectorConfig stores parameters for the {@link RdfSchemaCollector}.
 */
@Data
@Accessors(chain = true)
public class RdfSchemaCollectorConfig implements Serializable {
    @Parameter(names = "--rdf.typePredicate", description = "Additional URI apart from rdf:type to treat as type predicate. You can specify more predicates by repeating this parameter.", hidden = true)
    private List<String> typePredicates = new ArrayList<>();

    @Parameter(names = "--rdf.subclassPredicate", description = "Additional URI apart from rdfs:subClassOf to treat as subClassOf predicate. You can specify more predicates by repeating this parameter.", hidden = true)
    private List<String> subclassPredicates = new ArrayList<>();

    @Parameter(names = "--rdf.collectSubclassGraph", arity = 1, description = "Whether to collect the graph of subClass predicates.", hidden = true)
    private boolean collectSubclassGraph = true;

    @Parameter(names = "--rdf.collectLabels", arity = 1, description = "Whether to collect type and predicate labels (to be saved in meta tables and for name formatting if requested).", hidden = true)
    private boolean collectLabels = true;

    @Parameter(names = "--rdf.cacheFile", description = "File for saving and loading cached schema.", hidden = true)
    private String cacheFile = null;
    /**
     * List of additional type IRIs to include in the type index
     */
    private List<String> additionalTypes = new ArrayList<>();
    /**
     * List of additional predicate IRIs to include in the predicate index
     */
    private List<String> additionalPredicates = new ArrayList<>();
}
