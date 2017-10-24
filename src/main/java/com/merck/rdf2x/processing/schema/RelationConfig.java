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

package com.merck.rdf2x.processing.schema;

import com.beust.jcommander.Parameter;
import com.merck.rdf2x.processing.relations.RelationExtractor;
import lombok.Data;
import lombok.experimental.Accessors;

import java.io.Serializable;

import static com.merck.rdf2x.processing.schema.RelationSchemaStrategy.Types;

/**
 * RelationConfig stores instructions for the {@link RelationSchemaCollector} and  {@link RelationExtractor}.
 */
@Data
@Accessors(chain = true)
public class RelationConfig implements Serializable {

    @Parameter(names = "--relations.schema", description = "How to create relation tables (SingleTable, Types, Predicates, TypePredicates, None)")
    private RelationSchemaStrategy schemaStrategy = Types;

    @Parameter(names = "--relations.rootTypesOnly", arity = 1, description = "When creating relation tables between two instances of multiple types, create the relation table only for the root type pair. If false, relation tables are created for all combinations of types.")
    private boolean rootTypesOnly = true;
    /**
     * Whether entity names are forbidden to be used for predicate relation table names (only necessary if no prefix is added to table names)
     */
    private boolean entityNamesForbidden = true;
}
