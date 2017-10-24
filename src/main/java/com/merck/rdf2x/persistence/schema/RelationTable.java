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

package com.merck.rdf2x.persistence.schema;

import com.merck.rdf2x.processing.schema.RelationSchemaStrategy;
import lombok.Data;
import lombok.experimental.Accessors;

import java.io.Serializable;

/**
 * RelationTable represents the structure of a single relation table to be persisted in a database or file.
 */
@Data
@Accessors(chain = true)
public class RelationTable implements Comparable<RelationTable>, Serializable {

    /**
     * name of the table
     */
    private final String name;

    /**
     * Used strategy of storing relations
     */
    private final RelationSchemaStrategy strategy;

    /**
     * Filter that defines that this table stores relations of a given predicate, null if all predicates are stored
     */
    private RelationPredicateFilter predicateFilter = null;

    /**
     * Filter that defines that this table stores relations between two entities, null if relations of all entities are stored
     */
    private RelationEntityFilter entityFilter = null;

    public boolean isSingleTable() {
        return this.strategy == RelationSchemaStrategy.SingleTable;
    }

    @Override
    public int compareTo(RelationTable o) {
        return name.compareTo(o.name);
    }

}
