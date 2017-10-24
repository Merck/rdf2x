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

package com.merck.rdf2x.beans;

import lombok.Data;

import java.io.Serializable;

/**
 * RelationRow defines a single row in a relation table.
 * <p>
 * It is defined by a predicate URI, a from {@link TypeID} (source) and a to {@link TypeID} (target).
 */
@Data
public class RelationRow implements Serializable {
    /**
     * predicate index (represents type of the relationship)
     */
    private final Integer predicateIndex;
    /**
     * source {@link TypeID}
     */
    private final TypeID from;
    /**
     * target {@link TypeID}
     */
    private final TypeID to;

    @Override
    public String toString() {
        return "RelationRow{(Predicate" + predicateIndex + ") " +
                from + " => " + to +
                '}';
    }
}
