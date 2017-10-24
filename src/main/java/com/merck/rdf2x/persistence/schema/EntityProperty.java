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

import com.merck.rdf2x.beans.Predicate;
import lombok.Data;
import lombok.NonNull;

/**
 * EntityProperty stores a literal property of a specific entity. Can be saved in a column or the EAV table.
 */
@Data
public class EntityProperty {
    /**
     * the predicate stored by this property
     */
    @NonNull
    private final Predicate predicate;
    /**
     * whether the property has multiple values
     */
    private final boolean multivalued;
    /**
     * ratio of non-null values in this property
     */
    private final Double nonNullFraction;


    @Override
    public String toString() {
        return predicate +
                (multivalued ? ",Multivalued" : "") +
                (nonNullFraction == null ? "" : "," + (int) (nonNullFraction * 100) + "% non-null");
    }
}
