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

import com.merck.rdf2x.rdf.LiteralType;
import lombok.Data;
import lombok.NonNull;

import java.io.Serializable;

/**
 * Predicate is used as a key in maps of {@link Instance} values.
 * <p>
 * It is defined by a URI and a {@link LiteralType}.
 */
@Data
public class Predicate implements Serializable {
    /**
     * Predicate index representing the Predicate URI String
     */
    @NonNull
    private final Integer predicateIndex;
    /**
     * Type of the literal value that this Predicate will reference
     */
    private final int literalType;
    /**
     * Language of the literal value that this Predicate will reference (optional)
     */
    private final String language;

    /**
     * Initialize a predicate
     *
     * @param predicateIndex Predicate index representing the Predicate URI String
     * @param literalType    Type of the literal value that this Predicate will reference
     * @param language       Language of the literal value that this Predicate will reference (optional)
     */
    public Predicate(@NonNull Integer predicateIndex, int literalType, String language) {
        this.predicateIndex = predicateIndex;
        this.literalType = literalType;
        this.language = language;
    }

    /**
     * Initialize a predicate with null language
     *
     * @param predicateIndex Predicate index representing the Predicate URI String
     * @param literalType    Type of the literal value that this Predicate will reference
     */
    public Predicate(Integer predicateIndex, int literalType) {
        this(predicateIndex, literalType, null);
    }

    @Override
    public String toString() {
        return "Predicate" + predicateIndex + '(' + LiteralType.toString(literalType) + ')' + (language == null ? "" : "@" + language);
    }
}
