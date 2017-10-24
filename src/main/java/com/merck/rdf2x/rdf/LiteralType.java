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

package com.merck.rdf2x.rdf;

import com.merck.rdf2x.beans.Predicate;
import org.apache.commons.lang3.NotImplementedException;

/**
 * LiteralType defines the type of a literal value. Used along with a URI to define a {@link Predicate}.
 */
public class LiteralType {
    public final static int UNKNOWN = 0;
    public final static int STRING = 1;
    public final static int FLOAT = 2;
    public final static int DOUBLE = 3;
    public final static int INTEGER = 4;
    public final static int LONG = 5;
    public final static int BOOLEAN = 6;
    public final static int DATETIME = 7;

    private final static String[] NAMES = new String[]{
            "UNKNOWN", "STRING", "FLOAT", "DOUBLE", "INTEGER", "LONG", "BOOLEAN", "DATETIME"
    };

    public static String toString(int literalType) {
        if (literalType < 0 || literalType >= NAMES.length) {
            throw new NotImplementedException("Missing type representation for literal type " + literalType);
        }
        return NAMES[literalType];
    }
}
