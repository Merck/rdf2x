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

/**
 * ParseErrorHandling is used in config objects to define handling of RDF parse errors.
 */
public enum ParseErrorHandling {
    /**
     * Ignore quads containing errors
     */
    Ignore,

    /**
     * Store quads containing errors in an Error table
     */
    Store,

    /**
     * When encountering an error, throw an exception and exit.
     */
    Throw
}