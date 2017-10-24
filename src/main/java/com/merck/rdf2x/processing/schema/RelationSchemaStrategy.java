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

/**
 * RelationSchemaStrategy defines which relation table schema to create
 */
public enum RelationSchemaStrategy {
    /**
     * SingleTable - Store all relations in a single table.
     */
    SingleTable,
    /**
     * Types - Create relation tables for all combinations of the two instance's types (redundant).
     */
    Types,
    /**
     * Predicates - Create one relation table for each predicate
     */
    Predicates,
    /**
     * TypePredicates - Create one relation table for each predicate between two entity tables
     */
    TypePredicates,
    /**
     * None - Do not extract relations
     */
    None
}
