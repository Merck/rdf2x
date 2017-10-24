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

import lombok.Data;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * EntitySchema defines a sorted set of {@link EntityTable}s to be persisted in a file or database.
 */
@Data
public class EntitySchema implements Serializable {

    /**
     * List of tables in this schema.
     */
    private final List<EntityTable> tables;

    /**
     * Map of type URI -&gt; unique type name
     */
    private final Map<String, String> tableNames;

    /**
     * Map of predicate URI -&gt; unique property name
     */
    private final Map<String, String> propertyNames;
}
