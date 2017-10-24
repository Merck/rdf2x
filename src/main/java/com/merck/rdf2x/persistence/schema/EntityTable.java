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

import com.merck.rdf2x.beans.Instance;
import lombok.Data;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

/**
 * EntityTable represents the structure of a single entity table of {@link Instance}s to be persisted in a database or file.
 * <p>
 * It is defined by a name, type URI and a sorted set of {@link EntityColumn}s.
 * <p>
 * It does NOT define how to store an ID or an URI of a {@link Instance}, this is handled later by persistors.
 */
@Data
public class EntityTable implements Serializable {
    /**
     * name of the table
     */
    private final String name;
    /**
     * URI of the type stored by this table
     */
    private final String typeURI;
    /**
     * Number of rows stored by this table
     */
    private final Long numRows;
    /**
     * list of table columns
     */
    private final List<EntityColumn> columns;

    /**
     * properties to be stored in Entity-Attribute-Value table
     */
    private final Set<EntityProperty> attributes;

}
